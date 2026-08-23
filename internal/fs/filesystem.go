package fs

import (
	"os"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/NaveenSingh9999/squidfs/internal/api"
	"github.com/NaveenSingh9999/squidfs/internal/cache"
	"github.com/NaveenSingh9999/squidfs/internal/encryption"
	"github.com/NaveenSingh9999/squidfs/internal/platform"
	"github.com/NaveenSingh9999/squidfs/internal/stream"
)

type SquidFS struct {
	client     *api.Client
	cache      *cache.Cache
	encryptor  *encryption.Encryptor
	httpClient *http.Client
	Server     *fuse.Server
}

type Dir struct {
	fs.Inode
	squidfs *SquidFS
	id      string
	name    string
	path    string
	entries map[string]*fs.Inode
	mu      sync.RWMutex
}

type File struct {
	fs.Inode
	squidfs *SquidFS
	id      string
	name    string
	path    string
	size    int64
	data    []byte
	mu      sync.RWMutex

	// high-performance state
	dirty     bool
	tmpPath   string // spool for large writes
	lazy      bool   // serve reads via chunk cache (large files)
	chunks    []api.ChunkMetadata
	keyHex    string // platform key for this file ('' = none)
	platKeyOK bool
	modTime   time.Time
}

// uploadSignature tracks the last persisted content so unchanged files are
// never re-uploaded (file managers rewrite files constantly).
var uploadSignatures sync.Map // id -> "size:modUnixNano"

func New(client *api.Client, cache *cache.Cache, encryptor *encryption.Encryptor) *SquidFS {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: 5 * time.Second}
			for _, dns := range []string{"8.8.8.8:53", "1.1.1.1:53", "8.8.4.4:53"} {
				conn, err := d.DialContext(ctx, "udp", dns)
				if err == nil {
					return conn, nil
				}
			}
			return nil, fmt.Errorf("all DNS servers failed")
		},
	}
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				Resolver:  resolver,
			}).DialContext,
			MaxIdleConns:        10,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
	return &SquidFS{
		client:     client,
		cache:      cache,
		encryptor:  encryptor,
		httpClient: httpClient,
	}
}

func (s *SquidFS) Mount(mountPoint string) error {
	root := &Dir{
		squidfs: s,
		id:      "root",
		name:    "/",
		path:    "/",
		entries: make(map[string]*fs.Inode),
	}

	rawFS := fs.NewNodeFS(root, &fs.Options{
		AttrTimeout:  nil,
		EntryTimeout: nil,
	})

	server, err := fuse.NewServer(rawFS, mountPoint, &fuse.MountOptions{
		Name: "squidfs",
	})
	if err != nil {
		return fmt.Errorf("create fuse server: %w", err)
	}

	s.Server = server

	log.Printf("SquidCloud mounted at %s", mountPoint)
	server.Serve()

	return nil
}

func (n *Dir) Readdir(ctx context.Context) ([]fuse.DirEntry, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.squidfs.loadDir(n)

	var entries []fuse.DirEntry
	for name := range n.entries {
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Mode: fuse.S_IFDIR | 0755,
		})
	}

	return entries, 0
}

func (n *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if node, ok := n.entries[name]; ok {
		return node, 0
	}

	n.squidfs.loadDir(n)

	if node, ok := n.entries[name]; ok {
		return node, 0
	}

	return nil, syscall.ENOENT
}

func (n *Dir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	file := &File{
		squidfs: n.squidfs,
		name:    name,
		path:    path.Join(n.path, name),
		data:    make([]byte, 0),
	}

	child := n.NewInode(ctx, file, fs.StableAttr{Mode: fuse.S_IFREG})

	n.mu.Lock()
	n.entries[name] = child
	n.mu.Unlock()

	return child, 0
}

func (f *File) Getattr(ctx context.Context, out *fuse.AttrOut, fh uint64) syscall.Errno {
	f.mu.RLock()
	defer f.mu.RUnlock()

	out.Attr.Size = uint64(len(f.data))
	out.Attr.Mode = 0644
	out.Attr.Blksize = 512
	out.Attr.Blocks = (uint64(len(f.data)) + 511) / 512
	return 0
}

func (f *File) Read(ctx context.Context, data []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	f.mu.RLock()
	dirty := f.dirty
	lazy := f.lazy
	size := f.size
	f.mu.RUnlock()

	if off >= size {
		return nil, 0
	}

	// Dirty (being written) or small files: serve from the in-memory copy.
	if dirty || !lazy {
		if f.data == nil {
			f.mu.Lock()
			if err := f.download(); err != nil {
				f.mu.Unlock()
				log.Printf("Download error for %s: %v", f.id, err)
				return nil, syscall.EIO
			}
			f.mu.Unlock()
		}
		f.mu.RLock()
		defer f.mu.RUnlock()
		if off >= int64(len(f.data)) {
			return nil, 0
		}
		end := off + int64(len(data))
		if end > int64(len(f.data)) {
			end = int64(len(f.data))
		}
		return fuse.ReadResultData(f.data[off:end]), 0
	}

	// Lazy path: pull only the chunks covering this read.
	want := int64(len(data))
	if remaining := size - off; remaining < want {
		want = remaining
	}
	plain, err := f.readRange(off, want)
	if err != nil {
		log.Printf("Range read error for %s@%d: %v", f.name, off, err)
		return nil, syscall.EIO
	}
	n := copy(data, plain)
	// Prefetch the next window in the background for sequential readers.
	go func(next int64) {
		if _, err := f.readRange(next, 4*1024*1024); err != nil {
			log.Printf("prefetch error: %v", err)
		}
	}(off + want)
	return fuse.ReadResultData(plain[:n]), 0
}

// readRange returns exactly `length` bytes of file plaintext starting at off
// (clamped to file size), pulling/decrypting only the chunks involved.
func (f *File) readRange(off, length int64) ([]byte, error) {
	if length <= 0 {
		return []byte{}, nil
	}
	if off+length > f.size {
		length = f.size - off
	}
	if length <= 0 {
		return []byte{}, nil
	}

	idx := stream.ChunksCovering(off, length)
	out := make([]byte, 0, length)

	var urls map[int]string
	getURL := func(i int) (string, error) {
		if urls == nil {
			reqs := make([]api.DownloadChunkRequest, len(f.chunks))
			for j, c := range f.chunks {
				reqs[j] = api.DownloadChunkRequest{Path: c.Path, Index: c.Index, Bucket: c.Bucket}
			}
			resolved, err := f.squidfs.client.ResolveDownloadURLs(reqs)
			if err != nil {
				return "", err
			}
			urls = make(map[int]string, len(resolved))
			for _, u := range resolved {
				urls[u.Index] = u.DownloadURL
			}
		}
		u, ok := urls[i]
		if !ok {
			return "", fmt.Errorf("no url for chunk %d", i)
		}
		return u, nil
	}

	keyRaw := platform.DeriveKey(f.keyHex)

	for _, ci := range idx {
		ck := cache.CacheKey(f.id, ci)
		if cached, ok := f.squidfs.cache.Get(ck); ok {
			out = append(out, sliceFor(cached, off, length, ci)...)
			continue
		}
		u, err := getURL(ci)
		if err != nil {
			return nil, err
		}
		resp, err := f.squidfs.httpClient.Get(u)
		if err != nil {
			return nil, fmt.Errorf("chunk %d fetch: %w", ci, err)
		}
		blob, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("chunk %d read: %w", ci, err)
		}

		var plain []byte
		if f.platKeyOK {
			p2, derr := platform.DecryptChunk(keyRaw, blob)
			if derr != nil {
				plain = blob // legacy raw passthrough
			} else {
				plain = p2
			}
		} else if f.squidfs.encryptor != nil {
			p3, derr := f.squidfs.encryptor.Decrypt(blob)
			if derr != nil { plain = blob } else { plain = p3 }
		} else {
			plain = blob
		}

		f.squidfs.cache.Set(ck, plain)

		s := sliceFor(plain, off, length, ci)
		out = append(out, s...)

		// background prefetch of the following chunk
		if ci+1 < len(f.chunks) {
			go func(nx int) {
				_ = prefetchChunk(f, nx)
			}(ci + 1)
		}
	}
	return out, nil
}

func sliceFor(chunk []byte, off, total int64, chunkIndex int) []byte {
	cs := int64(stream.ChunkSize)
	start := int64(chunkIndex) * cs
	sOff := off - start
	if sOff < 0 { sOff = 0 }
	eOff := (off + total) - start
	if eOff > int64(len(chunk)) { eOff = int64(len(chunk)) }
	if sOff >= int64(len(chunk)) || sOff >= eOff { return nil }
	return chunk[sOff:eOff]
}

func prefetchChunk(f *File, index int) error {
	ck := cache.CacheKey(f.id, index)
	if f.squidfs.cache.Has(ck) { return nil }
	reqs := make([]api.DownloadChunkRequest, len(f.chunks))
	for j, c := range f.chunks { reqs[j] = api.DownloadChunkRequest{Path: c.Path, Index: c.Index, Bucket: c.Bucket} }
	resolved, err := f.squidfs.client.ResolveDownloadURLs(reqs)
	if err != nil { return err }
	for _, u := range resolved {
		if u.Index != index { continue }
		resp, err := f.squidfs.httpClient.Get(u.DownloadURL)
		if err != nil { return err }
		blob, err := io.ReadAll(resp.Body); resp.Body.Close()
		if err != nil { return err }
		var plain []byte
		keyRaw := platform.DeriveKey(f.keyHex)
		if f.platKeyOK {
			if p, derr := platform.DecryptChunk(keyRaw, blob); derr == nil { plain = p } else { plain = blob }
		} else { plain = blob }
		f.squidfs.cache.Set(ck, plain)
		return nil
	}
	return fmt.Errorf("no url")
}

const spoolThreshold = 32 * 1024 * 1024

func (f *File) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()

	start := int(off)
	end := start + len(data)

	// Large writes spill to a temp file instead of RAM.
	if end > spoolThreshold && f.tmpPath == "" {
		tmp, err := os.CreateTemp("", "squidfs-write-*")
		if err != nil {
			return 0, syscall.EIO
		}
		f.tmpPath = tmp.Name()
		tmp.Write(f.data)
		tmp.Close()
	}
	if f.tmpPath != "" {
		tf, err := os.OpenFile(f.tmpPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return 0, syscall.EIO
		}
		tf.WriteAt(data, off)
		tf.Close()
		if end > len(f.data) {
			f.data = make([]byte, end)
		} else {
			f.data = f.data[:end]
		}
	} else {
		if end > len(f.data) {
			grown := make([]byte, end)
			copy(grown, f.data)
			f.data = grown
		}
		copy(f.data[start:end], data)
	}

	f.size = int64(end)
	now := time.Now()
	f.modTime = now
	f.dirty = true

	// No upload here — Flush/Release triggers exactly one upload per close.
	return uint32(len(data)), 0
}

// Flush fires when an application fsyncs/closes the file.
func (f *File) Flush(ctx context.Context, fh uint64) syscall.Errno {
	f.mu.Lock()
	dirty := f.dirty
	f.mu.Unlock()
	if !dirty {
		return 0
	}
	if err := f.uploadOnce(); err != nil {
		log.Printf("Upload error for %s: %v", f.name, err)
		return syscall.EIO
	}
	return 0
}

// Release is the final close; uploads anything Flush missed.
func (f *File) Release(ctx context.Context, fh uint64) syscall.Errno {
	f.mu.RLock()
	dirty := f.dirty
	f.mu.RUnlock()
	if dirty {
		if err := f.uploadOnce(); err != nil {
			log.Printf("Release upload error for %s: %v", f.name, err)
		}
	}
	return 0
}

// uploadOnce persists the file exactly once per close cycle and skips
// re-uploads of identical content entirely.
func (f *File) uploadOnce() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dirty {
		return nil
	}

	var data []byte
	if f.tmpPath != "" {
		b, err := os.ReadFile(f.tmpPath)
		if err != nil {
			return fmt.Errorf("read spool: %w", err)
		}
		data = b
	} else {
		data = f.data
	}
	if len(data) == 0 {
		f.dirty = false
		return nil
	}

	sig := fmt.Sprintf("%d:%d", len(data), f.modTime.UnixNano())
	if prev, ok := uploadSignatures.Load(f.id); ok && prev == sig && f.platKeyOK {
		log.Printf("skip upload (unchanged): %s", f.name)
		f.dirty = false
		return nil
	}

	if err := f.upload(); err != nil {
		return err
	}

	uploadSignatures.Store(f.id, sig)
	f.dirty = false

	os.Remove(f.tmpPath)
	f.tmpPath = ""
	return nil
}

func (f *File) download() error {
	cacheKey := cache.CacheKey(f.id, 0)
	if data, ok := f.squidfs.cache.Get(cacheKey); ok {
		f.data = data
		f.size = int64(len(data))
		return nil
	}

	fileMeta, err := f.squidfs.client.GetFile(f.id)
	if err != nil {
		return fmt.Errorf("get file metadata: %w", err)
	}

	if fileMeta.StoragePath != "res54_distributed" {
		return fmt.Errorf("unsupported storage: %s", fileMeta.StoragePath)
	}

	var tags api.FileTags
	if fileMeta.Tags != nil {
		tagsRaw := fileMeta.Tags
		if len(fileMeta.Tags) > 0 && fileMeta.Tags[0] == '[' {
			var arr []json.RawMessage
			if err := json.Unmarshal(fileMeta.Tags, &arr); err == nil && len(arr) > 0 {
				tagsRaw = arr[0]
				var s string
				if err := json.Unmarshal(tagsRaw, &s); err == nil {
					tagsRaw = []byte(s)
				}
			}
		}
		if len(tagsRaw) > 0 && tagsRaw[0] == '"' {
			var s string
			if err := json.Unmarshal(tagsRaw, &s); err == nil {
				tagsRaw = []byte(s)
			}
		}
		if err := json.Unmarshal(tagsRaw, &tags); err != nil {
			return fmt.Errorf("parse tags: %w", err)
		}
	}

	if len(tags.Chunks) == 0 {
		return fmt.Errorf("no chunks found")
	}

	var downloadChunks []api.DownloadChunkRequest
	for _, c := range tags.Chunks {
		downloadChunks = append(downloadChunks, api.DownloadChunkRequest{
			Path:   c.Path,
			Index:  c.Index,
			Bucket: c.Bucket,
		})
	}

	urls, err := f.squidfs.client.ResolveDownloadURLs(downloadChunks)
	if err != nil {
		return fmt.Errorf("resolve download URLs: %w", err)
	}

	urlMap := make(map[int]string)
	for _, u := range urls {
		urlMap[u.Index] = u.DownloadURL
	}

	// Fetch every chunk object. Platform files keep one res54 envelope per
	// chunk; legacy squidfs files are raw slices (optionally whole-file
	// encrypted by the local encryptor).
	chunkBlobs := make([][]byte, 0, len(tags.Chunks))
	for _, c := range tags.Chunks {
		downloadURL, ok := urlMap[c.Index]
		if !ok {
			return fmt.Errorf("missing URL for chunk %d", c.Index)
		}
		resp, err := f.squidfs.httpClient.Get(downloadURL)
		if err != nil {
			return fmt.Errorf("download chunk %d: %w", c.Index, err)
		}
		chunkData, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("read chunk %d: %w", c.Index, err)
		}
		chunkBlobs = append(chunkBlobs, chunkData)
	}

	hasPlatformKey := tags.EncryptionKey != "" &&
		tags.EncryptionKey != "sha256:byok_encrypted" &&
		tags.EncryptionKey != "byok_encrypted" &&
		tags.EncryptionKey != "managed_key"

	var allData []byte
	switch {
	case hasPlatformKey:
		key := platform.DeriveKey(tags.EncryptionKey)
		for _, blob := range chunkBlobs {
			plain, err := platform.DecryptChunk(key, blob)
			if err != nil {
				// Raw slice (unencrypted legacy) — pass through as-is.
				allData = append(allData, blob...)
				continue
			}
			allData = append(allData, plain...)
		}
	case fileMeta.Encrypted && f.squidfs.encryptor.IsEnabled():
		blob := make([]byte, 0, 4096)
		for _, b := range chunkBlobs {
			blob = append(blob, b...)
		}
		decrypted, err := f.squidfs.encryptor.Decrypt(blob)
		if err != nil {
			return fmt.Errorf("decrypt: %w", err)
		}
		allData = decrypted
	default:
		for _, b := range chunkBlobs {
			allData = append(allData, b...)
		}
	}

	if err := f.squidfs.cache.Set(cacheKey, allData); err != nil {
		log.Printf("Cache write failed: %v", err)
	}

	f.data = allData
	f.size = int64(len(allData))
	return nil
}

// uploadFrom streams the file to cluster storage using parallel chunk PUTs,
// platform-compatible per-chunk envelopes, and a single metadata commit.
func (f *File) upload() error {
	data := f.data

	// Platform interop: encrypt each chunk into the legacy-compatible
	// res54 envelope with a per-file key the dashboard can resolve.
	usePlatformCrypto := !f.squidfs.encryptor.IsEnabled()
	var keyHex string
	if usePlatformCrypto {
		k, err := platform.GenerateKeyHex()
		if err != nil {
			return fmt.Errorf("generate key: %w", err)
		}
		keyHex = k
	}

	if f.squidfs.encryptor.IsEnabled() {
		encrypted, err := f.squidfs.encryptor.Encrypt(data)
		if err != nil {
			return fmt.Errorf("encrypt: %w", err)
		}
		data = encrypted
	}

	chunksNeeded := (len(data) + platform.ChunkSize() - 1) / platform.ChunkSize()
	if chunksNeeded == 0 {
		chunksNeeded = 1
	}

	chunkPaths := make([]api.ChunkUploadRequest, chunksNeeded)
	for i := 0; i < chunksNeeded; i++ {
		chunkPaths[i] = api.ChunkUploadRequest{
			Path:  fmt.Sprintf("squidfs/%s/%s_%d.bin", time.Now().Format("20060102"), f.name, i),
			Index: i,
		}
	}

	urlList, err := f.squidfs.client.GetUploadURLs(chunkPaths, int64(len(data)))
	if err != nil {
		return fmt.Errorf("get upload URLs: %w", err)
	}
	urlMap := make(map[int]api.UploadURLInfo, len(urlList))
	for _, u := range urlList {
		urlMap[u.Index] = u
	}

	// Prepare per-chunk slices (platform-sealed when applicable).
	slices := make([][]byte, chunksNeeded)
	key := platform.DeriveKey(keyHex)
	for i := 0; i < chunksNeeded; i++ {
		st := i * platform.ChunkSize()
		en := st + platform.ChunkSize()
		if en > len(data) {
			en = len(data)
		}
		sl := data[st:en]
		if usePlatformCrypto {
			sealed, serr := platform.EncryptChunk(key, sl)
			if serr != nil {
				return fmt.Errorf("encrypt chunk %d: %w", i, serr)
			}
			sl = sealed
		}
		slices[i] = sl
	}

	// Parallel PUT with bounded workers + retry.
	var mu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, stream.Workers)
	var wg sync.WaitGroup
	for i := 0; i < chunksNeeded; i++ {
		i := i
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			u, ok := urlMap[i]
			if !ok {
				mu.Lock(); if firstErr == nil { firstErr = fmt.Errorf("no URL for chunk %d", i) }; mu.Unlock()
				return
			}
			req, rerr := http.NewRequest(http.MethodPut, u.UploadURL, bytes.NewReader(slices[i]))
			if rerr != nil {
				mu.Lock(); if firstErr == nil { firstErr = rerr }; mu.Unlock()
				return
			}
			req.Header.Set("Content-Type", "application/octet-stream")
			resp, derr := f.squidfs.httpClient.Do(req)
			if derr != nil {
				mu.Lock(); if firstErr == nil { firstErr = derr }; mu.Unlock()
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 300 {
				mu.Lock(); if firstErr == nil { firstErr = fmt.Errorf("chunk %d HTTP %d", i, resp.StatusCode) }; mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	chunkMetas := make([]api.ChunkMetadata, chunksNeeded)
	for i := 0; i < chunksNeeded; i++ {
		st := i * platform.ChunkSize()
		en := st + platform.ChunkSize()
		if en > len(data) {
			en = len(data)
		}
		u := urlMap[i]
		chunkMetas[i] = api.ChunkMetadata{
			Index:       i,
			TotalChunks: chunksNeeded,
			Size:        en - st,
			Offset:      st,
			Path:        u.Path,
			Bucket:      u.Bucket,
			ClusterID:   u.ClusterID,
		}
	}

	tags := api.FileTags{
		FileName:      f.name,
		FileType:      api.MimeTypeFor(f.name),
		FileSize:      int64(len(f.data)),
		EncryptionKey: keyHex,
		Created:       time.Now().UTC().Format(time.RFC3339),
		Chunks:        chunkMetas,
	}
	tagsJSON, _ := json.Marshal(tags)
	tagsArray, _ := json.Marshal([]json.RawMessage{tagsJSON})

	fileRecord := api.UploadFileRecord{
		Name:         f.name,
		Type:         api.MimeTypeFor(f.name),
		Size:         int64(len(f.data)),
		StoragePath:  "res54_distributed",
		MimeType:     api.MimeTypeFor(f.name),
		Encrypted:    usePlatformCrypto || f.squidfs.encryptor.IsEnabled(),
		EncryptionKey: map[bool]string{true: "sha256:byok_encrypted"}[f.squidfs.encryptor.IsEnabled()],
		ParentFolder: f.path,
		Tags:         tagsArray,
	}

	created, err := f.squidfs.client.CreateFileRecord(fileRecord)
	if err != nil {
		return fmt.Errorf("create file record: %w", err)
	}

	f.id = created.ID

	cacheKey := cache.CacheKey(f.id, 0)
	if err := f.squidfs.cache.Set(cacheKey, f.data); err != nil {
		log.Printf("Cache write failed: %v", err)
	}

	return nil
}

func (s *SquidFS) loadDir(dir *Dir) {
	folderName := dir.name
	if dir.id == "root" {
		folderName = ""
	}

	result, err := s.client.ListFilesByName(folderName)
	if err != nil {
		log.Printf("Failed to load dir %s: %v", dir.name, err)
		return
	}

	for _, folder := range result.Folders {
		subDir := &Dir{
			squidfs: s,
			id:      folder.ID,
			name:    folder.Name,
			path:    folder.ParentFolder,
			entries: make(map[string]*fs.Inode),
		}
		child := dir.NewInode(context.Background(), subDir, fs.StableAttr{Mode: fuse.S_IFDIR})
		dir.entries[folder.Name] = child
	}

	for _, file := range result.Files {
		f := &File{
			squidfs: s,
			id:      file.ID,
			name:    file.Name,
			path:    file.ParentFolder,
			size:    file.Size,
		}
		child := dir.NewInode(context.Background(), f, fs.StableAttr{Mode: fuse.S_IFREG})
		dir.entries[file.Name] = child
	}
}
