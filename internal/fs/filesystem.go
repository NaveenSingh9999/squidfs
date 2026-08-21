package fs

import (
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
}

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
	defer f.mu.RUnlock()

	if f.data == nil {
		if err := f.download(); err != nil {
			log.Printf("Download error for %s: %v", f.id, err)
			return nil, syscall.EIO
		}
	}

	if off >= int64(len(f.data)) {
		return nil, 0
	}

	end := off + int64(len(data))
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}

	return fuse.ReadResultData(f.data[off:end]), 0
}

func (f *File) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()

	start := int(off)
	end := start + len(data)

	if end > len(f.data) {
		newData := make([]byte, end)
		copy(newData, f.data)
		f.data = newData
	}

	copy(f.data[start:end], data)

	if err := f.upload(); err != nil {
		log.Printf("Upload error for %s: %v", f.name, err)
		return 0, syscall.EIO
	}

	return uint32(len(data)), 0
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

	var allData []byte
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
		allData = append(allData, chunkData...)
	}

	if fileMeta.Encrypted && f.squidfs.encryptor.IsEnabled() {
		decrypted, err := f.squidfs.encryptor.Decrypt(allData)
		if err != nil {
			return fmt.Errorf("decrypt: %w", err)
		}
		allData = decrypted
	}

	if err := f.squidfs.cache.Set(cacheKey, allData); err != nil {
		log.Printf("Cache write failed: %v", err)
	}

	f.data = allData
	f.size = int64(len(allData))
	return nil
}

func (f *File) upload() error {
	data := f.data

	if f.squidfs.encryptor.IsEnabled() {
		encrypted, err := f.squidfs.encryptor.Encrypt(data)
		if err != nil {
			return fmt.Errorf("encrypt: %w", err)
		}
		data = encrypted
	}

	chunksNeeded := (len(data) + 2*1024*1024 - 1) / (2 * 1024 * 1024)
	if chunksNeeded == 0 {
		chunksNeeded = 1
	}

	var chunkPaths []api.ChunkUploadRequest
	for i := 0; i < chunksNeeded; i++ {
		chunkPaths = append(chunkPaths, api.ChunkUploadRequest{
			Path:  fmt.Sprintf("squidfs/%s/%s_%d.bin", time.Now().Format("20060102"), f.name, i),
			Index: i,
		})
	}

	urls, err := f.squidfs.client.GetUploadURLs(chunkPaths, int64(len(data)))
	if err != nil {
		return fmt.Errorf("get upload URLs: %w", err)
	}

	chunkSize := 2 * 1024 * 1024
	for _, u := range urls {
		start := u.Index * chunkSize
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[start:end]

		req, err := http.NewRequest("PUT", u.UploadURL, nil)
		if err != nil {
			return fmt.Errorf("create PUT request: %w", err)
		}
		req.Body = io.NopCloser(bytes.NewReader(chunk))
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := f.squidfs.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("upload chunk %d: %w", u.Index, err)
		}
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("upload chunk %d failed: %d", u.Index, resp.StatusCode)
		}
	}

	chunkMetas := make([]api.ChunkMetadata, len(urls))
	for _, u := range urls {
		start := u.Index * chunkSize
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunkMetas[u.Index] = api.ChunkMetadata{
			Index:       u.Index,
			TotalChunks: len(urls),
			Size:        end - start,
			Offset:      start,
			Path:        u.Path,
			Bucket:      u.Bucket,
			ClusterID:   u.ClusterID,
		}
	}

	tags := api.FileTags{
		FileName: f.name,
		FileType: "application/octet-stream",
		FileSize: int64(len(data)),
		Chunks:   chunkMetas,
	}
	tagsJSON, _ := json.Marshal(tags)
	tagsArray, _ := json.Marshal([]json.RawMessage{tagsJSON})

	fileRecord := api.UploadFileRecord{
		Name:         f.name,
		Type:         "application/octet-stream",
		Size:         int64(len(data)),
		StoragePath:  "res54_distributed",
		MimeType:     "application/octet-stream",
		Encrypted:    f.squidfs.encryptor.IsEnabled(),
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
