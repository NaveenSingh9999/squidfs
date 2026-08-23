package webdav

import (
	"strconv"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grandcat/zeroconf"
	"golang.org/x/net/webdav"

	"github.com/NaveenSingh9999/squidfs/internal/api"
	"github.com/NaveenSingh9999/squidfs/internal/cache"
	"github.com/NaveenSingh9999/squidfs/internal/platform"
	"github.com/NaveenSingh9999/squidfs/internal/stream"
	"github.com/NaveenSingh9999/squidfs/internal/encryption"
)

type WebDAVServer struct {
	handler    *webdav.Handler
	client     *api.Client
	cache      *cache.Cache
	encryptor  *encryption.Encryptor
	httpClient *http.Client
	listener   net.Listener
	port       int
	mu         sync.RWMutex
	nameToID   map[string]string
	fileInfo   map[string]*api.FileMetadata
	folderInfo map[string]*api.FolderMetadata
	mdns       *zeroconf.Server
}

type webdavFileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (fi *webdavFileInfo) Name() string      { return fi.name }
func (fi *webdavFileInfo) Size() int64       { return fi.size }
func (fi *webdavFileInfo) Mode() os.FileMode { if fi.isDir { return 0755 | os.ModeDir }; return 0644 }
func (fi *webdavFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *webdavFileInfo) IsDir() bool       { return fi.isDir }
func (fi *webdavFileInfo) Sys() interface{}  { return nil }

func NewServer(client *api.Client, cache *cache.Cache, encryptor *encryption.Encryptor, port int) *WebDAVServer {
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
	return &WebDAVServer{
		client:     client,
		cache:      cache,
		encryptor:  encryptor,
		httpClient: httpClient,
		port:       port,
		nameToID:   make(map[string]string),
		fileInfo:   make(map[string]*api.FileMetadata),
		folderInfo: make(map[string]*api.FolderMetadata),
	}
}

func (s *WebDAVServer) registerMDNS() error {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "squidfs"
	}
	txtRecords := []string{
		"path=/",
		"version=1",
		fmt.Sprintf("port=%d", s.port),
	}
	var err error
	s.mdns, err = zeroconf.Register(hostname, "_webdav._tcp", "local.", s.port, txtRecords, nil)
	if err != nil {
		return fmt.Errorf("register mDNS: %w", err)
	}
	log.Printf("mDNS registered: %s._webdav._tcp.local on port %d", hostname, s.port)
	return nil
}

func (s *WebDAVServer) unregisterMDNS() {
	if s.mdns != nil {
		s.mdns.Shutdown()
	}
}

func (s *WebDAVServer) Start() error {
	s.handler = &webdav.Handler{
		Prefix:     "/",
		FileSystem: s,
		LockSystem: webdav.NewMemLS(),
		Logger: func(r *http.Request, err error) {
			if err != nil {
				log.Printf("WebDAV error: %v", err)
			}
		},
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" || r.Method == "HEAD" {
			name := strings.TrimPrefix(r.URL.Path, "/")
			name = strings.TrimSuffix(name, "/")
			if name == "" {
				http.Error(w, "cannot GET root", 405)
				return
			}
			log.Printf("GET %s", name)

			fi, err := s.statFor(name)
			if err == nil && fi.StoragePath == "res54_distributed" {
				w.Header().Set("Accept-Ranges", "bytes")
			}

			rangeHdr := r.Header.Get("Range")
			var start, length int64
			hasRange := false
			if rangeHdr != "" && fi != nil {
				if parsed, ok := parseByteRange(rangeHdr, fi.Size); ok {
					start, length = parsed.start, parsed.length
					hasRange = true
				}
			}

			var data []byte
			if hasRange && fi.StoragePath == "res54_distributed" {
				data, err = s.readRangeLazy(fi, start, length)
				if err != nil {
					log.Printf("GET %s range error: %v", name, err)
					http.Error(w, "range read failed", http.StatusInternalServerError)
					return
				}
				ct := mimeTypes[path.Ext(name)]
				if ct == "" { ct = "application/octet-stream" }
				w.Header().Set("Content-Type", ct)
				w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+int64(len(data))-1, fi.Size))
				w.WriteHeader(http.StatusPartialContent)
				if r.Method == "GET" { w.Write(data) }
				return
			}

			data, err = s.downloadFile(name)
			if err != nil {
				log.Printf("GET %s error: %v", name, err)
				http.NotFound(w, r)
				return
			}
			ct := mimeTypes[path.Ext(name)]
			if ct == "" {
				ct = "application/octet-stream"
			}
			w.Header().Set("Content-Type", ct)
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			if r.Method == "GET" {
				w.Write(data)
			}
			return
		}
		if r.Method == "PROPFIND" {
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		}
		sw := &stripErrorWriter{ResponseWriter: w}
		s.handler.ServeHTTP(sw, r)
	})

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.listener = listener

	if err := s.registerMDNS(); err != nil {
		log.Printf("mDNS failed (non-fatal): %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		s.Stop()
		os.Exit(0)
	}()

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "localhost"
	}
	log.Printf("WebDAV server on port %d", s.port)
	log.Printf("  http://%s:%d", hostname, s.port)

	return http.Serve(listener, handler)
}

func (s *WebDAVServer) Stop() error {
	s.unregisterMDNS()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *WebDAVServer) cacheEntries(dirPath string, folders []api.FolderMetadata, files []api.FileMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for i := range folders {
		f := &folders[i]
		key := f.Name
		if dirPath != "" {
			key = dirPath + "/" + f.Name
		}
		s.nameToID[key] = f.ID
		if f.CreatedAt.IsZero() {
			f.CreatedAt = now
		}
		s.folderInfo[key] = f
	}
	for i := range files {
		f := &files[i]
		key := f.Name
		if dirPath != "" {
			key = dirPath + "/" + f.Name
		}
		s.nameToID[key] = f.ID
		if f.UpdatedAt.IsZero() {
			f.UpdatedAt = now
		}
		if f.CreatedAt.IsZero() {
			f.CreatedAt = now
		}
		s.fileInfo[key] = f
	}
}

func (s *WebDAVServer) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	name = strings.TrimPrefix(name, "/")
	name = strings.TrimSuffix(name, "/")
	parentDir := path.Dir(name)
	if parentDir == "." {
		parentDir = ""
	}
	folderName := path.Base(name)
	_, err := s.client.CreateFolder(folderName, parentDir)
	return err
}

func (s *WebDAVServer) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	name = strings.TrimPrefix(name, "/")
	name = strings.TrimSuffix(name, "/")

	if name == "" {
		if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
			return &WebDAVFile{
				server:   s,
				name:     "",
				flag:     flag,
				isNew:    true,
				parentID: "",
			}, nil
		}
		entries, err := s.listDir("")
		if err != nil {
			return nil, err
		}
		return &WebDAVDir{server: s, name: "/", entries: entries}, nil
	}

	parts := strings.SplitN(name, "/", 2)
	topLevel := parts[0]

	s.mu.RLock()
	fi, isFile := s.fileInfo[topLevel]
	_, isFolder := s.folderInfo[topLevel]
	s.mu.RUnlock()

	if isFolder {
		if len(parts) > 1 && parts[1] != "" {
			entries, err := s.listDir(topLevel)
			if err == nil {
				subName := parts[1]
				for _, e := range entries {
					if e.Name() == subName {
						if e.IsDir() {
							subEntries, err := s.listDir(path.Join(topLevel, subName))
							if err == nil {
								return &WebDAVDir{server: s, name: subName, entries: subEntries}, nil
							}
						}
						fullKey := path.Join(topLevel, subName)
						s.mu.RLock()
						sfi, ok := s.fileInfo[fullKey]
						s.mu.RUnlock()
						if ok {
							return &WebDAVFile{server: s, name: name, flag: flag, size: sfi.Size, existing: sfi}, nil
						}
						return &WebDAVFile{server: s, name: name, flag: flag}, nil
					}
				}
			}
			return nil, os.ErrNotExist
		}
		entries, err := s.listDir(topLevel)
		if err != nil {
			return nil, err
		}
		return &WebDAVDir{server: s, name: topLevel, entries: entries}, nil
	}

	if isFile {
		if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
			return &WebDAVFile{server: s, name: name, flag: flag, isNew: false, existing: fi}, nil
		}
		return &WebDAVFile{server: s, name: name, flag: flag, size: fi.Size, existing: fi}, nil
	}

	if flag&(os.O_CREATE|os.O_WRONLY|os.O_RDWR) != 0 {
		return &WebDAVFile{server: s, name: name, flag: flag, isNew: true, parentID: topLevel}, nil
	}

	return nil, os.ErrNotExist
}

func (s *WebDAVServer) RemoveAll(ctx context.Context, name string) error {
	name = strings.TrimPrefix(name, "/")
	name = strings.TrimSuffix(name, "/")
	if name == "" {
		return fmt.Errorf("cannot delete root")
	}
	s.mu.RLock()
	id, ok := s.nameToID[name]
	s.mu.RUnlock()
	if !ok {
		return os.ErrNotExist
	}
	return s.client.DeleteFile(id)
}

func (s *WebDAVServer) Rename(ctx context.Context, oldName, newName string) error {
	oldName = strings.TrimPrefix(oldName, "/")
	newName = strings.TrimPrefix(newName, "/")
	s.mu.RLock()
	id, ok := s.nameToID[oldName]
	s.mu.RUnlock()
	if !ok {
		return os.ErrNotExist
	}
	return s.client.RenameFile(id, path.Base(newName))
}

func (s *WebDAVServer) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	name = strings.TrimPrefix(name, "/")
	name = strings.TrimSuffix(name, "/")
	now := time.Now()

	if name == "" {
		return &webdavFileInfo{name: "/", isDir: true, modTime: now}, nil
	}

	parts := strings.SplitN(name, "/", 2)
	topLevel := parts[0]

	s.mu.RLock()
	fi, isFile := s.fileInfo[topLevel]
	fdi, isFolder := s.folderInfo[topLevel]
	s.mu.RUnlock()

	if isFile && len(parts) == 1 {
		mt := fi.UpdatedAt
		if mt.IsZero() {
			mt = now
		}
		return &webdavFileInfo{name: fi.Name, size: fi.Size, modTime: mt, isDir: false}, nil
	}

	if isFolder {
		if len(parts) > 1 && parts[1] != "" {
			subName := parts[1]
			subParts := strings.SplitN(subName, "/", 2)
			fullKey := path.Join(topLevel, subParts[0])
			s.mu.RLock()
			sfi, sfIsFile := s.fileInfo[fullKey]
			smu, sfIsFolder := s.folderInfo[fullKey]
			s.mu.RUnlock()
			if sfIsFile && len(subParts) == 1 {
				mt := sfi.UpdatedAt
				if mt.IsZero() { mt = now }
				return &webdavFileInfo{name: sfi.Name, size: sfi.Size, modTime: mt, isDir: false}, nil
			}
			if sfIsFolder {
				mt := smu.CreatedAt
				if mt.IsZero() { mt = now }
				return &webdavFileInfo{name: smu.Name, modTime: mt, isDir: true}, nil
			}
			return &webdavFileInfo{name: subParts[0], modTime: now, isDir: false}, nil
		}
		mt := fdi.CreatedAt
		if mt.IsZero() { mt = now }
		return &webdavFileInfo{name: fdi.Name, modTime: mt, isDir: true}, nil
	}

	return &webdavFileInfo{name: path.Base(name), modTime: now, isDir: false}, nil
}

func (s *WebDAVServer) listDir(dirPath string) ([]os.FileInfo, error) {
	result, err := s.client.ListFilesByName(dirPath)
	if err != nil {
		return nil, err
	}

	s.cacheEntries(dirPath, result.Folders, result.Files)

	now := time.Now()
	seen := make(map[string]bool)
	var entries []os.FileInfo
	for _, folder := range result.Folders {
		if seen[folder.Name] { continue }
		seen[folder.Name] = true
		mt := folder.CreatedAt
		if mt.IsZero() { mt = now }
		entries = append(entries, &webdavFileInfo{name: folder.Name, modTime: mt, isDir: true})
	}
	for _, file := range result.Files {
		if seen[file.Name] { continue }
		seen[file.Name] = true
		mt := file.UpdatedAt
		if mt.IsZero() { mt = now }
		entries = append(entries, &webdavFileInfo{name: file.Name, size: file.Size, modTime: mt, isDir: false})
	}
	return entries, nil
}

func (s *WebDAVServer) downloadFile(name string) ([]byte, error) {
	s.mu.RLock()
	fi, ok := s.fileInfo[name]
	s.mu.RUnlock()
	if !ok {
		parentDir := path.Dir(name)
		if parentDir == "." {
			parentDir = ""
		}
		if _, err := s.listDir(parentDir); err != nil {
			return nil, fmt.Errorf("list parent: %w", err)
		}
		s.mu.RLock()
		fi, ok = s.fileInfo[name]
		s.mu.RUnlock()
		if !ok {
			return nil, os.ErrNotExist
		}
	}

	if fi.StoragePath != "res54_distributed" {
		return nil, fmt.Errorf("unsupported storage: %s", fi.StoragePath)
	}

	return s.downloadRes54(fi)
}

func (s *WebDAVServer) downloadRes54(fi *api.FileMetadata) ([]byte, error) {
	cacheKey := cache.CacheKey(fi.ID, 0)
	if s.cache != nil {
		if data, ok := s.cache.Get(cacheKey); ok {
			return data, nil
		}
	}

	var tags api.FileTags
	if fi.Tags != nil {
		tagsRaw := fi.Tags
		// Tags may be ["{json_string}"] — array with stringified JSON
		if len(fi.Tags) > 0 && fi.Tags[0] == '[' {
			var arr []json.RawMessage
			if err := json.Unmarshal(fi.Tags, &arr); err == nil && len(arr) > 0 {
				tagsRaw = arr[0]
				// If arr[0] is a string, unwrap it
				var s string
				if err := json.Unmarshal(tagsRaw, &s); err == nil {
					tagsRaw = []byte(s)
				}
			}
		}
		// Or it might be a bare JSON string "{...}"
		if len(tagsRaw) > 0 && tagsRaw[0] == '"' {
			var s string
			if err := json.Unmarshal(tagsRaw, &s); err == nil {
				tagsRaw = []byte(s)
			}
		}
		if err := json.Unmarshal(tagsRaw, &tags); err != nil {
			return nil, fmt.Errorf("parse tags: %w", err)
		}
	}

	if len(tags.Chunks) == 0 {
		return nil, fmt.Errorf("no chunks found")
	}

	var downloadChunks []api.DownloadChunkRequest
	for _, c := range tags.Chunks {
		downloadChunks = append(downloadChunks, api.DownloadChunkRequest{
			Path:   c.Path,
			Index:  c.Index,
			Bucket: c.Bucket,
		})
	}

	urls, err := s.client.ResolveDownloadURLs(downloadChunks)
	if err != nil {
		return nil, fmt.Errorf("resolve download URLs: %w", err)
	}

	urlMap := make(map[int]string)
	for _, u := range urls {
		urlMap[u.Index] = u.DownloadURL
	}

	chunkBlobs := make([][]byte, 0, len(tags.Chunks))
	for _, c := range tags.Chunks {
		downloadURL, ok := urlMap[c.Index]
		if !ok {
			return nil, fmt.Errorf("missing URL for chunk %d", c.Index)
		}
		resp, err := s.httpClient.Get(downloadURL)
		if err != nil {
			return nil, fmt.Errorf("download chunk %d: %w", c.Index, err)
		}
		chunkData, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read chunk %d: %w", c.Index, err)
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
				allData = append(allData, blob...)
				continue
			}
			allData = append(allData, plain...)
		}
	case s.encryptor != nil && s.encryptor.IsEnabled() && fi.Encrypted:
		blob := make([]byte, 0, 4096)
		for _, b := range chunkBlobs {
			blob = append(blob, b...)
		}
		decrypted, err := s.encryptor.Decrypt(blob)
		if err != nil {
			return nil, fmt.Errorf("decrypt: %w", err)
		}
		allData = decrypted
	default:
		for _, b := range chunkBlobs {
			allData = append(allData, b...)
		}
	}

	if s.cache != nil {
		if err := s.cache.Set(cacheKey, allData); err != nil {
			log.Printf("Cache write failed: %v", err)
		}
	}

	return allData, nil
}

type WebDAVDir struct {
	server  *WebDAVServer
	name    string
	entries []os.FileInfo
	offset  int
}

func (d *WebDAVDir) Close() error                       { return nil }
func (d *WebDAVDir) Read(p []byte) (int, error)         { return 0, io.EOF }
func (d *WebDAVDir) Write(p []byte) (int, error)        { return 0, fmt.Errorf("cannot write to directory") }
func (d *WebDAVDir) Seek(offset int64, whence int) (int64, error) { return 0, fmt.Errorf("cannot seek directory") }

func (d *WebDAVDir) Readdir(count int) ([]os.FileInfo, error) {
	if count <= 0 {
		entries := d.entries[d.offset:]
		d.offset = len(d.entries)
		return entries, nil
	}
	end := d.offset + count
	if end > len(d.entries) {
		end = len(d.entries)
	}
	entries := d.entries[d.offset:end]
	d.offset = end
	return entries, nil
}

func (d *WebDAVDir) Stat() (os.FileInfo, error) {
	return &webdavFileInfo{name: d.name, isDir: true, modTime: time.Now()}, nil
}

type WebDAVFile struct {
	server   *WebDAVServer
	name     string
	data     []byte
	offset   int64
	flag     int
	size     int64
	isNew    bool
	existing *api.FileMetadata
	parentID string
	closed   bool
}


// statFor resolves file metadata by DAV path.
func (s *WebDAVServer) statFor(name string) (*api.FileMetadata, error) {
	s.mu.RLock()
	fi, ok := s.fileInfo[name]
	s.mu.RUnlock()
	if ok {
		return fi, nil
	}
	parentDir := path.Dir(name)
	if parentDir == "." { parentDir = "" }
	if _, err := s.listDir(parentDir); err != nil {
		return nil, err
	}
	s.mu.RLock()
	fi, ok = s.fileInfo[name]
	s.mu.RUnlock()
	if !ok {
		return nil, os.ErrNotExist
	}
	return fi, nil
}

type byteRange struct{ start, length int64 }

func parseByteRange(h string, total int64) (byteRange, bool) {
	var br byteRange
	if !strings.HasPrefix(h, "bytes=") || total <= 0 {
		return br, false
	}
	spec := strings.TrimPrefix(h, "bytes=")
	if i := strings.Index(spec, ","); i >= 0 { spec = spec[:i] } // first range only
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 { return br, false }
	if parts[0] == "" {
		// suffix form bytes=-N
		n, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || n <= 0 { return br, false }
		if n > total { n = total }
		br.start = total - n
		br.length = n
		return br, true
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= total { return br, false }
	end := total - 1
	if parts[1] != "" {
		e, e2 := strconv.ParseInt(parts[1], 10, 64)
		if e2 != nil || e < start { return br, false }
		if e < end { end = e }
	}
	br.start = start
	br.length = end - start + 1
	return br, true
}

// readRangeLazy fetches and decrypts only the chunks covering [start,len).
func (s *WebDAVServer) readRangeLazy(fi *api.FileMetadata, start, length int64) ([]byte, error) {
	if length <= 0 { return []byte{}, nil }
	if start+length > fi.Size { length = fi.Size - start }

	var tags api.FileTags
	tagsRaw := fi.Tags
	if len(tagsRaw) > 0 && tagsRaw[0] == '[' {
		var arr []json.RawMessage
		if err := json.Unmarshal(tagsRaw, &arr); err == nil && len(arr) > 0 {
			tagsRaw = arr[0]
			var str string
			if err := json.Unmarshal(tagsRaw, &str); err == nil { tagsRaw = []byte(str) }
		}
	} else if len(tagsRaw) > 0 && tagsRaw[0] == '"' {
		var str string
		if err := json.Unmarshal(tagsRaw, &str); err == nil { tagsRaw = []byte(str) }
	}
	if err := json.Unmarshal(tagsRaw, &tags); err != nil {
		return nil, fmt.Errorf("parse tags: %w", err)
	}

	keyRaw := platform.DeriveKey(tags.EncryptionKey)

	idx := stream.ChunksCovering(start, length)

	// Serve cached chunks immediately; collect misses for parallel fetch.
	parts := make([][]byte, len(idx))
	var missIdx []int
	for j, ci := range idx {
		var ck string
		if s.cache != nil {
			ck = cache.CacheKey(fi.ID, ci)
			if d, ok := s.cache.Get(ck); ok {
			parts[j] = sliceChunk(d, start, length, ci)
			continue
		}
		}
		missIdx = append(missIdx, ci)
	}

	if len(missIdx) > 0 {
		reqs := make([]api.DownloadChunkRequest, len(missIdx))
		for j, ci := range missIdx {
			reqs[j] = api.DownloadChunkRequest{
				Path: tags.Chunks[ci].Path, Index: ci, Bucket: tags.Chunks[ci].Bucket,
			}
		}
		resolved, err := s.client.ResolveDownloadURLs(reqs)
		if err != nil {
			return nil, fmt.Errorf("resolve: %w", err)
		}
		umap := make(map[int]string, len(resolved))
		for _, u := range resolved {
			umap[u.Index] = u.DownloadURL
		}

		fetched := make([][]byte, len(missIdx))
		var fetchMu sync.Mutex
		errFetch := stream.FetchChunksParallel(s.httpClient, umap, missIdx, func(i int, blob []byte) {
			var plain []byte
			if hasPlatformKey(tags) {
				if p, derr := platform.DecryptChunk(platform.DeriveKey(tags.EncryptionKey), blob); derr == nil {
					plain = p
				} else {
					plain = blob
				}
			} else if s.encryptor != nil && s.encryptor.IsEnabled() && fi.Encrypted {
				if p, derr := s.encryptor.Decrypt(blob); derr == nil {
					plain = p
				} else {
					plain = blob
				}
			} else {
				plain = blob
			}
			s.cache.Set(cache.CacheKey(fi.ID, i), plain)
			fetchMu.Lock()
			for j, ci := range missIdx {
				if ci == i {
					fetched[j] = plain
					break
				}
			}
			fetchMu.Unlock()
		})
		if errFetch != nil {
			return nil, errFetch
		}

		fj := 0
		for j, ci := range idx {
			if parts[j] == nil && fj < len(missIdx) && missIdx[fj] == ci {
				parts[j] = fetched[fj]
				fj++
			}
		}
	}

	_ = keyRaw

	out := make([]byte, 0, length)
	for _, p := range parts {
		if p == nil {
			continue
		}
		out = append(out, p...)
	}
	if max := fi.Size - start; int64(len(out)) > max && max >= 0 {
		out = out[:max]
	}
	return out, nil
}

func sliceChunk(chunk []byte, off, total int64, idx int) []byte {
	cs := int64(stream.ChunkSize)
	cStart := int64(idx) * cs
	sOff := off - cStart
	if sOff < 0 { sOff = 0 }
	eOff := (off + total) - cStart
	if eOff > int64(len(chunk)) { eOff = int64(len(chunk)) }
	if sOff >= int64(len(chunk)) || sOff >= eOff { return nil }
	return chunk[sOff:eOff]
}

func hasPlatformKey(t api.FileTags) bool {
	k := t.EncryptionKey
	return k != "" && k != "managed_key" && k != "sha256:byok_encrypted" &&
		k != "byok_encrypted" && k != "byok_protected"
}


func (f *WebDAVFile) Close() error {
	if f.closed { return nil }
	f.closed = true

	if f.data == nil || len(f.data) == 0 {
		return nil
	}

	mimeType := "application/octet-stream"
	if ext := filepath.Ext(f.name); ext != "" {
		if ct, ok := mimeTypes[ext]; ok {
			mimeType = ct
		}
	}

	folderID := ""
	if f.parentID != "" {
		f.server.mu.RLock()
		fi, ok := f.server.folderInfo[f.parentID]
		f.server.mu.RUnlock()
		if ok {
			folderID = fi.ID
		}
	}

	log.Printf("Upload %s (%d bytes)", f.name, len(f.data))

	keyHex, err := platform.GenerateKeyHex()
	if err != nil {
		return fmt.Errorf("generate key: %w", err)
	}

	chunksNeeded := (len(f.data) + platform.ChunkSize() - 1) / platform.ChunkSize()
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

	urls, err := f.server.client.GetUploadURLs(chunkPaths, int64(len(f.data)))
	if err != nil {
		return fmt.Errorf("get upload URLs: %w", err)
	}

	chunkSize := platform.ChunkSize()
	for _, u := range urls {
		start := u.Index * chunkSize
		end := start + chunkSize
		if end > len(f.data) {
			end = len(f.data)
		}
		chunk := f.data[start:end]

		if keyHex != "" {
			key := platform.DeriveKey(keyHex)
			sealed, err := platform.EncryptChunk(key, chunk)
			if err != nil {
				return fmt.Errorf("encrypt chunk %d: %w", u.Index, err)
			}
			chunk = sealed
		}

		req, err := http.NewRequest("PUT", u.UploadURL, bytes.NewReader(chunk))
		if err != nil {
			return fmt.Errorf("create PUT request: %w", err)
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := f.server.httpClient.Do(req)
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
		if end > len(f.data) {
			end = len(f.data)
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
		FileName:      path.Base(f.name),
		FileType:      mimeType,
		FileSize:      int64(len(f.data)),
		EncryptionKey: keyHex,
		Created:       time.Now().UTC().Format(time.RFC3339),
		Chunks:        chunkMetas,
	}
	tagsJSON, _ := json.Marshal(tags)
	tagsArray, _ := json.Marshal([]json.RawMessage{tagsJSON})

	fileRecord := api.UploadFileRecord{
		Name:         path.Base(f.name),
		Type:         mimeType,
		Size:         int64(len(f.data)),
		StoragePath:  "res54_distributed",
		MimeType:     mimeType,
		Encrypted:    true,
		ParentFolder: folderID,
		Tags:         tagsArray,
	}

	created, err := f.server.client.CreateFileRecord(fileRecord)
	if err != nil {
		return fmt.Errorf("create file record: %w", err)
	}

	f.server.mu.Lock()
	f.server.nameToID[f.name] = created.ID
	f.server.fileInfo[f.name] = created
	f.server.mu.Unlock()

	return nil
}

func (f *WebDAVFile) Read(p []byte) (n int, err error) {
	if f.data == nil {
		if f.existing != nil {
			data, dlErr := f.server.downloadFile(f.name)
			if dlErr != nil {
				return 0, dlErr
			}
			f.data = data
			f.size = int64(len(data))
		}
		if f.data == nil {
			return 0, io.EOF
		}
	}
	if f.offset >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n = copy(p, f.data[f.offset:])
	f.offset += int64(n)
	return n, nil
}

func (f *WebDAVFile) Write(p []byte) (n int, err error) {
	if f.data == nil {
		f.data = make([]byte, 0, len(p))
	}
	start := f.offset
	end := start + int64(len(p))
	if end > int64(len(f.data)) {
		newData := make([]byte, end)
		copy(newData, f.data)
		f.data = newData
	}
	n = copy(f.data[start:end], p)
	f.offset = end
	return n, nil
}

func (f *WebDAVFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		f.offset = int64(len(f.data)) + offset
	}
	return f.offset, nil
}

func (f *WebDAVFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, nil
}

func (f *WebDAVFile) Stat() (os.FileInfo, error) {
	sz := int64(0)
	if f.data != nil {
		sz = int64(len(f.data))
	}
	if f.size > sz {
		sz = f.size
	}
	mt := time.Now()
	if f.existing != nil && !f.existing.UpdatedAt.IsZero() {
		mt = f.existing.UpdatedAt
	}
	return &webdavFileInfo{name: path.Base(f.name), size: sz, modTime: mt}, nil
}

type stripErrorWriter struct {
	http.ResponseWriter
	wroteBody bool
}

func (sw *stripErrorWriter) Write(b []byte) (int, error) {
	s := string(b)
	if sw.wroteBody && (strings.HasPrefix(s, "Internal Server Error") ||
		strings.HasPrefix(s, "Not Found") ||
		strings.HasPrefix(s, "Method Not Allowed")) {
		return len(b), nil
	}
	sw.wroteBody = true
	return sw.ResponseWriter.Write(b)
}

var mimeTypes = map[string]string{
	".txt":    "text/plain",
	".html":   "text/html",
	".htm":    "text/html",
	".css":    "text/css",
	".js":     "application/javascript",
	".json":   "application/json",
	".xml":    "application/xml",
	".pdf":    "application/pdf",
	".png":    "image/png",
	".jpg":    "image/jpeg",
	".jpeg":   "image/jpeg",
	".gif":    "image/gif",
	".svg":    "image/svg+xml",
	".mp3":    "audio/mpeg",
	".mp4":    "video/mp4",
	".zip":    "application/zip",
	".gz":     "application/gzip",
	".tar":    "application/x-tar",
	".doc":    "application/msword",
	".docx":   "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	".xls":    "application/vnd.ms-excel",
	".xlsx":   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	".ppt":    "application/vnd.ms-powerpoint",
	".pptx":   "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	".c":      "text/x-c",
	".cpp":    "text/x-c++",
	".go":     "text/x-go",
	".py":     "text/x-python",
	".rs":     "text/x-rust",
	".java":   "text/x-java",
	".epub":   "application/epub+zip",
	".csv":    "text/csv",
	".md":     "text/markdown",
}
