package webdav

import (
	"context"
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
	"github.com/NaveenSingh9999/squidfs/internal/encryption"
)

type WebDAVServer struct {
	handler    *webdav.Handler
	client     *api.Client
	cache      *cache.Cache
	encryptor  *encryption.Encryptor
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
	return &WebDAVServer{
		client:     client,
		cache:      cache,
		encryptor:  encryptor,
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
	s.mdns, err = zeroconf.Register(
		hostname,
		"_webdav._tcp",
		"local.",
		s.port,
		txtRecords,
		nil,
	)
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

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" || r.Method == "HEAD" {
			name := strings.TrimPrefix(r.URL.Path, "/")
			name = strings.TrimSuffix(name, "/")
			if name == "" {
				http.Error(w, "cannot GET root", 405)
				return
			}
			data, err := s.downloadFile(name)
			if err != nil {
				http.NotFound(w, r)
				return
			}
			ct := mimeTypes[path.Ext(name)]
			if ct == "" {
				ct = "application/octet-stream"
			}
			w.Header().Set("Content-Type", ct)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			if r.Method == "GET" {
				w.Write(data)
			}
			return
		}
		s.handler.ServeHTTP(w, r)
	})

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.listener = listener

	if err := s.registerMDNS(); err != nil {
		log.Printf("mDNS registration failed (non-fatal): %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received signal, shutting down...")
		s.Stop()
		os.Exit(0)
	}()

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "localhost"
	}
	log.Printf("WebDAV server listening on port %d", s.port)
	log.Printf("  Direct:  http://%s:%d", hostname, s.port)
	log.Printf("  davfs:   mount -t davfs http://%s:%d /mnt/squidfs", hostname, s.port)
	log.Printf("  Finder:  Go to Connect to Server → http://%s:%d", hostname, s.port)
	log.Printf("  File Manager: smb://%s:%d or dav://%s:%d", hostname, s.port, hostname, s.port)

	return http.Serve(listener, mux)
}

func (s *WebDAVServer) Stop() error {
	s.unregisterMDNS()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *WebDAVServer) Port() int {
	return s.port
}

func (s *WebDAVServer) cacheEntries(dirPath string, folders []api.FolderMetadata, files []api.FileMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range folders {
		f := &folders[i]
		key := f.Name
		if dirPath != "" {
			key = dirPath + "/" + f.Name
		}
		s.nameToID[key] = f.ID
		s.folderInfo[key] = f
	}
	for i := range files {
		f := &files[i]
		key := f.Name
		if dirPath != "" {
			key = dirPath + "/" + f.Name
		}
		s.nameToID[key] = f.ID
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
		return &WebDAVDir{
			server:  s,
			name:    "/",
			entries: entries,
		}, nil
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
								return &WebDAVDir{
									server:  s,
									name:    subName,
									entries: subEntries,
								}, nil
							}
						}
						return &WebDAVFile{
							server: s,
							name:   name,
							flag:   flag,
						}, nil
					}
				}
			}
			return nil, os.ErrNotExist
		}
		entries, err := s.listDir(topLevel)
		if err != nil {
			return nil, err
		}
		return &WebDAVDir{
			server:  s,
			name:    topLevel,
			entries: entries,
		}, nil
	}

	if isFile {
		if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
			return &WebDAVFile{
				server:   s,
				name:     name,
				flag:     flag,
				isNew:    false,
				existing: fi,
			}, nil
		}
		return &WebDAVFile{
			server:   s,
			name:     name,
			flag:     flag,
			size:     fi.Size,
			existing: fi,
		}, nil
	}

	if flag&(os.O_CREATE|os.O_WRONLY|os.O_RDWR) != 0 {
		return &WebDAVFile{
			server:   s,
			name:     name,
			flag:     flag,
			isNew:    true,
			parentID: topLevel,
		}, nil
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

	if name == "" {
		return &webdavFileInfo{
			name:    "/",
			isDir:   true,
			modTime: time.Now(),
		}, nil
	}

	parts := strings.SplitN(name, "/", 2)
	topLevel := parts[0]

	s.mu.RLock()
	fi, isFile := s.fileInfo[topLevel]
	fdi, isFolder := s.folderInfo[topLevel]
	s.mu.RUnlock()

	if isFile && len(parts) == 1 {
		return &webdavFileInfo{
			name:    fi.Name,
			size:    fi.Size,
			modTime: fi.UpdatedAt,
			isDir:   false,
		}, nil
	}

	if isFolder {
		if len(parts) > 1 && parts[1] != "" {
			subName := parts[1]
			subParts := strings.SplitN(subName, "/", 2)
			s.mu.RLock()
			sfi, sfIsFile := s.fileInfo[subParts[0]]
			smu, sfIsFolder := s.folderInfo[subParts[0]]
			s.mu.RUnlock()
			if sfIsFile && len(subParts) == 1 {
				return &webdavFileInfo{
					name:    sfi.Name,
					size:    sfi.Size,
					modTime: sfi.UpdatedAt,
					isDir:   false,
				}, nil
			}
			if sfIsFolder {
				return &webdavFileInfo{
					name:    smu.Name,
					modTime: smu.CreatedAt,
					isDir:   true,
				}, nil
			}
			return &webdavFileInfo{
				name:    subParts[0],
				modTime: time.Now(),
				isDir:   false,
			}, nil
		}
		return &webdavFileInfo{
			name:    fdi.Name,
			modTime: fdi.CreatedAt,
			isDir:   true,
		}, nil
	}

	return &webdavFileInfo{
		name:    path.Base(name),
		modTime: time.Now(),
		isDir:   false,
	}, nil
}

func (s *WebDAVServer) listDir(dirPath string) ([]os.FileInfo, error) {
	result, err := s.client.ListFilesByName(dirPath)
	if err != nil {
		return nil, err
	}

	s.cacheEntries(dirPath, result.Folders, result.Files)

	var entries []os.FileInfo
	for _, folder := range result.Folders {
		entries = append(entries, &webdavFileInfo{
			name:    folder.Name,
			modTime: folder.CreatedAt,
			isDir:   true,
		})
	}
	for _, file := range result.Files {
		entries = append(entries, &webdavFileInfo{
			name:    file.Name,
			size:    file.Size,
			modTime: file.UpdatedAt,
			isDir:   false,
		})
	}
	return entries, nil
}

func (s *WebDAVServer) downloadFile(name string) ([]byte, error) {
	s.mu.RLock()
	id, ok := s.nameToID[name]
	s.mu.RUnlock()
	if !ok {
		log.Printf("downloadFile: %q not in nameToID", name)
		return nil, os.ErrNotExist
	}
	log.Printf("downloadFile: %q -> id=%s", name, id)

	cacheKey := cache.CacheKey(id, 0)
	if data, ok := s.cache.Get(cacheKey); ok {
		return data, nil
	}

	data, err := s.client.DownloadFile(id)
	if err != nil {
		return nil, err
	}

	if s.encryptor.IsEnabled() {
		decrypted, err := s.encryptor.Decrypt(data)
		if err != nil {
			return nil, err
		}
		data = decrypted
	}

	if err := s.cache.Set(cacheKey, data); err != nil {
		log.Printf("Failed to cache file %s: %v", name, err)
	}
	return data, nil
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
	return &webdavFileInfo{
		name:  d.name,
		isDir: true,
	}, nil
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

func (f *WebDAVFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true

	if f.data == nil || len(f.data) == 0 {
		return nil
	}

	mimeType := "application/octet-stream"
	if ext := filepath.Ext(f.name); ext != "" {
		mimeType = mimeTypes[ext]
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
	}

	folderID := ""
	if f.parentID != "" {
		s := f.server
		s.mu.RLock()
		fi, ok := s.folderInfo[f.parentID]
		s.mu.RUnlock()
		if ok {
			folderID = fi.ID
		}
	}

	log.Printf("Uploading %s (%d bytes) to folder %q", f.name, len(f.data), folderID)
	uploadResp, err := f.server.client.UploadFile(path.Base(f.name), f.data, mimeType, folderID)
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}
	log.Printf("Upload complete: %s (id=%s)", f.name, uploadResp.File.ID)

	f.server.mu.Lock()
	f.server.nameToID[f.name] = uploadResp.File.ID
	f.server.fileInfo[f.name] = &api.FileMetadata{
		ID:           uploadResp.File.ID,
		Name:         path.Base(f.name),
		Size:         int64(len(f.data)),
		MimeType:     mimeType,
		ParentFolder: f.parentID,
		UpdatedAt:    time.Now(),
	}
	f.server.mu.Unlock()

	return nil
}

func (f *WebDAVFile) Read(p []byte) (n int, err error) {
	log.Printf("Read(%s): data=%v existing=%v offset=%d", f.name, f.data != nil, f.existing != nil, f.offset)
	if f.data == nil {
		if f.existing != nil {
			log.Printf("Read: downloading %s (id=%s)", f.name, f.existing.ID)
			data, dlErr := f.server.downloadFile(f.name)
			if dlErr != nil {
				log.Printf("Read: download failed: %v", dlErr)
				return 0, dlErr
			}
			f.data = data
			f.size = int64(len(data))
			log.Printf("Read: downloaded %d bytes", len(data))
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
	fi := &webdavFileInfo{
		name: path.Base(f.name),
		size: sz,
	}
	log.Printf("Stat(%s): size=%d data=%v", f.name, sz, f.data != nil)
	return fi, nil
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
