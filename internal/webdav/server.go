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
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/webdav"

	"github.com/NaveenSingh9999/squidfs/internal/api"
	"github.com/NaveenSingh9999/squidfs/internal/cache"
	"github.com/NaveenSingh9999/squidfs/internal/encryption"
)

type WebDAVServer struct {
	handler   *webdav.Handler
	client    *api.Client
	cache     *cache.Cache
	encryptor *encryption.Encryptor
	listener  net.Listener
	port      int
	mu        sync.RWMutex
	nameToID  map[string]string
	fileInfo  map[string]*api.FileMetadata
	folderInfo map[string]*api.FolderMetadata
}

type webdavFileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (fi *webdavFileInfo) Name() string       { return fi.name }
func (fi *webdavFileInfo) Size() int64        { return fi.size }
func (fi *webdavFileInfo) Mode() os.FileMode  { if fi.isDir { return 0755 | os.ModeDir }; return 0644 }
func (fi *webdavFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *webdavFileInfo) IsDir() bool        { return fi.isDir }
func (fi *webdavFileInfo) Sys() interface{}   { return nil }

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
	mux.Handle("/", s.handler)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	s.listener = listener

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received signal, shutting down...")
		s.Stop()
		os.Exit(0)
	}()

	log.Printf("WebDAV server listening on port %d", s.port)
	log.Printf("Mount with: mount -t davfs http://localhost:%d /mnt/squidfs", s.port)

	return http.Serve(listener, mux)
}

func (s *WebDAVServer) Stop() error {
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
		s.nameToID[f.Name] = f.ID
		s.folderInfo[f.Name] = f
	}
	for i := range files {
		f := &files[i]
		s.nameToID[f.Name] = f.ID
		s.fileInfo[f.Name] = f
	}
}

func (s *WebDAVServer) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	parentDir := path.Dir(name)
	folderName := path.Base(name)
	_, err := s.client.CreateFolder(folderName, parentDir)
	return err
}

func (s *WebDAVServer) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	name = strings.TrimPrefix(name, "/")
	name = strings.TrimSuffix(name, "/")

	if name == "" {
		if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
			return nil, fmt.Errorf("cannot write to root")
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
		return &WebDAVFile{
			server: s,
			name:   name,
			size:   fi.Size,
		}, nil
	}

	return nil, os.ErrNotExist
}

func (s *WebDAVServer) RemoveAll(ctx context.Context, name string) error {
	name = strings.TrimPrefix(name, "/")
	s.mu.RLock()
	id, ok := s.nameToID[name]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("file not found: %s", name)
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
		return fmt.Errorf("file not found: %s", oldName)
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
		if len(parts) > 1 {
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
		return nil, os.ErrNotExist
	}

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
	server *WebDAVServer
	name   string
	data   []byte
	offset int64
	flag   int
	size   int64
}

func (f *WebDAVFile) Close() error {
	return nil
}

func (f *WebDAVFile) Read(p []byte) (n int, err error) {
	if f.data == nil {
		return 0, io.EOF
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
		f.data = make([]byte, 0)
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

	data := f.data
	if f.server.encryptor.IsEnabled() {
		encrypted, err := f.server.encryptor.Encrypt(data)
		if err != nil {
			return 0, err
		}
		data = encrypted
	}

	_, err = f.server.client.UploadFile(f.name, data, "application/octet-stream", "")
	if err != nil {
		return 0, err
	}

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
	sz := int64(len(f.data))
	if f.size > sz {
		sz = f.size
	}
	return &webdavFileInfo{
		name: f.name,
		size: sz,
	}, nil
}
