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
	files     map[string]*FileInfo
}

type FileInfo struct {
	Name    string
	Size    int64
	ModTime time.Time
	IsDir   bool
}

func NewServer(client *api.Client, cache *cache.Cache, encryptor *encryption.Encryptor, port int) *WebDAVServer {
	return &WebDAVServer{
		client:    client,
		cache:     cache,
		encryptor: encryptor,
		port:      port,
		files:     make(map[string]*FileInfo),
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

func (s *WebDAVServer) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	_, err := s.client.CreateFolder(name, "")
	return err
}

func (s *WebDAVServer) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		return &WebDAVFile{
			server: s,
			name:   name,
			flag:   flag,
		}, nil
	}

	data, err := s.downloadFile(name)
	if err != nil {
		return nil, err
	}

	return &WebDAVFile{
		server: s,
		name:   name,
		data:   data,
		flag:   flag,
	}, nil
}

func (s *WebDAVServer) RemoveAll(ctx context.Context, name string) error {
	return s.client.DeleteFile(name)
}

func (s *WebDAVServer) Rename(ctx context.Context, oldName, newName string) error {
	return s.client.RenameFile(oldName, newName)
}

func (s *WebDAVServer) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	file, err := s.client.GetFile(name)
	if err != nil {
		return nil, err
	}

	return &webdavFileInfo{
		name:    file.Name,
		size:    file.Size,
		modTime: file.UpdatedAt,
		isDir:   file.Type == "folder",
	}, nil
}

func (s *WebDAVServer) downloadFile(name string) ([]byte, error) {
	cacheKey := cache.CacheKey(name, 0)
	if data, ok := s.cache.Get(cacheKey); ok {
		return data, nil
	}

	data, err := s.client.DownloadFile(name)
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

type webdavFileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (fi *webdavFileInfo) Name() string      { return fi.name }
func (fi *webdavFileInfo) Size() int64       { return fi.size }
func (fi *webdavFileInfo) Mode() os.FileMode { return 0644 }
func (fi *webdavFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *webdavFileInfo) IsDir() bool       { return fi.isDir }
func (fi *webdavFileInfo) Sys() interface{}  { return nil }

type WebDAVFile struct {
	server *WebDAVServer
	name   string
	data   []byte
	offset int64
	flag   int
}

func (f *WebDAVFile) Close() error {
	return nil
}

func (f *WebDAVFile) Read(p []byte) (n int, err error) {
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

	cacheKey := cache.CacheKey(f.name, 0)
	if err := f.server.cache.Set(cacheKey, f.data); err != nil {
		log.Printf("Failed to cache file %s: %v", f.name, err)
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
	return &webdavFileInfo{
		name: f.name,
		size: int64(len(f.data)),
	}, nil
}
