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
	"os/exec"
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
	apiToken      string
	anonKey       string
	decryptorURL  string
	decryptorAuth string
	listCache map[string][]os.FileInfo
	listTime  map[string]time.Time
	listMu    sync.Mutex
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

func (s *WebDAVServer) SetAuth(token, anon string) {
	s.apiToken = token
	s.anonKey = anon
}

// SetDecryptor wires the local decryption daemon (reserved port 6763).
// When set, every content read is proxied through it and the Go binary
// never touches crypto itself.
func (s *WebDAVServer) SetDecryptor(url, auth string) {
	s.decryptorURL = url
	s.decryptorAuth = auth
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
				log.Printf("WebDAV error: %s %s: %v", r.Method, r.URL.Path, err)
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

			// All SquidFS files are remote and end-to-end encrypted: always
			// proxy through the decryptor daemon (or platform fallback), which
			// returns fully decrypted plaintext for any storage backend.
			if fi != nil {
				full, cliErr := s.downloadFile(name)
				if cliErr != nil {
					log.Printf("GET %s error: %v", name, cliErr)
					http.Error(w, "download failed", 500)
					return
				}
				ct := mimeTypes[path.Ext(name)]
				if ct == "" {
					ct = "application/octet-stream"
				}
				w.Header().Set("Content-Type", ct)
				w.Header().Set("Accept-Ranges", "bytes")

				status := 200
				data := full
				if rangeHdr != "" {
					if br, ok := parseByteRange(rangeHdr, int64(len(full))); ok {
						data = full[br.start : br.start+br.length]
						w.Header().Set("Content-Range",
							fmt.Sprintf("bytes %d-%d/%d", br.start, br.start+br.length-1, len(full)))
						status = 206
					}
				}
				w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
				w.WriteHeader(status)
				if r.Method == "GET" { w.Write(data) }
				return
			}
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
	if s.listCache == nil {
		s.listCache = make(map[string][]os.FileInfo)
	}
	s.listMu.Lock()
	if ents, ok := s.listCache[dirPath]; ok && time.Since(s.listTime[dirPath]) < 15*time.Second {
		s.listMu.Unlock()
		return ents, nil
	}
	s.listMu.Unlock()

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
	s.listMu.Lock()
	s.listCache[dirPath] = entries
	if s.listTime == nil {
		s.listTime = make(map[string]time.Time)
	}
	s.listTime[dirPath] = time.Now()
	s.listMu.Unlock()

	return entries, nil
}

// lookupMeta resolves a WebDAV path to its FileMetadata.
func (s *WebDAVServer) lookupMeta(name string) *api.FileMetadata {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if fi, ok := s.fileInfo[name]; ok {
		return fi
	}
	return nil
}

// downloadFileBytes fetches decrypted plaintext. Primary path: the local
// decryptor daemon on reserved port 6763 (exact web-frontend crypto chain,
// no rate limits, no server round-trip for keys). Fallback: the platform
// REST API's server-side decrypt endpoint.
func (s *WebDAVServer) downloadFileBytes(fileID string) ([]byte, error) {
	cacheKey := cache.CacheKey(fileID, 0)
	if s.cache != nil {
		if data, ok := s.cache.Get(cacheKey); ok {
			return data, nil
		}
	}
	data, err := s.fetchDecrypted(fileID)
	if err != nil {
		return nil, err
	}
	if s.cache != nil {
		s.cache.Set(cacheKey, data)
	}
	return data, nil
}

func (s *WebDAVServer) fetchDecrypted(fileID string) ([]byte, error) {
	if s.decryptorURL != "" {
		req, err := http.NewRequest("GET", strings.TrimRight(s.decryptorURL, "/")+"/file/"+fileID, nil)
		if err == nil {
			if s.decryptorAuth != "" {
				req.Header.Set("X-SquidFS-Auth", s.decryptorAuth)
			}
			resp, derr := s.httpClient.Do(req)
			if derr == nil {
				data, rerr := io.ReadAll(resp.Body)
				resp.Body.Close()
				if rerr == nil && resp.StatusCode < 300 {
					return data, nil
				}
				if rerr == nil {
					log.Printf("[decryptor] %s: HTTP %d — falling back to API", fileID[:min(8, len(fileID))], resp.StatusCode)
				}
			} else {
				log.Printf("[decryptor] unreachable (%v) — falling back to API", derr)
			}
		}
	}

	url := fmt.Sprintf("https://squidcloud.vercel.app/api/v1/download/%s", fileID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil { return nil, err }
	req.Header.Set("Authorization", "Bearer "+s.apiToken)
	if s.anonKey != "" {
		req.Header.Set("apikey", s.anonKey)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil { return nil, fmt.Errorf("download request: %w", err) }
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil { return nil, fmt.Errorf("read response: %w", err) }
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("download failed: HTTP %d: %s", resp.StatusCode, string(data[:min(len(data), 200)]))
	}
	return data, nil
}

// downloadFile proxies through SquidCloud's REST API which handles all
// decryption server-side. Returns fully decrypted plaintext bytes.
func (s *WebDAVServer) downloadFile(name string) ([]byte, error) {
	fi := s.lookupMeta(name)
	if fi == nil {
		return nil, os.ErrNotExist
	}
	return s.downloadFileBytes(fi.ID)
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
	written  bool // true only after an actual Write() — read-only opens never upload
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


// downloadViaAPI proxies through SquidCloud's download API which handles
// all decryption server-side. Returns fully decrypted plaintext.
func (s *WebDAVServer) downloadViaAPI(fi *api.FileMetadata) ([]byte, error) {
	url := fmt.Sprintf("https://squidcloud.vercel.app/api/v1/download/%s", fi.ID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil { return nil, err }
	req.Header.Set("Authorization", "Bearer "+s.apiToken)
	if s.anonKey != "" { req.Header.Set("apikey", s.anonKey) }
	resp, err := s.httpClient.Do(req)
	if err != nil { return nil, fmt.Errorf("download: %w", err) }
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil { return nil, fmt.Errorf("read response: %w", err) }
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("download failed: HTTP %d: %s", resp.StatusCode, string(data[:min(120,len(data))]))
	}
	return data, nil
}


// downloadViaCLI spawns squidcloudctl storage dl to get fully decrypted content.
func (s *WebDAVServer) downloadViaCLI(fi *api.FileMetadata) ([]byte, error) {
	cliBin := os.Getenv("SQUIDCLOUDCTL_BIN")
	if cliBin == "" {
		for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
			p := filepath.Join(dir, "squidcloudctl")
			if _, err := os.Stat(p); err == nil { cliBin = p; break }
		}
	}
	if cliBin == "" {
		cliBin = os.Getenv("HOME") + "/.squidcloud/bin/squidcloudctl"
		if _, err := os.Stat(cliBin); err != nil {
			return nil, fmt.Errorf("squidcloudctl not found in PATH or ~/.squidcloud/bin")
		}
	}

	tmpFile := fmt.Sprintf("/tmp/squidfs-dl-%d", time.Now().UnixNano())
	defer os.Remove(tmpFile)

	cmd := exec.Command(cliBin, "storage", "dl", fi.ID, "-o", tmpFile)
	cmd.Env = append(os.Environ(),
		"SQUIDCLOUD_NONINTERACTIVE=1",
		"SQUIDFS_DECRYPT_PORT=8066",
	)
	
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("CLI: %v: %s", err, stderr.String()[:min(200, stderr.Len())])
	}

	// CLI saves the file with its original name in cwd
	data, err := os.ReadFile(tmpFile)
	if err != nil {
		return nil, fmt.Errorf("read output: %w", err)
	}
	return data, nil
}


func (f *WebDAVFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true

	// Read-only handles must NEVER trigger uploads — this was the cause of
	// "file manager uploads everything on browse".
	if !f.written {
		return nil
	}

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

	chunksNeeded := (len(f.data) + platform.ChunkSize - 1) / platform.ChunkSize
	if chunksNeeded == 0 {
		chunksNeeded = 1
	}

	keyHex, err := platform.GenerateKeyHex()
	if err != nil {
		return fmt.Errorf("generate key: %w", err)
	}

	chunkPaths := make([]api.ChunkUploadRequest, chunksNeeded)
	for i := 0; i < chunksNeeded; i++ {
		chunkPaths[i] = api.ChunkUploadRequest{
			Path:  fmt.Sprintf("squidfs/%s/%s_%d.bin", time.Now().Format("20060102"), filepath.Base(f.name), i),
			Index: i,
		}
	}

	urls, err := f.server.client.GetUploadURLs(chunkPaths, int64(len(f.data)))
	if err != nil {
		return fmt.Errorf("get upload URLs: %w", err)
	}
	urlMap := make(map[int]api.UploadURLInfo, len(urls))
	for _, u := range urls {
		urlMap[u.Index] = u
	}

	key := platform.DeriveKey(keyHex)
	chunkSize := platform.ChunkSize

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
			if !ok { return }
			st := i * chunkSize
			en := st + chunkSize
			if en > len(f.data) { en = len(f.data) }
			sealed, serr := platform.EncryptChunk(key, f.data[st:en])
			if serr != nil {
				mu.Lock(); if firstErr == nil { firstErr = serr }; mu.Unlock()
				return
			}
			req, rerr := http.NewRequest(http.MethodPut, u.UploadURL, bytes.NewReader(sealed))
			if rerr != nil {
				mu.Lock(); if firstErr == nil { firstErr = rerr }; mu.Unlock()
				return
			}
			req.Header.Set("Content-Type", "application/octet-stream")
			resp, derr := f.server.httpClient.Do(req)
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
		u := urls[i]
		st := i * chunkSize
		en := st + chunkSize
		if en > len(f.data) { en = len(f.data) }
		chunkMetas[i] = api.ChunkMetadata{
			Index: i, TotalChunks: chunksNeeded, Size: en - st, Offset: st,
			Path: u.Path, Bucket: u.Bucket, ClusterID: u.ClusterID,
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
	if fi, err2 := f.server.client.GetFile(created.ID); err2 == nil {
		f.server.fileInfo[f.name] = fi
	}
	f.server.mu.Unlock()

	return nil
}


const lazyReadThreshold = 8 * 1024 * 1024 // >8MB → full API download + slice

func (f *WebDAVFile) Read(p []byte) (n int, err error) {
	// Large read-only files: fetch once via API (server decrypts), serve from memory.
	if !f.written && f.existing != nil && f.existing.Size > lazyReadThreshold && f.offset < f.existing.Size {
		full, rerr := f.server.downloadFileBytes(f.existing.ID)
		if rerr != nil {
			return 0, rerr
		}
		if f.data == nil {
			f.data = full
		}
	}

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
	f.written = true
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
