package fs

import (
	"context"
	"fmt"
	"log"
	"path"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/NaveenSingh9999/squidfs/internal/api"
	"github.com/NaveenSingh9999/squidfs/internal/cache"
	"github.com/NaveenSingh9999/squidfs/internal/encryption"
)

type SquidFS struct {
	client    *api.Client
	cache     *cache.Cache
	encryptor *encryption.Encryptor
	Server    *fuse.Server
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
	encVer  string
	mu      sync.RWMutex
}

func New(client *api.Client, cache *cache.Cache, encryptor *encryption.Encryptor) *SquidFS {
	return &SquidFS{
		client:    client,
		cache:     cache,
		encryptor: encryptor,
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

	data, err := f.squidfs.client.DownloadFile(f.id)
	if err != nil {
		return fmt.Errorf("download file: %w", err)
	}

	if fileMeta.Encrypted && f.squidfs.encryptor.IsEnabled() {
		version := encryption.DetectVersion(data)
		switch version {
		case "v3":
			log.Printf("V3 encryption detected for %s", f.id)
		case "v2":
			decrypted, err := f.squidfs.encryptor.Decrypt(data)
			if err != nil {
				return fmt.Errorf("decrypt V2 file: %w", err)
			}
			data = decrypted
		case "v1":
			decrypted, err := f.squidfs.encryptor.Decrypt(data)
			if err != nil {
				return fmt.Errorf("decrypt V1 file: %w", err)
			}
			data = decrypted
		}
	}

	if err := f.squidfs.cache.Set(cacheKey, data); err != nil {
		log.Printf("Failed to cache file %s: %v", f.id, err)
	}

	f.data = data
	f.size = int64(len(data))
	return nil
}

func (f *File) upload() error {
	data := f.data

	if f.squidfs.encryptor.IsEnabled() {
		encrypted, err := f.squidfs.encryptor.Encrypt(data)
		if err != nil {
			return fmt.Errorf("encrypt file: %w", err)
		}
		data = encrypted
	}

	resp, err := f.squidfs.client.UploadFile(f.name, data, "application/octet-stream", "")
	if err != nil {
		return fmt.Errorf("upload file: %w", err)
	}

	f.id = resp.File.ID

	cacheKey := cache.CacheKey(f.id, 0)
	if err := f.squidfs.cache.Set(cacheKey, f.data); err != nil {
		log.Printf("Failed to cache file %s: %v", f.id, err)
	}

	return nil
}

func (s *SquidFS) loadDir(dir *Dir) {
	result, err := s.client.ListFiles(dir.id)
	if err != nil {
		log.Printf("Failed to load dir %s: %v", dir.id, err)
		return
	}

	for _, file := range result.Files {
		if file.Type == "folder" {
			subDir := &Dir{
				squidfs: s,
				id:      file.ID,
				name:    file.Name,
				path:    file.ParentFolder,
				entries: make(map[string]*fs.Inode),
			}
			child := dir.NewInode(context.Background(), subDir, fs.StableAttr{Mode: fuse.S_IFDIR})
			dir.entries[file.Name] = child
		} else {
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
}
