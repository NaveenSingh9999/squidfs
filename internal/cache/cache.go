package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type CacheEntry struct {
	Key       string
	Data      []byte
	Size      int64
	CreatedAt time.Time
	AccessAt  time.Time
}

type Cache struct {
	dir       string
	maxSize   int64
	entries   map[string]*CacheEntry
	mu        sync.RWMutex
	hitCount  int64
	missCount int64
}

func NewCache(dir string, maxSize int64) (*Cache, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	c := &Cache{
		dir:     dir,
		maxSize: maxSize,
		entries: make(map[string]*CacheEntry),
	}

	if err := c.loadExisting(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Cache) loadExisting() error {
	return filepath.Walk(c.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}

		key := filepath.Base(path)
		c.entries[key] = &CacheEntry{
			Key:       key,
			Size:      info.Size(),
			CreatedAt: info.ModTime(),
			AccessAt:  info.ModTime(),
		}
		return nil
	})
}

// Has reports whether a fresh entry exists for key.
func (c *Cache) Has(key string) bool {
	_, ok := c.Get(key)
	return ok
}

func (c *Cache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[key]
	if !exists {
		c.missCount++
		return nil, false
	}

	path := filepath.Join(c.dir, entry.Key)
	data, err := os.ReadFile(path)
	if err != nil {
		c.missCount++
		return nil, false
	}

	entry.AccessAt = time.Now()
	c.hitCount++

	return data, true
}

func (c *Cache) Set(key string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	path := filepath.Join(c.dir, key)

	// Check if we need to evict
	if err := c.evictIfNeeded(int64(len(data))); err != nil {
		return err
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write cache file: %w", err)
	}

	c.entries[key] = &CacheEntry{
		Key:       key,
		Data:      data,
		Size:      int64(len(data)),
		CreatedAt: time.Now(),
		AccessAt:  time.Now(),
	}

	return nil
}

func (c *Cache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.entries[key]
	if !exists {
		return nil
	}

	path := filepath.Join(c.dir, entry.Key)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove cache file: %w", err)
	}

	delete(c.entries, key)
	return nil
}

func (c *Cache) evictIfNeeded(needed int64) error {
	var totalSize int64
	for _, entry := range c.entries {
		totalSize += entry.Size
	}

	if totalSize+needed <= c.maxSize {
		return nil
	}

	// Find least recently accessed entries to evict
	type entryWithKey struct {
		key   string
		entry *CacheEntry
	}

	entries := make([]entryWithKey, 0, len(c.entries))
	for k, e := range c.entries {
		entries = append(entries, entryWithKey{k, e})
	}

	// Sort by access time (oldest first)
	for i := 0; i < len(entries)-1; i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].entry.AccessAt.After(entries[j].entry.AccessAt) {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	// Evict until we have enough space
	for _, e := range entries {
		if totalSize+needed <= c.maxSize {
			break
		}

		path := filepath.Join(c.dir, e.key)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			continue
		}

		totalSize -= e.entry.Size
		delete(c.entries, e.key)
	}

	return nil
}

func (c *Cache) Stats() (hitCount, missCount int64, entryCount int, totalSize int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, entry := range c.entries {
		totalSize += entry.Size
	}

	return c.hitCount, c.missCount, len(c.entries), totalSize
}

func (c *Cache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.entries {
		path := filepath.Join(c.dir, key)
		os.Remove(path)
	}

	c.entries = make(map[string]*CacheEntry)
	c.hitCount = 0
	c.missCount = 0

	return nil
}

func CacheKey(fileID string, version int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", fileID, version)))
	return hex.EncodeToString(h[:])
}
