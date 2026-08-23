// Package stream is the high-performance chunk engine shared by FUSE and
// WebDAV: lazy ranged reads, parallel transfers, retry/backoff.
package stream

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/NaveenSingh9999/squidfs/internal/api"
)

const (
	ChunkSize = 2 * 1024 * 1024 // matches platform.ChunkSize
	Workers   = 8               // parallel chunk transfers
	Retries   = 3
)

// ── chunk math ──

// ChunksCovering returns chunk indices overlapping [off, off+len).
func ChunksCovering(off, length int64) []int {
	if length <= 0 {
		return nil
	}
	first := int(off / ChunkSize)
	last := int((off + length - 1) / ChunkSize)
	out := make([]int, 0, last-first+1)
	for i := first; i <= last; i++ {
		out = append(out, i)
	}
	return out
}

// ── shared plumbing ──

type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

func sleepBackoff(attempt int) {
	d := []time.Duration{500 * time.Millisecond, 1500 * time.Millisecond, 4 * time.Second}
	if attempt < len(d) {
		time.Sleep(d[attempt])
	}
}

// ── parallel download ──

// FetchChunksParallel downloads chunks concurrently. urlsByIndex maps chunk
// index → presigned GET url. onChunk is invoked once per completed chunk
// (already locked — safe for cache writes).
func FetchChunksParallel(
	client *http.Client,
	urlsByIndex map[int]string,
	indices []int,
	onChunk func(index int, data []byte),
) error {
	var mu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, Workers)
	var wg sync.WaitGroup

	for _, i := range indices {
		i := i
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			url, ok := urlsByIndex[i]
			mu.Lock()
			if !ok {
				if firstErr == nil {
					firstErr = fmt.Errorf("missing URL for chunk %d", i)
				}
				mu.Unlock()
				return
			}
			mu.Unlock()

			for attempt := 0; attempt <= Retries; attempt++ {
				resp, err := client.Get(url)
				if err != nil {
					mu.Lock()
					if firstErr == nil && attempt == Retries {
						firstErr = fmt.Errorf("chunk %d: %w", i, err)
					}
					mu.Unlock()
					sleepBackoff(attempt)
					continue
				}
				data, rerr := io.ReadAll(resp.Body)
				status := resp.StatusCode
				resp.Body.Close()
				if rerr != nil {
					mu.Lock()
					if firstErr == nil && attempt == Retries {
						firstErr = fmt.Errorf("chunk %d read: %w", i, rerr)
					}
					mu.Unlock()
					sleepBackoff(attempt)
					continue
				}
				if status >= 300 {
					mu.Lock()
					if firstErr == nil && attempt == Retries {
						firstErr = fmt.Errorf("chunk %d: HTTP %d", i, status)
					}
					mu.Unlock()
					sleepBackoff(attempt)
					continue
				}

				mu.Lock()
				if onChunk != nil {
					onChunk(i, data)
				}
				mu.Unlock()
				return
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// ── parallel upload ──

type UploadChunk struct {
	Index     int
	Data      []byte
	Path      string
	Bucket    string
	ClusterID string
}

type UploadURLInfo = api.UploadURLInfo
type ChunkUploadRequest = api.ChunkUploadRequest
type ChunkMetadataOut = api.ChunkMetadata

// PutChunksParallel uploads all slices concurrently against pre-fetched
// signed PUT urls. Returns chunk metadata sorted by index.
func PutChunksParallel(
	client *http.Client,
	slices [][]byte,
	urls map[int]UploadURLInfo,
	totalChunks int,
	onDone func(index int, n int),
) ([]ChunkMetadataOut, error) {
	metas := make([]ChunkMetadataOut, totalChunks)
	var mu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, Workers)
	var wg sync.WaitGroup

	for i := 0; i < totalChunks; i++ {
		i := i
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			u, ok := urls[i]
			mu.Lock()
			if !ok {
				if firstErr == nil {
					firstErr = fmt.Errorf("no upload URL for chunk %d", i)
				}
				mu.Unlock()
				return
			}
			mu.Unlock()

			slice := slices[i]
			var lastErr error
			for attempt := 0; attempt <= Retries; attempt++ {
				req, rerr := http.NewRequest(http.MethodPut, u.UploadURL, bytes.NewReader(slice))
				if rerr != nil {
					lastErr = rerr
					break
				}
				req.Header.Set("Content-Type", "application/octet-stream")
				resp, derr := client.Do(req)
				if derr != nil {
					lastErr = derr
				} else if resp.StatusCode < 300 {
					resp.Body.Close()
					mu.Lock()
					metas[i] = api.ChunkMetadata{
						Index:       i,
						TotalChunks: totalChunks,
						Size:        len(slice),
						Offset:      i * ChunkSize,
						Path:        u.Path,
						Bucket:      u.Bucket,
						ClusterID:   u.ClusterID,
					}
					mu.Unlock()
					if onDone != nil {
						onDone(i, len(slice))
					}
					return
				} else {
					lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
					resp.Body.Close()
				}
				sleepBackoff(attempt)
			}
			mu.Lock()
			if firstErr == nil {
				firstErr = fmt.Errorf("chunk %d: %v", i, lastErr)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	sort.Slice(metas, func(a, b int) bool { return metas[a].Index < metas[b].Index })
	return metas, nil
}

var _ = sort.Ints // keep sort imported for stable builds
