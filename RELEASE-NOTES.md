# squidfs v2.1.0 — Native-speed mount engine

## Highlights
- **Lazy ranged reads** — opening a file no longer downloads it; chunks are
  fetched on demand and served straight into your app (video players can seek
  instantly via HTTP Range/206).
- **Parallel chunk transfers** — up to 8 concurrent chunk PUTs/GETs with retry
  + backoff (previously fully serial).
- **Chunk-level disk cache (500 MB LRU)** — repeated opens/reads hit local disk.
- **Write-back, not write-through** — edits buffer locally (RAM ≤32 MB, then
  spooled to disk) and upload exactly once on close. File managers that touch
  files for thumbnails no longer trigger uploads of everything.
- **Unchanged-upload skip** — identical content is never sent twice.
- **Correct MIME types + res54 interop** — files decrypt in the dashboard and
  the CLI; platform-managed files stream with per-chunk envelopes.

## Platforms
| File | Platform |
|---|---|
| squidfs-linux-amd64 | Linux x86_64 |
| squidfs-linux-arm64 | Linux ARM64 / Termux |
| squidfs-linux-arm | Linux ARM32 |
| squidfs-darwin-amd64 | macOS Intel |
| squidfs-darwin-arm64 | macOS Apple Silicon |
| squidfs-windows-amd64.exe | Windows x86_64 |

All builds are static (CGO disabled) — drop one in your PATH and run.

## Quick start
```bash
squidcloudctl fs webdav        # http://localhost:8077 (works everywhere)
squidcloudctl fs mount /mnt/sq # FUSE where available
```

Verify checksums: `sha256sum -c checksums-sha256.txt`
