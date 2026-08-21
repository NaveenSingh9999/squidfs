<div align="center">
  <h1>squidfs</h1>
  <p><strong>Mount your <a href="https://squidcloud.vercel.app">SquidCloud</a> storage as a local filesystem</strong></p>

  [![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go)](https://golang.org/)
  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
  [![Release](https://img.shields.io/github/v/release/NaveenSingh9999/squidfs)](https://github.com/NaveenSingh9999/squidfs/releases)

  Linux · macOS · Windows · Termux/Android
</div>

---

## What is squidfs?

squidfs mounts your SquidCloud files as a local disk. Once mounted, your cloud files appear in your file manager, terminal, and every application — just like a USB drive plugged into your computer.

- **Browse** your SquidCloud files in Nautilus, Finder, Explorer, or any file manager
- **Open** files directly in any application (VS Code, Vim, Excel, etc.)
- **Copy, move, delete** files using standard OS commands
- **Edit** files in place — changes sync back to SquidCloud automatically
- **Zero-knowledge encryption** — your files stay encrypted end-to-end

---

## Quick Start

### 1. Get your API key

1. Sign in at [squidcloud.vercel.app](https://squidcloud.vercel.app)
2. Go to **Settings → API Keys**
3. Click **Generate Key**
4. Copy the key (it starts with `cb_`)

### 2. Download squidfs

Grab the binary for your platform from the [**latest release**](https://github.com/NaveenSingh9999/squidfs/releases/latest):

| Platform | File | Architecture |
|----------|------|-------------|
| Linux | `squidfs-linux-amd64` | x86_64 |
| Linux | `squidfs-linux-arm64` | ARM64 |
| macOS | `squidfs-darwin-amd64` | Intel |
| macOS | `squidfs-darwin-arm64` | Apple Silicon |
| Windows | Build from source | See below |
| Termux | `squidfs-linux-arm64` | ARM64 |

### 3. Make it executable (Linux/macOS)

```bash
chmod +x squidfs-linux-amd64
sudo mv squidfs-linux-amd64 /usr/local/bin/squidfs
```

### 4. Mount your storage

```bash
squidfs -key cb_your_api_key_here
```

That's it. Your SquidCloud files are now at `/mnt/squidfs` (Linux) or `/Volumes/squidfs` (macOS).

---

## Installation

### Pre-built Binaries (Recommended)

Download from [Releases](https://github.com/NaveenSingh9999/squidfs/releases/latest), extract, and place in your `$PATH`.

### Homebrew (macOS/Linux)

```bash
# Coming soon — build from source for now
```

### Go Install

```bash
go install github.com/NaveenSingh9999/squidfs/cmd/squidfs@latest
```

### Build from Source

```bash
git clone https://github.com/NaveenSingh9999/squidfs.git
cd squidfs
make install
```

Requires Go 1.22+.

---

## Usage

### Linux / macOS (FUSE)

```bash
squidfs -key cb_your_api_key_here
```

Files appear at `/mnt/squidfs`.

Custom mount point:

```bash
squidfs -key cb_your_api_key_here -mount ~/cloud
```

### Windows

```bash
squidfs -key cb_your_api_key_here -webdav
```

Then map as a network drive:

```cmd
net use Z: http://localhost:8080
```

Or in PowerShell:

```powershell
New-PSDrive -Name "Z" -PSProvider FileSystem -Root "http://localhost:8080"
```

### Termux / Android

```bash
squidfs -key cb_your_api_key_here -webdav
```

Then mount with davfs2:

```bash
pkg install davfs2
mkdir -p /mnt/squidfs
mount -t davfs http://localhost:8080 /mnt/squidfs
```

### Docker

```bash
docker run -it --device /dev/fuse --cap-add SYS_ADMIN \
  -v /mnt/squidfs:/mnt/squidfs \
  squidfs -key cb_your_api_key_here
```

---

## Command Line Reference

```
squidfs [flags]

Flags:
  -key        SquidCloud API key (required, format: cb_...)
  -url        SquidCloud project URL (default: built-in)
  -mount      Mount point path (default: /mnt/squidfs or /Volumes/squidfs)
  -cache-dir  Cache directory (default: ~/.squidfs/cache)
  -cache-size Cache size in bytes (default: 104857600 = 100MB)
  -byok       BYOK encryption passphrase for zero-knowledge access
  -webdav     Use WebDAV mode instead of FUSE
  -port       WebDAV server port (default: 8080)
  -version    Print version and exit
  -help       Show help
```

### Examples

```bash
# Basic mount
squidfs -key cb_abc123...

# Mount at custom path with 1GB cache
squidfs -key cb_abc123... -mount ~/mycloud -cache-size 1073741824

# Mount with BYOK encryption (zero-knowledge)
squidfs -key cb_abc123... -byok "my-secret-passphrase"

# WebDAV server on custom port
squidfs -key cb_abc123... -webdav -port 9090

# Check version
squidfs -version
```

---

## BYOK Encryption

squidfs supports **Bring Your Own Key** (BYOK) encryption. When enabled:

- All files are encrypted/decrypted **locally** on your machine
- Your encryption key **never** leaves your device
- SquidCloud is cryptographically unable to read your files
- Uses AES-256-GCM with PBKDF2 key derivation (600,000 iterations)

```bash
squidfs -key cb_abc123... -byok "my-secret-passphrase"
```

### V3 Encryption (Advanced)

squidfs supports SquidCloud's V3 HKDF key hierarchy:

- **Per-file keys** derived from your master key using HKDF
- **Per-chunk keys** derived from file key for each 2-8MB chunk
- **Deterministic IVs** derived from HKDF (not random)
- **AEAD** with authenticated additional data per chunk

> **Warning:** If you lose your BYOK passphrase, your files cannot be recovered. Keep a secure backup.

---

## Caching

squidfs caches files locally for fast repeated access:

- **Default location:** `~/.squidfs/cache/`
- **Default size:** 100MB
- **Eviction:** LRU (Least Recently Used)

### Adjust cache size

```bash
squidfs -key cb_abc123... -cache-size 536870912    # 512MB
squidfs -key cb_abc123... -cache-size 1073741824   # 1GB
```

### Clear cache

```bash
rm -rf ~/.squidfs/cache/
```

### Use a different cache directory

```bash
squidfs -key cb_abc123... -cache-dir /tmp/squidfs-cache
```

---

## Platform Requirements

### Linux

FUSE 3 is required:

```bash
# Debian / Ubuntu
sudo apt-get install fuse3
sudo usermod -aG fuse $USER
# Log out and back in

# Fedora / RHEL
sudo dnf install fuse3

# Arch Linux
sudo pacman -S fuse3

# openSUSE
sudo zypper install fuse3
```

### macOS

[macFUSE](https://osxfuse.github.io/) is required:

```bash
brew install --cask macfuse
```

You may need to approve the kernel extension in **System Preferences → Security & Privacy**.

### Windows

WebDAV mode is used by default. For FUSE support, install [WinFSP](https://winfsp.dev/).

### Termux / Android

WebDAV mode only (FUSE is not available on Android).

---

## Troubleshooting

### "permission denied" on mount

Your user needs FUSE permissions:

```bash
# Linux
sudo usermod -aG fuse $USER
# Log out and back in

# macOS
# Reinstall macFUSE and approve in System Preferences
```

### Files not showing up

1. Verify your API key is correct (starts with `cb_`)
2. Check your internet connection
3. Try refreshing: `ls -la /mnt/squidfs`

### Slow performance

1. Increase cache size: `-cache-size 1073741824` (1GB)
2. Use FUSE mode instead of WebDAV when possible
3. Check your network speed

### "connection refused" on WebDAV

Make sure squidfs is running. The WebDAV server must be active for Windows/Termux mounts.

### Mount point already in use

```bash
# Linux
fusermount -u /mnt/squidfs

# macOS
umount /Volumes/squidfs
```

---

## How It Works

```
Your Application
      │
      ▼
┌─────────────┐
│   squidfs   │  FUSE / WebDAV filesystem
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ SquidCloud  │  Res54 distributed storage
│     API     │  (encrypted chunks across B2/S3)
└─────────────┘
```

1. **squidfs** creates a virtual filesystem using FUSE (Linux/macOS) or WebDAV (Windows/Termux)
2. When you access a file, squidfs fetches it from SquidCloud's Res54 storage
3. Files are encrypted at rest — BYOK mode keeps keys client-side only
4. A local cache speeds up repeated access
5. When you modify a file, changes sync back to SquidCloud

---

## Security

| Feature | Details |
|---------|---------|
| Encryption at rest | AES-256-GCM |
| Key derivation | PBKDF2 (600K iterations) or HKDF (V3) |
| Transport | HTTPS only |
| BYOK | Zero-knowledge — key never leaves your device |
| API keys | Scoped (read/write/delete), hashed with SHA-256 |
| Audit logging | Every API request logged |
| No data selling | Never |
| No AI training | Never |

---

## Contributing

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -am 'Add my feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

## Support

- **Issues:** [GitHub Issues](https://github.com/NaveenSingh9999/squidfs/issues)
- **SquidCloud:** [squidcloud.vercel.app](https://squidcloud.vercel.app)
- **Email:** hellosquidcloud@gmail.com

---

<div align="center">
  <p>Built for <a href="https://squidcloud.vercel.app">SquidCloud</a></p>
</div>
