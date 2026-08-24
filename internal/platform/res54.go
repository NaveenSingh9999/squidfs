// Package platform implements SquidCloud res54 chunk-format interop.
package platform

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"time"
	"errors"
	"fmt"
	"strings"

)

const ChunkSize = 2 * 1024 * 1024
const V3Magic1 = 0x52 // 'R'
const V3Magic2 = 0x45 // 'E'
const V3Magic3 = 0x03

type ChunkVersion int

const (
	V1Legacy ChunkVersion = iota
	V2Aead
	V3HKDF
)

// ErrNotPlatformChunk means the blob doesn't match any known format.
var ErrNotPlatformChunk = errors.New("not a res54 chunk")

func GenerateKeyHex() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// DeriveKey mirrors: key.slice(0,32).padEnd(32,'0') as UTF-8 bytes.
func DeriveKey(keyString string) []byte {
	if len(keyString) > 32 {
		keyString = keyString[:32]
	}
	for len(keyString) < 32 {
		keyString += "0"
	}
	return []byte(keyString)
}

// ── encryption (for SquidFS writes) ──

// EncryptChunkV2 seals one chunk in v2 AEAD format:
// [4B LE ADLen][AD][12B IV][ct+tag]
// The AD is "res54-v2-<timestamp>" matching what res54.ts produces.
func EncryptChunkV2(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	ts := time_now_millis()
	ad := fmt.Sprintf("res54-v2-%d", ts)
	adBytes := []byte(ad)

	iv := make([]byte, 12)
	if _, err := rand.Read(iv); err != nil {
		return nil, err
	}

	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, uint32(len(adBytes)))
	out = append(out, adBytes...)
	out = append(out, iv...)
	out = gcm.Seal(out, iv, plaintext, adBytes)
	return out, nil
}

func time_now_millis() int64 { return nowMilli() }

var nowMilli = func() int64 { return 0 }

func SetTimeHook(fn func() int64) { nowMilli = fn }

// ── decryption ──

// DecryptBlob auto-detects and decrypts using the correct format.
// It tries every known envelope in sequence.
func DecryptBlob(key, blob []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// v3 HKDF: magic bytes [52 45 03] + origLen(4) + IV(12) + ct
	if len(blob) > 3 && blob[0] == V3Magic1 && blob[1] == V3Magic2 && blob[2] == V3Magic3 {
		if len(blob) > 19 {
			iv := blob[7:19]
			ct := blob[19:]
			if plain, err := gcm.Open(nil, iv, ct, nil); err == nil {
				return plain, nil
			}
		}
	}

	// v2 AEAD: [4B LE ADLen][AD][12B IV][ct+tag]
	if len(blob) > 4 {
		adLen := binary.LittleEndian.Uint32(blob[:4])
		if adLen >= 8 && int(adLen) <= 256 && len(blob) > int(4+adLen+12) {
			iv := blob[4+adLen : 4+adLen+12]
			ct := blob[4+adLen+12:]
			if plain, err := gcm.Open(nil, iv, ct, blob[4:4+adLen]); err == nil {
				return plain, nil
			}
		}
	}

	// Legacy [12B IV][ct+tag]
	if len(blob) > 28 {
		if plain, err := gcm.Open(nil, blob[:12], blob[12:], nil); err == nil {
			return plain, nil
		}
	}

	// Raw passthrough — unencrypted data
	return blob, nil
}

// ── multi-chunk header format (large files uploaded via web) ──

type MultiChunkHeader struct {
	Version   int      `json:"version"`
	Chunks    int      `json:"chunks"`
	Ivs       [][]byte `json:"-"`
	IvSingle  []int    `json:"-"`
	RawJSON   string   `json:"-"`
}

// DetectAndDecryptFull handles the multi-chunk JSON-header format used by
// large web uploads where all sub-chunk ciphertexts are concatenated.
func DecryptMultiChunk(key, blob []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	if len(blob) < 4 {
		return blob, nil
	}

	hdrLen := binary.LittleEndian.Uint32(blob[:4])
	if hdrLen == 0 || hdrLen >= 1000 || int(hdrLen)+4 > len(blob) {
		// Not multi-chunk format
		return nil, ErrNotPlatformChunk
	}

	// Parse the JSON header between offset 4 and 4+hdrLen
	headerBytes := blob[4 : 4+hdrLen]
	_ = headerBytes // We don't need to parse the JSON; just skip it

	// Data starts after header
	dataStart := 4 + hdrLen
	if int(dataStart) >= len(blob) {
		return nil, ErrNotPlatformChunk
	}

	// Try standard GCM decrypt on remaining data with various IV positions
	if len(blob[dataStart:]) > 28 {
		if plain, err := gcm.Open(nil, blob[dataStart:dataStart+12], blob[dataStart+12:], nil); err == nil {
			return plain, nil
		}
	}

	return nil, ErrNotPlatformChunk
}

// EncryptChunk seals one chunk in v2 AEAD format: [4B LE ADLen][AD][12B IV][ct+tag]
func EncryptChunk(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil { return nil, err }
	gcm, err := cipher.NewGCM(block)
	if err != nil { return nil, err }

	ts := timeNowMillis()
	ad := fmt.Sprintf("res54-v2-%d", ts)
	adBytes := []byte(ad)

	iv := make([]byte, 12)
	if _, err := rand.Read(iv); err != nil { return nil, err }

	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, uint32(len(adBytes)))
	out = append(out, adBytes...)
	out = append(out, iv...)
	out = gcm.Seal(out, iv, plaintext, adBytes)
	return out, nil
}

var timeNowMillis = func() int64 { return time.Now().UnixMilli() }

// DecryptChunk tries all known res54 envelope formats on a single chunk blob.
func DecryptChunk(key, blob []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil { return nil, err }
	gcm, err := cipher.NewGCM(block)
	if err != nil { return nil, err }

	// v2 AEAD: [4B LE ADLen][AD][12B IV][ct+tag]
	if len(blob) > 4 {
		adLen := binary.LittleEndian.Uint32(blob[:4])
		if adLen >= 8 && int(adLen) <= 256 && len(blob) > int(4+adLen+12) {
			ad := blob[4 : 4+adLen]
			iv := blob[4+adLen : 4+adLen+12]
			ct := blob[4+adLen+12:]
			if plain, e := gcm.Open(nil, iv, ct, ad); e == nil { return plain, nil }
		}
	}

	// Legacy [12B IV][ct+tag]
	if len(blob) > 28 {
		if plain, e := gcm.Open(nil, blob[:12], blob[12:], nil); e == nil { return plain, nil }
	}

	return nil, ErrNotPlatformChunk
}

// EncryptChunkV2 seals one chunk in v2 AEAD envelope format.


// MimeTypeFor returns a MIME type from a filename extension.
func MimeTypeFor(name string) string {
	ext := name
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		ext = strings.ToLower(name[idx:])
	}
	mimeMap := map[string]string{
		".txt": "text/plain", ".md": "text/markdown", ".html": "text/html", ".htm": "text/html",
		".css": "text/css", ".js": "application/javascript", ".ts": "application/typescript",
		".json": "application/json", ".xml": "application/xml", ".yml": "application/x-yaml",
		".yaml": "application/x-yaml", ".toml": "application/toml", ".csv": "text/csv",
		".pdf": "application/pdf", ".zip": "application/zip", ".gz": "application/gzip",
		".tar": "application/x-tar", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
		".gif": "image/gif", ".webp": "image/webp", ".svg": "image/svg+xml", ".ico": "image/x-icon",
		".mp3": "audio/mpeg", ".wav": "audio/wav", ".ogg": "audio/ogg", ".flac": "audio/flac",
		".mp4": "video/mp4", ".webm": "video/webm", ".mov": "video/quicktime", ".mkv": "video/x-matroska",
		".doc": "application/msword", ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		".xls": "application/vnd.ms-excel", ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
		".go": "text/x-go", ".py": "text/x-python", ".rs": "text/rust", ".sh": "text/x-shellscript",
		".sql": "application/sql", ".woff": "font/woff", ".woff2": "font/woff2", ".ttf": "font/ttf",
		".url": "application/internet-shortcut", ".epub": "application/epub+zip",
	}
	if ct, ok := mimeMap[ext]; ok {
		return ct
	}
	return "application/octet-stream"
}
