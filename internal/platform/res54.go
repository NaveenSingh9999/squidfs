// Package platform implements SquidCloud res54 chunk-format interop so files
// written by squidfs decrypt in the dashboard/API and vice versa.
package platform

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

const (
	chunkSize = 2 * 1024 * 1024
)

// ErrNotPlatformChunk indicates the blob is not a res54 envelope (possibly raw
// legacy data written before interop, or already-plaintext).
var ErrNotPlatformChunk = errors.New("not a res54 chunk")

// GenerateKeyHex returns a random 256-bit key as hex — the same shape the web
// client stores in file metadata.
func GenerateKeyHex() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// DeriveKey replicates the platform's key derivation exactly:
//
//	key.slice(0, 32).padEnd(32, '0') interpreted as UTF-8 bytes
//
// (NOT hex-decoded.) This keeps every writer/reader interoperable.
func DeriveKey(keyString string) []byte {
	if len(keyString) > 32 {
		keyString = keyString[:32]
	}
	for len(keyString) < 32 {
		keyString += "0"
	}
	return []byte(keyString)
}

// EncryptChunk seals one chunk in the legacy-compatible envelope:
//
//	[12-byte IV | AES-256-GCM ciphertext+tag]
//
// The platform's legacy fallback path decrypts this format directly.
func EncryptChunk(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	iv := make([]byte, 12)
	if _, err := rand.Read(iv); err != nil {
		return nil, err
	}
	out := make([]byte, 0, 12+len(plaintext)+16)
	out = append(out, iv...)
	out = gcm.Seal(out, iv, plaintext, nil)
	return out, nil
}

// DecryptChunk opens platform envelopes. It tries, in order:
//  1. v2 AEAD envelope: [4B LE AD-length | AD | 12B IV | ciphertext]
//  2. legacy envelope: [12B IV | ciphertext]
//  3. raw passthrough (unencrypted legacy data)
func DecryptChunk(key, blob []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// v2 envelope
	if len(blob) > 4 {
		adLen := binary.LittleEndian.Uint32(blob[:4])
		if adLen >= 8 && int(adLen) <= 128 && len(blob) > int(4+adLen+12) {
			ad := blob[4 : 4+adLen]
			iv := blob[4+adLen : 4+adLen+12]
			ct := blob[4+adLen+12:]
			if plain, err := gcm.Open(nil, iv, ct, ad); err == nil {
				return plain, nil
			}
		}
	}

	// legacy [IV][ct]
	if len(blob) > 12+16 {
		if plain, err := gcm.Open(nil, blob[:12], blob[12:], nil); err == nil {
			return plain, nil
		}
	}

	return nil, fmt.Errorf("%w: %d bytes", ErrNotPlatformChunk, len(blob))
}

// ChunkSize exposes the platform chunk size used for uploads.
func ChunkSize() int { return chunkSize }
