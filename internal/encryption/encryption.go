package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"golang.org/x/crypto/hkdf"
	"golang.org/x/crypto/pbkdf2"
)

const (
	V1Magic = 0x00
	V2Magic = 0x01
	V3Magic = 0x524503

	KeyLength    = 32
	IVLength     = 12
	SaltLength   = 32
	Iterations   = 600000
	TagPrefix    = "SQUID_BYOK_V1"
	V3FileMaster = "v3-file-master"
	V3FileHMAC   = "v3-file-hmac"
	V3Chunk      = "v3-chunk"
	V3IV         = "v3-iv"
	V3ChunkHeader = 19
)

type Encryptor struct {
	key       []byte
	userId    string
	isEnabled bool
}

func NewEncryptor(password string, userId string) (*Encryptor, error) {
	if password == "" {
		return &Encryptor{isEnabled: false}, nil
	}

	salt := []byte(TagPrefix + userId)
	key := pbkdf2.Key([]byte(password), salt, Iterations, KeyLength, sha256.New)

	return &Encryptor{
		key:       key,
		userId:    userId,
		isEnabled: true,
	}, nil
}

func (e *Encryptor) IsEnabled() bool {
	return e.isEnabled
}

func (e *Encryptor) Encrypt(data []byte) ([]byte, error) {
	if !e.isEnabled {
		return data, nil
	}

	iv := make([]byte, IVLength)
	if _, err := rand.Read(iv); err != nil {
		return nil, fmt.Errorf("generate IV: %w", err)
	}

	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	ciphertext := gcm.Seal(nil, iv, data, nil)

	result := make([]byte, 0, 1+4+len(iv)+len(ciphertext))
	result = append(result, byte(V2Magic))

	aadLen := make([]byte, 4)
	binary.BigEndian.PutUint32(aadLen, 0)
	result = append(result, aadLen...)
	result = append(result, iv...)
	result = append(result, ciphertext...)

	return result, nil
}

func (e *Encryptor) Decrypt(data []byte) ([]byte, error) {
	if !e.isEnabled {
		return data, nil
	}

	if len(data) < 1+4+IVLength+aes.BlockSize {
		return nil, fmt.Errorf("data too short")
	}

	version := data[0]
	switch version {
	case V1Magic:
		return e.decryptV1(data)
	case V2Magic:
		return e.decryptV2(data)
	default:
		if len(data) >= 3 && data[0] == 0x52 && data[1] == 0x45 && data[2] == 0x03 {
			return nil, fmt.Errorf("V3 encryption requires master key")
		}
		return nil, fmt.Errorf("unknown encryption version: %d", version)
	}
}

func (e *Encryptor) decryptV1(data []byte) ([]byte, error) {
	if len(data) < IVLength+aes.BlockSize {
		return nil, fmt.Errorf("V1 data too short")
	}

	iv := data[:IVLength]
	ciphertext := data[IVLength:]

	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	plaintext, err := gcm.Open(nil, iv, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return plaintext, nil
}

func (e *Encryptor) decryptV2(data []byte) ([]byte, error) {
	if len(data) < 1+4+IVLength+aes.BlockSize {
		return nil, fmt.Errorf("V2 data too short")
	}

	offset := 1
	aadLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	var aad []byte
	if aadLen > 0 {
		aad = data[offset : offset+aadLen]
		offset += aadLen
	}

	iv := data[offset : offset+IVLength]
	offset += IVLength

	ciphertext := data[offset:]

	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	plaintext, err := gcm.Open(nil, iv, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return plaintext, nil
}

func (e *Encryptor) DeriveFileKey(masterKey []byte, fileId string) ([]byte, []byte, error) {
	fileKey, err := e.hkdfExpand(masterKey, []byte(V3FileMaster+fileId), KeyLength)
	if err != nil {
		return nil, nil, fmt.Errorf("derive file key: %w", err)
	}

	hmacKey, err := e.hkdfExpand(masterKey, []byte(V3FileHMAC+fileId), KeyLength)
	if err != nil {
		return nil, nil, fmt.Errorf("derive HMAC key: %w", err)
	}

	return fileKey, hmacKey, nil
}

func (e *Encryptor) DeriveChunkKey(fileKey []byte, index int) ([]byte, []byte, error) {
	indexBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(indexBytes, uint32(index))

	chunkKey, err := e.hkdfExpand(fileKey, append([]byte(V3Chunk), indexBytes...), KeyLength)
	if err != nil {
		return nil, nil, fmt.Errorf("derive chunk key: %w", err)
	}

	iv, err := e.hkdfExpand(fileKey, append([]byte(V3IV), indexBytes...), IVLength)
	if err != nil {
		return nil, nil, fmt.Errorf("derive chunk IV: %w", err)
	}

	return chunkKey, iv, nil
}

func (e *Encryptor) hkdfExpand(key, info []byte, length int) ([]byte, error) {
	result := make([]byte, length)
	hkdfReader := hkdf.New(sha256.New, key, nil, info)
	if _, err := io.ReadFull(hkdfReader, result); err != nil {
		return nil, err
	}

	return result, nil
}

func (e *Encryptor) EncryptChunkV3(fileKey []byte, fileId string, userId string, chunkIndex int, totalChunks int, data []byte) ([]byte, error) {
	chunkKey, iv, err := e.DeriveChunkKey(fileKey, chunkIndex)
	if err != nil {
		return nil, err
	}

	aad := fmt.Sprintf("res54-v3|%s|%s|%d|%d", userId, fileId, chunkIndex, totalChunks)

	block, err := aes.NewCipher(chunkKey)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	ciphertext := gcm.Seal(nil, iv, data, []byte(aad))

	originalLen := make([]byte, 4)
	binary.BigEndian.PutUint32(originalLen, uint32(len(data)))

	result := make([]byte, 0, V3ChunkHeader+len(ciphertext))
	result = append(result, 0x52, 0x45, 0x03)
	result = append(result, originalLen...)
	result = append(result, iv...)
	result = append(result, ciphertext...)

	return result, nil
}

func (e *Encryptor) DecryptChunkV3(fileKey []byte, fileId string, userId string, chunkIndex int, totalChunks int, data []byte) ([]byte, error) {
	if len(data) < V3ChunkHeader {
		return nil, fmt.Errorf("V3 chunk too short")
	}

	if data[0] != 0x52 || data[1] != 0x45 || data[2] != 0x03 {
		return nil, fmt.Errorf("invalid V3 magic bytes")
	}

	originalLen := binary.BigEndian.Uint32(data[3:7])
	iv := data[7:19]
	ciphertext := data[19:]

	chunkKey, _, err := e.DeriveChunkKey(fileKey, chunkIndex)
	if err != nil {
		return nil, err
	}

	aad := fmt.Sprintf("res54-v3|%s|%s|%d|%d", userId, fileId, chunkIndex, totalChunks)

	block, err := aes.NewCipher(chunkKey)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	plaintext, err := gcm.Open(nil, iv, ciphertext, []byte(aad))
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	if uint32(len(plaintext)) != originalLen {
		return nil, fmt.Errorf("length mismatch: expected %d, got %d", originalLen, len(plaintext))
	}

	return plaintext, nil
}

func (e *Encryptor) WrapPerFileKey(fileKey []byte, masterKey []byte) ([]byte, []byte, error) {
	iv := make([]byte, IVLength)
	if _, err := rand.Read(iv); err != nil {
		return nil, nil, fmt.Errorf("generate IV: %w", err)
	}

	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, fmt.Errorf("create GCM: %w", err)
	}

	wrappedKey := gcm.Seal(nil, iv, fileKey, nil)
	return wrappedKey, iv, nil
}

func (e *Encryptor) UnwrapPerFileKey(wrappedKey []byte, iv []byte, masterKey []byte) ([]byte, error) {
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	fileKey, err := gcm.Open(nil, iv, wrappedKey, nil)
	if err != nil {
		return nil, fmt.Errorf("unwrap key: %w", err)
	}

	return fileKey, nil
}

func (e *Encryptor) ComputeManifestHMAC(fileKey []byte, fileId string, totalChunks int) ([]byte, error) {
	h := hmac.New(sha256.New, fileKey)
	h.Write([]byte(fmt.Sprintf("manifest|%s|%d", fileId, totalChunks)))
	return h.Sum(nil), nil
}

func (e *Encryptor) VerifyManifestHMAC(fileKey []byte, fileId string, totalChunks int, expectedMAC []byte) bool {
	computed, err := e.ComputeManifestHMAC(fileKey, fileId, totalChunks)
	if err != nil {
		return false
	}
	return hmac.Equal(computed, expectedMAC)
}

func DetectVersion(data []byte) string {
	if len(data) == 0 {
		return "unknown"
	}

	if len(data) >= 3 && data[0] == 0x52 && data[1] == 0x45 && data[2] == 0x03 {
		return "v3"
	}

	if data[0] == V2Magic {
		return "v2"
	}

	if data[0] == V1Magic {
		return "v1"
	}

	if strings.HasPrefix(string(data), "U2FsdGVkX1") {
		return "v1"
	}

	return "unknown"
}
