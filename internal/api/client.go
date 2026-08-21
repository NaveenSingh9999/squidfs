package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

type Client struct {
	bridgeURL  string
	apiKey     string
	httpClient *http.Client
	userID     string
	mu         sync.RWMutex
}

type BridgeResponse struct {
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Files   []FileMetadata  `json:"files,omitempty"`
	Folders []FolderMetadata `json:"folders,omitempty"`
	File    *FileMetadata   `json:"file,omitempty"`
	Folder  *FolderMetadata `json:"folder,omitempty"`
	URLs    []UploadURLInfo `json:"urls,omitempty"`
}

type FileMetadata struct {
	ID            string          `json:"id"`
	UserID        string          `json:"user_id"`
	Name          string          `json:"name"`
	Type          string          `json:"type"`
	Size          int64           `json:"size"`
	StoragePath   string          `json:"storage_path"`
	MimeType      string          `json:"mime_type"`
	Encrypted     bool            `json:"encrypted"`
	EncryptionKey string          `json:"encryption_key"`
	ParentFolder  string          `json:"parent_folder"`
	Tags          json.RawMessage `json:"tags"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	IsDeleted     bool            `json:"is_deleted"`
}

type FolderMetadata struct {
	ID           string    `json:"id"`
	UserID       string    `json:"user_id"`
	Name         string    `json:"name"`
	Path         string    `json:"path"`
	ParentFolder string    `json:"parent_folder"`
	CreatedAt    time.Time `json:"created_at"`
}

type UploadURLInfo struct {
	Path      string `json:"path"`
	Index     int    `json:"index"`
	UploadURL string `json:"uploadUrl"`
	DownloadURL string `json:"downloadUrl,omitempty"`
	ClusterID string `json:"clusterId"`
	Bucket    string `json:"bucket"`
}

type ChunkMetadata struct {
	Index      int    `json:"index"`
	TotalChunks int   `json:"totalChunks"`
	Size       int    `json:"size"`
	Offset     int    `json:"offset"`
	Repo       string `json:"repo"`
	Path       string `json:"path"`
	ClusterID  string `json:"clusterId"`
	Bucket     string `json:"bucket"`
}

type FileTags struct {
	FileName string          `json:"fileName"`
	FileType string          `json:"fileType"`
	FileSize int64           `json:"fileSize"`
	Chunks   []ChunkMetadata `json:"chunks"`
}

func NewClient(bridgeURL, apiKey string) *Client {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: 5 * time.Second}
			for _, server := range []string{"8.8.8.8:53", "1.1.1.1:53"} {
				conn, err := d.DialContext(ctx, "udp", server)
				if err == nil {
					return conn, nil
				}
			}
			return d.DialContext(ctx, network, address)
		},
	}

	return &Client{
		bridgeURL: bridgeURL,
		apiKey:    apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   10 * time.Second,
					KeepAlive: 30 * time.Second,
					Resolver:  resolver,
				}).DialContext,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (c *Client) callBridge(action string, params map[string]interface{}) (*BridgeResponse, error) {
	params["api_key"] = c.apiKey
	params["action"] = action

	body, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", c.bridgeURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("apikey", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFvdXFjd2jkb3lyY2NqY3JoepppIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDI5NjAyMTEsImV4cCI6MTk5ODUzNjIxMX0.zMWVYnM2zGEBMVJMBib5RnU4MQPfBOKmNXpU31xBVlI")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bridge request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var result BridgeResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if result.Error != "" {
		return nil, fmt.Errorf("bridge error: %s", result.Error)
	}

	return &result, nil
}

func (c *Client) ListFiles(folderID string) (*ListFilesResponse, error) {
	params := map[string]interface{}{
		"folder_id": folderID,
	}
	result, err := c.callBridge("list", params)
	if err != nil {
		return nil, err
	}
	return &ListFilesResponse{
		Success: true,
		Files:   result.Files,
		Folders: result.Folders,
	}, nil
}

type ListFilesResponse struct {
	Success bool           `json:"success"`
	Files   []FileMetadata `json:"files"`
	Folders []FolderMetadata `json:"folders"`
}

func (c *Client) ListFilesByName(folderName string) (*ListFilesResponse, error) {
	return c.ListFiles(folderName)
}

func (c *Client) GetFile(fileID string) (*FileMetadata, error) {
	params := map[string]interface{}{
		"file_id": fileID,
	}
	result, err := c.callBridge("file_meta", params)
	if err != nil {
		return nil, err
	}
	return result.File, nil
}

func (c *Client) CreateFolder(name, parentFolder string) (*FolderMetadata, error) {
	params := map[string]interface{}{
		"name":          name,
		"parent_folder": parentFolder,
	}
	result, err := c.callBridge("create_folder", params)
	if err != nil {
		return nil, err
	}
	return result.Folder, nil
}

func (c *Client) DeleteFile(fileID string) error {
	params := map[string]interface{}{
		"file_id": fileID,
	}
	_, err := c.callBridge("delete", params)
	return err
}

func (c *Client) RenameFile(fileID, newName string) error {
	params := map[string]interface{}{
		"file_id": fileID,
		"name":    newName,
	}
	_, err := c.callBridge("rename", params)
	return err
}

func (c *Client) MoveFile(fileID, folderID string) error {
	params := map[string]interface{}{
		"file_id":       fileID,
		"parent_folder": folderID,
	}
	_, err := c.callBridge("move", params)
	return err
}

func (c *Client) GetUploadURLs(chunks []ChunkUploadRequest, totalSize int64) ([]UploadURLInfo, error) {
	type chunkReq struct {
		Path  string `json:"path"`
		Index int    `json:"index"`
	}
	var chunkReqs []chunkReq
	for _, ch := range chunks {
		chunkReqs = append(chunkReqs, chunkReq{Path: ch.Path, Index: ch.Index})
	}
	params := map[string]interface{}{
		"chunks":     chunkReqs,
		"total_size": totalSize,
	}
	result, err := c.callBridge("get_upload_urls", params)
	if err != nil {
		return nil, err
	}
	return result.URLs, nil
}

type ChunkUploadRequest struct {
	Path  string
	Index int
}

func (c *Client) ResolveDownloadURLs(chunks []DownloadChunkRequest) ([]UploadURLInfo, error) {
	type chunkReq struct {
		Path   string `json:"path"`
		Index  int    `json:"index"`
		Bucket string `json:"bucket,omitempty"`
	}
	var chunkReqs []chunkReq
	for _, ch := range chunks {
		chunkReqs = append(chunkReqs, chunkReq{
			Path:   ch.Path,
			Index:  ch.Index,
			Bucket: ch.Bucket,
		})
	}
	params := map[string]interface{}{
		"chunks": chunkReqs,
	}
	result, err := c.callBridge("resolve_download", params)
	if err != nil {
		return nil, err
	}
	return result.URLs, nil
}

type DownloadChunkRequest struct {
	Path   string
	Index  int
	Bucket string
}

func (c *Client) CreateFileRecord(file UploadFileRecord) (*FileMetadata, error) {
	params := map[string]interface{}{
		"name":           file.Name,
		"type":           file.MimeType,
		"size":           file.Size,
		"storage_path":   file.StoragePath,
		"encrypted":      file.Encrypted,
		"encryption_key": file.EncryptionKey,
		"parent_folder":  file.ParentFolder,
		"tags":           file.Tags,
	}
	result, err := c.callBridge("create_file_record", params)
	if err != nil {
		return nil, err
	}
	return result.File, nil
}

type UploadFileRecord struct {
	Name           string
	Type           string
	Size           int64
	StoragePath    string
	MimeType       string
	Encrypted      bool
	EncryptionKey  string
	ParentFolder   string
	Tags           json.RawMessage
}

func ComputeSHA256(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}
