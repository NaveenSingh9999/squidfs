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
	"net/url"
	"sync"
	"time"
)

type Client struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
	resolvedIP string
	resolvedMu sync.Mutex
}

type FileMetadata struct {
	ID            string    `json:"id"`
	UserID        string    `json:"user_id"`
	Name          string    `json:"name"`
	Type          string    `json:"type"`
	Size          int64     `json:"size"`
	StoragePath   string    `json:"storage_path"`
	MimeType      string    `json:"mime_type"`
	Encrypted     bool      `json:"encrypted"`
	EncryptionKey string    `json:"encryption_key,omitempty"`
	ParentFolder  string    `json:"parent_folder,omitempty"`
	WorkspaceID   string    `json:"workspace_id,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type FolderMetadata struct {
	ID           string    `json:"id"`
	UserID       string    `json:"user_id"`
	Name         string    `json:"name"`
	Path         string    `json:"path"`
	ParentFolder string    `json:"parent_folder,omitempty"`
	WorkspaceID  string    `json:"workspace_id,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

type UploadResponse struct {
	Success bool         `json:"success"`
	File    FileMetadata `json:"file"`
	Message string       `json:"message"`
}

type ListFilesResponse struct {
	Success      bool             `json:"success"`
	Files        []FileMetadata   `json:"files"`
	Folders      []FolderMetadata `json:"folders"`
	TotalFiles   int              `json:"total_files"`
	TotalFolders int              `json:"total_folders"`
	Page         int              `json:"page"`
	PerPage      int              `json:"per_page"`
	TotalPages   int              `json:"total_pages"`
}

type ErrorResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

func NewClient(baseURL, apiKey string) *Client {
	c := &Client{
		BaseURL: baseURL,
		APIKey:  apiKey,
	}

	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: 5 * time.Second}
			for _, server := range []string{"8.8.8.8:53", "1.1.1.1:53", "8.8.4.4:53"} {
				conn, err := d.DialContext(ctx, "udp", server)
				if err == nil {
					return conn, nil
				}
			}
			return d.DialContext(ctx, network, address)
		},
	}

	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Resolver:  resolver,
	}

	c.HTTPClient = &http.Client{
		Timeout: 5 * time.Minute,
		Transport: &http.Transport{
			DialContext:         dialer.DialContext,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	return c
}

func (c *Client) apiURL(subpath string) string {
	return fmt.Sprintf("%s/functions/v1/cloudbliss-api%s", c.BaseURL, subpath)
}

func (c *Client) headers() map[string]string {
	return map[string]string{
		"apikey":           c.APIKey,
		"Authorization":    fmt.Sprintf("Bearer %s", c.APIKey),
		"x-squidcloud-key": c.APIKey,
	}
}

type UploadInitResponse struct {
	Success       bool              `json:"success"`
	UploadID      string            `json:"upload_id"`
	FileID        string            `json:"file_id"`
	EncryptionKey string            `json:"encryption_key"`
	URLs          []UploadURL       `json:"urls"`
	ChunkSize     int               `json:"chunk_size"`
	TotalChunks   int               `json:"total_chunks"`
	BucketAssigns map[string]string `json:"bucket_assignments"`
}

type UploadURL struct {
	Index     int    `json:"index"`
	Path      string `json:"path"`
	UploadURL string `json:"upload_url"`
	Bucket    string `json:"bucket"`
	ClusterID string `json:"cluster_id"`
}

func (c *Client) UploadFile(name string, data []byte, mimeType string, folderID string) (*UploadResponse, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	initBody, _ := json.Marshal(map[string]interface{}{
		"name":      name,
		"type":      mimeType,
		"size":      len(data),
		"folder_id": folderID,
	})
	req, err := http.NewRequest("POST", c.apiURL("/upload"), bytes.NewReader(initBody))
	if err != nil {
		return nil, fmt.Errorf("create init request: %w", err)
	}
	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("init upload: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("init upload failed (%d): %s", resp.StatusCode, string(respBody))
	}
	var initResp UploadInitResponse
	if err := json.Unmarshal(respBody, &initResp); err != nil {
		return nil, fmt.Errorf("decode init response: %w", err)
	}

	chunkSize := initResp.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 512 * 1024
	}
	for i := 0; i < initResp.TotalChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[start:end]

		if i >= len(initResp.URLs) {
			return nil, fmt.Errorf("no upload URL for chunk %d", i)
		}
		putReq, err := http.NewRequest("PUT", initResp.URLs[i].UploadURL, bytes.NewReader(chunk))
		if err != nil {
			return nil, fmt.Errorf("create chunk %d request: %w", i, err)
		}
		putReq.Header.Set("Content-Type", "application/octet-stream")
		putReq.Header.Set("Content-Length", fmt.Sprintf("%d", len(chunk)))

		putResp, err := c.HTTPClient.Do(putReq)
		if err != nil {
			return nil, fmt.Errorf("upload chunk %d: %w", i, err)
		}
		putResp.Body.Close()
		if putResp.StatusCode != http.StatusOK && putResp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(putResp.Body)
			return nil, fmt.Errorf("chunk %d upload failed (%d): %s", i, putResp.StatusCode, string(body))
		}
	}

	completeBody, _ := json.Marshal(map[string]interface{}{
		"upload_id": initResp.UploadID,
	})
	compReq, err := http.NewRequest("POST", c.apiURL("/upload/complete"), bytes.NewReader(completeBody))
	if err != nil {
		return nil, fmt.Errorf("create complete request: %w", err)
	}
	for k, v := range c.headers() {
		compReq.Header.Set(k, v)
	}
	compReq.Header.Set("Content-Type", "application/json")

	compResp, err := c.HTTPClient.Do(compReq)
	if err != nil {
		return nil, fmt.Errorf("complete upload: %w", err)
	}
	defer compResp.Body.Close()
	compBody, _ := io.ReadAll(compResp.Body)
	if compResp.StatusCode != http.StatusOK && compResp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("complete upload failed (%d): %s", compResp.StatusCode, string(compBody))
	}

	var uploadResp UploadResponse
	if err := json.Unmarshal(compBody, &uploadResp); err != nil {
		return &UploadResponse{Success: true, File: FileMetadata{ID: initResp.FileID, Name: name}}, nil
	}
	return &uploadResp, nil
}

func (c *Client) GetFile(fileID string) (*FileMetadata, error) {
	req, err := http.NewRequest("GET", c.apiURL("/files/"+fileID+"/metadata"), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get file request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil {
			return nil, fmt.Errorf("get file failed (%d): %s", resp.StatusCode, errResp.Error)
		}
		return nil, fmt.Errorf("get file failed (%d): %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Success bool         `json:"success"`
		File    FileMetadata `json:"file"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &result.File, nil
}

func (c *Client) ListFiles(folderID string) (*ListFilesResponse, error) {
	reqURL := c.apiURL("/files?per_page=100")
	if folderID != "" && folderID != "root" {
		reqURL += "&folder_id=" + folderID
	}

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list files request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil {
			return nil, fmt.Errorf("list files failed (%d): %s", resp.StatusCode, errResp.Error)
		}
		return nil, fmt.Errorf("list files failed (%d): %s", resp.StatusCode, string(respBody))
	}

	var result ListFilesResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &result, nil
}

func (c *Client) ListFilesByName(folderName string) (*ListFilesResponse, error) {
	reqURL := c.apiURL("/files?per_page=100")
	if folderName != "" {
		reqURL += "&folder_id=" + url.QueryEscape(folderName)
	}

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list files request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil {
			return nil, fmt.Errorf("list files failed (%d): %s", resp.StatusCode, errResp.Error)
		}
		return nil, fmt.Errorf("list files failed (%d): %s", resp.StatusCode, string(respBody))
	}

	var result ListFilesResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &result, nil
}

func (c *Client) DeleteFile(fileID string) error {
	req, err := http.NewRequest("DELETE", c.apiURL("/files/"+fileID), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil {
			return fmt.Errorf("delete failed (%d): %s", resp.StatusCode, errResp.Error)
		}
		return fmt.Errorf("delete failed (%d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (c *Client) DownloadFile(fileID string) ([]byte, error) {
	req, err := http.NewRequest("GET", c.apiURL("/files/"+fileID+"/download"), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("download failed (%d): %s", resp.StatusCode, string(respBody))
	}

	return io.ReadAll(resp.Body)
}

func (c *Client) CreateFolder(name, parentFolder string) (*FolderMetadata, error) {
	body := map[string]interface{}{
		"name": name,
	}
	if parentFolder != "" && parentFolder != "root" {
		body["parent_folder"] = parentFolder
	}

	bodyBytes, _ := json.Marshal(body)

	req, err := http.NewRequest("POST", c.apiURL("/folders"), bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("create folder request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil {
			return nil, fmt.Errorf("create folder failed (%d): %s", resp.StatusCode, errResp.Error)
		}
		return nil, fmt.Errorf("create folder failed (%d): %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Success       bool   `json:"success"`
		ID            string `json:"id"`
		Name          string `json:"name"`
		ParentFolder  string `json:"parent_folder"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &FolderMetadata{
		ID:           result.ID,
		Name:         result.Name,
		ParentFolder: result.ParentFolder,
	}, nil
}

func (c *Client) RenameFile(fileID, newName string) error {
	body := map[string]interface{}{
		"name": newName,
	}
	bodyBytes, _ := json.Marshal(body)

	req, err := http.NewRequest("PUT", c.apiURL("/files/"+fileID+"/rename"), bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("rename request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil {
			return fmt.Errorf("rename failed (%d): %s", resp.StatusCode, errResp.Error)
		}
		return fmt.Errorf("rename failed (%d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (c *Client) MoveFile(fileID, folderID string) error {
	body := map[string]interface{}{
		"folder_id": folderID,
	}
	bodyBytes, _ := json.Marshal(body)

	req, err := http.NewRequest("POST", c.apiURL("/files/"+fileID+"/move"), bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("move request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil {
			return fmt.Errorf("move failed (%d): %s", resp.StatusCode, errResp.Error)
		}
		return fmt.Errorf("move failed (%d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func ComputeSHA256(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}
