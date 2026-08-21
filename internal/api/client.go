package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path"
	"time"
)

type Client struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

type FileMetadata struct {
	ID            string    `json:"id"`
	UserID        string    `json:"user_id"`
	Name          string    `json:"name"`
	Type          string    `json:"type"`
	Size          int64     `json:"size"`
	StoragePath   string    `json:"storage_path"`
	Encrypted     bool      `json:"encrypted"`
	EncryptionKey string    `json:"encryption_key,omitempty"`
	GitHubRepo    string    `json:"github_repo,omitempty"`
	ParentFolder  string    `json:"parent_folder,omitempty"`
	WorkspaceID   string    `json:"workspace_id,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type UploadResponse struct {
	Success   bool         `json:"success"`
	File      FileMetadata `json:"file"`
	Message   string       `json:"message"`
	Encryption struct {
		Mode string `json:"mode"`
	} `json:"encryption"`
}

type ListFilesResponse struct {
	Success bool           `json:"success"`
	Files   []FileMetadata `json:"files"`
}

type ErrorResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		BaseURL: baseURL,
		APIKey:  apiKey,
		HTTPClient: &http.Client{
			Timeout: 5 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (c *Client) apiURL(functionName string) string {
	return fmt.Sprintf("%s/functions/v1/%s", c.BaseURL, functionName)
}

func (c *Client) headers() map[string]string {
	return map[string]string{
		"apikey":     c.APIKey,
		"Authorization": fmt.Sprintf("Bearer %s", c.APIKey),
	}
}

func (c *Client) UploadFile(filePath string, data []byte, mimeType string, encryptionKey string) (*UploadResponse, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	part, err := writer.CreateFormFile("file", path.Base(filePath))
	if err != nil {
		return nil, fmt.Errorf("create form file: %w", err)
	}

	if _, err := part.Write(data); err != nil {
		return nil, fmt.Errorf("write file data: %w", err)
	}

	if encryptionKey != "" {
		if err := writer.WriteField("encryption_key", encryptionKey); err != nil {
			return nil, fmt.Errorf("write encryption key: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequest("POST", c.apiURL("api-file-upload"), &body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("upload request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil {
			return nil, fmt.Errorf("upload failed (%d): %s", resp.StatusCode, errResp.Error)
		}
		return nil, fmt.Errorf("upload failed (%d): %s", resp.StatusCode, string(respBody))
	}

	var uploadResp UploadResponse
	if err := json.Unmarshal(respBody, &uploadResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &uploadResp, nil
}

func (c *Client) GetFile(fileID string) (*FileMetadata, error) {
	req, err := http.NewRequest("POST", c.apiURL("secure-file-metadata"), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")

	body := map[string]interface{}{
		"action": "get",
		"fileId": fileID,
	}
	bodyBytes, _ := json.Marshal(body)
	req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	req.ContentLength = int64(len(bodyBytes))

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
	req, err := http.NewRequest("POST", c.apiURL("secure-file-metadata"), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")

	body := map[string]interface{}{
		"action":     "list",
		"parentFolder": folderID,
	}
	bodyBytes, _ := json.Marshal(body)
	req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	req.ContentLength = int64(len(bodyBytes))

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

	var result struct {
		Success bool           `json:"success"`
		Files   []FileMetadata `json:"files"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &ListFilesResponse{Success: result.Success, Files: result.Files}, nil
}

func (c *Client) DeleteFile(fileID string) error {
	req, err := http.NewRequest("POST", c.apiURL("api-file-delete"), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}

	req.URL.Path = path.Join(req.URL.Path, fileID)

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
	req, err := http.NewRequest("POST", c.apiURL("secure-file-metadata"), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")

	body := map[string]interface{}{
		"action": "download",
		"fileId": fileID,
	}
	bodyBytes, _ := json.Marshal(body)
	req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	req.ContentLength = int64(len(bodyBytes))

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

func (c *Client) CreateFolder(name, parentID string) (*FileMetadata, error) {
	req, err := http.NewRequest("POST", c.apiURL("secure-file-metadata"), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")

	body := map[string]interface{}{
		"name":         name,
		"type":         "folder",
		"size":         0,
		"storagePath":  "folder",
		"encrypted":    false,
		"parentFolder": parentID,
	}
	bodyBytes, _ := json.Marshal(body)
	req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	req.ContentLength = int64(len(bodyBytes))

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("create folder request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil {
			return nil, fmt.Errorf("create folder failed (%d): %s", resp.StatusCode, errResp.Error)
		}
		return nil, fmt.Errorf("create folder failed (%d): %s", resp.StatusCode, string(respBody))
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

func (c *Client) RenameFile(fileID, newName string) error {
	req, err := http.NewRequest("POST", c.apiURL("secure-file-metadata"), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	for k, v := range c.headers() {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")

	body := map[string]interface{}{
		"action": "rename",
		"fileId": fileID,
		"name":   newName,
	}
	bodyBytes, _ := json.Marshal(body)
	req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	req.ContentLength = int64(len(bodyBytes))

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

func ComputeSHA256(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}
