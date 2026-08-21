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

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}

	queryParams := ""
	if encryptionKey != "" {
		queryParams = "?encryption_key=" + encryptionKey
	}

	req, err := http.NewRequest("POST", c.apiURL("/files"+queryParams), &body)
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

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
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
		reqURL += "&folder_id=" + folderName
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
