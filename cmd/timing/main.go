package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/NaveenSingh9999/squidfs/internal/api"
)

func main() {
	kb, _ := os.ReadFile(os.Getenv("HOME") + "/.squidcloud/bridge-key.json")
	var j struct{ Key string `json:"key"` }
	json.Unmarshal(kb, &j)
	key := j.Key
	fmt.Println("using key len:", len(key))

	url := "https://aouqcwbdoyrccjcrhzzi.supabase.co/functions/v1/squidfs-bridge"

	cl := api.NewClient(url, key)
	t0 := time.Now()
	r, err := cl.ListFilesByName("")
	fmt.Printf("api.Client: err=%v files=%d t=%v\n", err, len(r.Files), time.Since(t0))

	body := fmt.Sprintf(`{"action":"list","api_key":%q}`, key)
	req, _ := http.NewRequest("POST", url, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err2 := http.DefaultClient.Do(req)
	if err2 == nil {
		t, _ := io.ReadAll(resp.Body)
		fmt.Printf("manual: %d %s\n", resp.StatusCode, string(t)[:min(60, len(t))])
		resp.Body.Close()
	}
}

func min(a, b int) int { if a < b { return a }; return b }
