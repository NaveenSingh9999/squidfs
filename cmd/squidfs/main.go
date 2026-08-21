package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/NaveenSingh9999/squidfs/internal/api"
	"github.com/NaveenSingh9999/squidfs/internal/cache"
	"github.com/NaveenSingh9999/squidfs/internal/encryption"
	squidfs "github.com/NaveenSingh9999/squidfs/internal/fs"
	"github.com/NaveenSingh9999/squidfs/internal/mount"
	"github.com/NaveenSingh9999/squidfs/internal/webdav"
)

const (
	defaultCacheSize = 100 * 1024 * 1024
	defaultCacheDir  = ".squidfs/cache"
	defaultWebDAVPort = 8080
	defaultBaseURL   = "https://aouqcwbdoyrccjcrhzzi.supabase.co"
)

var (
	version   = "1.0.0"
	buildDate = "unknown"
)

func main() {
	var (
		baseURL     string
		apiKey      string
		mountPoint  string
		cacheDir    string
		cacheSize   int64
		byokKey     string
		webdavMode  bool
		webdavPort  int
		showVersion bool
		help        bool
	)

	flag.StringVar(&baseURL, "url", defaultBaseURL, "SquidCloud project URL")
	flag.StringVar(&apiKey, "key", "", "SquidCloud API key (cb_ prefix)")
	flag.StringVar(&mountPoint, "mount", "", "Mount point path")
	flag.StringVar(&cacheDir, "cache-dir", defaultCacheDir, "Cache directory")
	flag.Int64Var(&cacheSize, "cache-size", defaultCacheSize, "Cache size in bytes")
	flag.StringVar(&byokKey, "byok", "", "BYOK encryption passphrase")
	flag.BoolVar(&webdavMode, "webdav", false, "Use WebDAV mode instead of FUSE")
	flag.IntVar(&webdavPort, "port", defaultWebDAVPort, "WebDAV server port")
	flag.BoolVar(&showVersion, "version", false, "Show version")
	flag.BoolVar(&help, "help", false, "Show help")

	flag.Parse()

	if showVersion {
		fmt.Printf("squidfs %s (built %s)\n", version, buildDate)
		os.Exit(0)
	}

	if help || apiKey == "" {
		printUsage()
		os.Exit(0)
	}

	if !strings.HasPrefix(apiKey, "cb_") {
		log.Fatal("Invalid API key format. Must start with 'cb_'")
	}

	client := api.NewClient(baseURL, apiKey)

	cache, err := cache.NewCache(cacheDir, cacheSize)
	if err != nil {
		log.Fatalf("Failed to create cache: %v", err)
	}

	var encryptor *encryption.Encryptor
	if byokKey != "" {
		userId := extractUserID(apiKey)
		encryptor, err = encryption.NewEncryptor(byokKey, userId)
		if err != nil {
			log.Fatalf("Failed to create encryptor: %v", err)
		}
		log.Println("BYOK encryption enabled")
	} else {
		encryptor, err = encryption.NewEncryptor("", "")
		if err != nil {
			log.Fatalf("Failed to create encryptor: %v", err)
		}
	}

	if !webdavMode && isFuseAvailable() {
		mountPoint = resolveMountPoint(mountPoint)
		log.Printf("Mounting SquidCloud at %s", mountPoint)

		filesystem := squidfs.New(client, cache, encryptor)

		mountPoint, err := mount.Mount(mountPoint, filesystem)
		if err != nil {
			log.Fatalf("Failed to mount: %v", err)
		}

		log.Printf("SquidCloud mounted at %s", mountPoint.Point())
		log.Println("Press Ctrl+C to unmount")
	} else {
		log.Printf("Starting WebDAV server on port %d", webdavPort)
		server := webdav.NewServer(client, cache, encryptor, webdavPort)
		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start WebDAV server: %v", err)
		}
	}
}

func printUsage() {
	fmt.Print(`squidfs - Mount SquidCloud storage as a local filesystem

Usage:
  squidfs -key <api-key> [options]

Options:
  -key        SquidCloud API key (required, format: cb_...)
  -url        SquidCloud project URL (default: https://aouqcwbdoyrccjcrhzzi.supabase.co)
  -mount      Mount point path
  -cache-dir  Cache directory (default: ~/.squidfs/cache)
  -cache-size Cache size in bytes (default: 104857600 = 100MB)
  -byok       BYOK encryption passphrase
  -webdav     Use WebDAV mode instead of FUSE
  -port       WebDAV server port (default: 8080)
  -version    Show version
  -help       Show help

Examples:
  # Mount with FUSE (Linux/macOS)
  squidfs -key cb_your_api_key_here -mount /mnt/squidfs

  # Mount with WebDAV (Windows/Termux)
  squidfs -key cb_your_api_key_here -webdav

  # Mount with BYOK encryption
  squidfs -key cb_your_api_key_here -byok "your-passphrase"

  # Mount with custom cache
  squidfs -key cb_your_api_key_here -cache-dir /tmp/squidfs -cache-size 536870912
`)
}

func isFuseAvailable() bool {
	if runtime.GOOS == "windows" {
		cmd := exec.Command("where", "winfsp-x64.dll")
		return cmd.Run() == nil
	}

	cmd := exec.Command("which", "fusermount")
	if cmd.Run() == nil {
		return true
	}

	cmd = exec.Command("which", "fusermount3")
	if cmd.Run() == nil {
		return true
	}

	if _, err := os.Stat("/dev/fuse"); err == nil {
		return true
	}

	return false
}

func resolveMountPoint(mountPoint string) string {
	if mountPoint != "" {
		return mountPoint
	}

	switch runtime.GOOS {
	case "linux":
		return "/mnt/squidfs"
	case "darwin":
		return "/Volumes/squidfs"
	case "windows":
		home, _ := os.UserHomeDir()
		return home + "\\squidfs"
	default:
		home, _ := os.UserHomeDir()
		return home + "/squidfs"
	}
}

func extractUserID(apiKey string) string {
	if len(apiKey) > 3 {
		return apiKey[3:]
	}
	return "default"
}

func getArchitecture() string {
	arch := runtime.GOARCH
	if strings.Contains(arch, "arm") || strings.Contains(arch, "aarch") {
		return "arm64"
	}
	return arch
}
