package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/NaveenSingh9999/squidfs/internal/api"
	"github.com/NaveenSingh9999/squidfs/internal/cache"
	"github.com/NaveenSingh9999/squidfs/internal/encryption"
	"github.com/NaveenSingh9999/squidfs/internal/webdav"
)

var version = "dev"

func main() {
	var (
		bridgeURL  string
		apiKey     string
		port       int
		webdavMode bool
		password   string
		userID     string
		mountPoint string
		bindHost   string
		authHash   string
		mdns       bool
		showVer    bool
	)

	flag.StringVar(&bridgeURL, "bridge", "https://aouqcwbdoyrccjcrhzzi.supabase.co/functions/v1/squidfs-bridge", "Bridge endpoint URL")
	flag.StringVar(&apiKey, "key", "", "CloudBliss API key (cb_ prefix)")
	flag.IntVar(&port, "port", 8099, "WebDAV server port")
	flag.BoolVar(&webdavMode, "webdav", true, "Run WebDAV server (default: true)")
	flag.StringVar(&password, "password", "", "Encryption password (optional)")
	flag.StringVar(&userID, "user", "", "User ID for encryption (optional)")
	flag.StringVar(&mountPoint, "mount", "/mnt/squidfs", "FUSE mount point")
	flag.StringVar(&bindHost, "bind", "", "Bind address (default 127.0.0.1 — loopback only)")
	flag.StringVar(&authHash, "auth-hash", "", "PBKDF2 hash file enabling WebDAV Basic auth (env SQUIDCLOUD_WEBDAV_AUTH_HASH)")
	flag.BoolVar(&mdns, "mdns", false, "Advertise via mDNS on the LAN (off by default)")
	flag.BoolVar(&showVer, "version", false, "Show version")

	flag.Parse()

	if showVer {
		fmt.Printf("squidfs %s\n", version)
		os.Exit(0)
	}

	if apiKey == "" {
		apiKey = os.Getenv("SQUIDCLOUD_API_KEY")
	}
	if apiKey == "" {
		log.Fatal("API key required: use -key flag or SQUIDCLOUD_API_KEY env var")
	}

	log.Printf("squidfs %s starting...", version)
	log.Printf("Bridge: %s", bridgeURL)

	client := api.NewClient(bridgeURL, apiKey)
	cacheDir := os.Getenv("HOME") + "/.squidfs/cache"
	diskCache, err := cache.NewCache(cacheDir, 500*1024*1024)
	if err != nil {
		log.Printf("Cache init failed: %v (continuing without cache)", err)
	}

	var encryptor *encryption.Encryptor
	if password != "" {
		encUserID := userID
		if encUserID == "" {
			encUserID = "default"
		}
		encryptor, err = encryption.NewEncryptor(password, encUserID)
		if err != nil {
			log.Fatalf("Encryption init failed: %v", err)
		}
		log.Println("Encryption enabled")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if webdavMode {
		// Fail closed: a non-loopback bind without password auth would expose
		// every decrypted file to the network.
		effectiveBind := bindHost
		if effectiveBind == "" {
			effectiveBind = "127.0.0.1"
		}
		hashPath := authHash
		if hashPath == "" {
			hashPath = os.Getenv("SQUIDCLOUD_WEBDAV_AUTH_HASH")
		}
		isLoopback := effectiveBind == "127.0.0.1" || effectiveBind == "localhost" || effectiveBind == "::1"
		if !isLoopback && hashPath == "" {
			log.Fatal("refusing to bind non-loopback without --auth-hash (WebDAV password auth is mandatory for remote access)")
		}

		server := webdav.NewServer(client, diskCache, encryptor, port)
		server.SetAuth(os.Getenv("SQUIDCLOUD_AUTH_TOKEN"), os.Getenv("SQUIDCLOUD_ANON_KEY"))
		server.SetBindHost(effectiveBind)
		server.SetMdnsEnabled(mdns && !isLoopback)
		if hashPath != "" {
			h, err := webdav.ParsePbkdf2HashFile(hashPath)
			if err != nil {
				log.Fatalf("WebDAV auth hash: %v", err)
			}
			server.SetWebdavAuthHash(h)
			log.Println("WebDAV password auth: enabled")
		} else {
			log.Println("WebDAV password auth: none (loopback-only bind)")
		}
		if durl := os.Getenv("SQUIDCLOUD_DECRYPTOR"); durl != "" {
			server.SetDecryptor(durl, os.Getenv("SQUIDCLOUD_DECRYPTOR_AUTH"))
			log.Printf("Decryptor: %s", durl)
		}
		go func() {
			<-sigChan
			server.Stop()
			os.Exit(0)
		}()
		if err := server.Start(); err != nil {
			log.Fatalf("WebDAV server error: %v", err)
		}
	} else {
		log.Printf("Mount point: %s", mountPoint)
		log.Fatal("FUSE mode not yet implemented in rewrite")
	}
}
