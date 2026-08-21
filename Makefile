.PHONY: all clean build linux macos windows termux install

VERSION ?= 1.0.0
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.buildDate=$(BUILD_DATE)"

all: linux macos windows termux

clean:
	rm -rf dist/
	mkdir -p dist/

linux: clean
	@echo "Building for Linux amd64..."
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o dist/squidfs-linux-amd64 ./cmd/squidfs/
	@echo "Building for Linux arm64..."
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o dist/squidfs-linux-arm64 ./cmd/squidfs/
	@chmod +x dist/squidfs-linux-*

macos: clean
	@echo "Building for macOS amd64..."
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o dist/squidfs-darwin-amd64 ./cmd/squidfs/
	@echo "Building for macOS arm64..."
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o dist/squidfs-darwin-arm64 ./cmd/squidfs/
	@chmod +x dist/squidfs-darwin-*

windows: clean
	@echo "Building for Windows amd64..."
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o dist/squidfs-windows-amd64.exe ./cmd/squidfs/
	@echo "Building for Windows arm64..."
	GOOS=windows GOARCH=arm64 go build $(LDFLAGS) -o dist/squidfs-windows-arm64.exe ./cmd/squidfs/

termux: clean
	@echo "Building for Termux/Android arm64..."
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build $(LDFLAGS) -o dist/squidfs-termux-arm64 ./cmd/squidfs/
	@chmod +x dist/squidfs-termux-arm64

install: linux
	@echo "Installing squidfs..."
	cp dist/squidfs-linux-$(shell uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/') /usr/local/bin/squidfs
	@chmod +x /usr/local/bin/squidfs
	@echo "Installed to /usr/local/bin/squidfs"

test:
	go test -v ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

# Development
dev:
	go run ./cmd/squidfs/ -help

# Docker
docker:
	docker build -t squidfs:$(VERSION) .

# Release
release: all
	@echo "Creating release..."
	tar -czf dist/squidfs-$(VERSION)-linux-amd64.tar.gz -C dist squidfs-linux-amd64
	tar -czf dist/squidfs-$(VERSION)-linux-arm64.tar.gz -C dist squidfs-linux-arm64
	tar -czf dist/squidfs-$(VERSION)-darwin-amd64.tar.gz -C dist squidfs-darwin-amd64
	tar -czf dist/squidfs-$(VERSION)-darwin-arm64.tar.gz -C dist squidfs-darwin-arm64
	zip dist/squidfs-$(VERSION)-windows-amd64.zip dist/squidfs-windows-amd64.exe
	zip dist/squidfs-$(VERSION)-windows-arm64.zip dist/squidfs-windows-arm64.exe
	tar -czf dist/squidfs-$(VERSION)-termux-arm64.tar.gz -C dist squidfs-termux-arm64
	@echo "Release created in dist/"
