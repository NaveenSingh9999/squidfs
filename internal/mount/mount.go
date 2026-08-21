package mount

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	squidfs "github.com/NaveenSingh9999/squidfs/internal/fs"
)

type MountPoint struct {
	fs    *squidfs.SquidFS
	point string
}

func Mount(mountPoint string, filesystem *squidfs.SquidFS) (*MountPoint, error) {
	if _, err := os.Stat(mountPoint); os.IsNotExist(err) {
		if err := os.MkdirAll(mountPoint, 0755); err != nil {
			return nil, fmt.Errorf("create mount point: %w", err)
		}
	}

	mount := &MountPoint{
		fs:    filesystem,
		point: mountPoint,
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received signal, unmounting...")
		mount.Unmount()
		os.Exit(0)
	}()

	if err := filesystem.Mount(mountPoint); err != nil {
		return nil, fmt.Errorf("mount filesystem: %w", err)
	}

	return mount, nil
}

func (m *MountPoint) Unmount() error {
	if m.fs.Server != nil {
		return m.fs.Server.Unmount()
	}
	return nil
}

func (m *MountPoint) Point() string {
	return m.point
}
