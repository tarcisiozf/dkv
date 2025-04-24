package filesys

import (
	"fmt"
	"os"
)

type DiskFileSystem struct {
}

func NewDiskFileSystem() *DiskFileSystem {
	return &DiskFileSystem{}
}

func (fs *DiskFileSystem) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm)
}

func (fs *DiskFileSystem) Exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("checking file existence: %w", err)
	}
	return true, nil
}
