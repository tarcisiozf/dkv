package filesys

import (
	"io"
	"os"
)

type File interface {
	io.Closer
	io.Reader
	io.Writer
	io.Seeker
	Sync() error
}

type FileSystem interface {
	Exists(path string) (bool, error)
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
}
