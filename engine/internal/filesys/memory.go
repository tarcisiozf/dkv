package filesys

import (
	"errors"
	"io"
	"os"
	"sync"
)

type InMemoryFile struct {
	Name   string
	Flag   int
	Perm   os.FileMode
	Closed bool
	Data   []byte
	Offset int64
}

func (f *InMemoryFile) Close() error {
	if f.Closed {
		return errors.New("file already closed")
	}
	f.Closed = true
	return nil
}

func (f *InMemoryFile) Read(p []byte) (n int, err error) {
	if f.Closed {
		return 0, errors.New("file is closed")
	}
	if f.Offset >= int64(len(f.Data)) {
		return 0, io.EOF
	}
	n = copy(p, f.Data[f.Offset:])
	f.Offset += int64(n)
	return n, nil
}

func (f *InMemoryFile) Write(p []byte) (n int, err error) {
	if f.Closed {
		return 0, errors.New("file is closed")
	}
	if f.Flag&os.O_WRONLY == 0 && f.Flag&os.O_RDWR == 0 {
		return 0, errors.New("file not opened for writing")
	}
	f.Data = append(f.Data[:f.Offset], p...)
	n = len(p)
	f.Offset += int64(n)
	return n, nil
}

func (f *InMemoryFile) Seek(offset int64, whence int) (int64, error) {
	if f.Closed {
		return 0, errors.New("file is closed")
	}
	if offset < 0 {
		return 0, errors.New("negative offset")
	}
	if whence != io.SeekStart {
		return 0, errors.New("invalid whence or not implemented")
	}
	f.Offset = offset
	return f.Offset, nil
}

func (f *InMemoryFile) Sync() error {
	if f.Closed {
		return errors.New("file is closed")
	}
	return nil
}

type InMemoryFileSystem struct {
	files map[string]*InMemoryFile
	mutex *sync.RWMutex
}

func NewInMemoryFileSystem() *InMemoryFileSystem {
	return &InMemoryFileSystem{
		files: make(map[string]*InMemoryFile),
		mutex: &sync.RWMutex{},
	}
}

func (fs *InMemoryFileSystem) Exists(path string) (bool, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	_, exists := fs.files[path]
	return exists, nil
}

func (fs *InMemoryFileSystem) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	file := &InMemoryFile{
		Name: name,
		Flag: flag,
		Perm: perm,
	}
	fs.files[name] = file
	return file, nil
}

func (fs *InMemoryFileSystem) Close() {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	for _, file := range fs.files {
		_ = file.Close()
	}
}

func (fs *InMemoryFileSystem) Find(path string) *InMemoryFile {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	return fs.files[path]
}

func (fs *InMemoryFileSystem) Files() []*InMemoryFile {
	files := make([]*InMemoryFile, 0, len(fs.files))
	for _, file := range fs.files {
		files = append(files, file)
	}
	return files
}
