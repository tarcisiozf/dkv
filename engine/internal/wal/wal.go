package wal

import (
	"encoding/binary"
	"fmt"
	"github.com/tarcisiozf/dkv/engine/internal/filesys"
	"io"
	"iter"
	"math"
	"os"
	"sync"
)

const walFile = "WAL"

type WriteAheadLog struct {
	path     string
	offset   uint64
	file     filesys.File
	filePath string
	open     bool
	mutex    *sync.RWMutex
}

func NewWriteAheadLog(fs filesys.FileSystem, path string) (*WriteAheadLog, error) {
	filePath := path + "/" + walFile

	fileExists, err := fs.Exists(filePath)
	if err != nil {
		return nil, fmt.Errorf("checking WAL file existence: %w", err)
	}

	flag := os.O_APPEND
	if fileExists {
		flag |= os.O_RDWR
	} else {
		flag |= os.O_CREATE | os.O_WRONLY
	}

	file, err := fs.OpenFile(filePath, flag, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening WAL file: %w", err)
	}

	wal := &WriteAheadLog{
		filePath: filePath,
		file:     file,
		mutex:    &sync.RWMutex{},
	}

	if fileExists {
		err := wal.loadOffset()
		if err != nil {
			return nil, fmt.Errorf("loading WAL offset: %w", err)
		}
	}

	wal.open = true

	return wal, nil
}

func (wal *WriteAheadLog) Write(data []byte) (uint64, error) {
	if !wal.open {
		return 0, fmt.Errorf("WAL is not open")
	}
	lenData := len(data)
	if lenData == 0 {
		return 0, fmt.Errorf("invalid empty data")
	}
	if lenData > math.MaxUint32 {
		return 0, fmt.Errorf("data too long")
	}

	sizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBytes, uint32(lenData))

	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	offset := wal.offset
	if err := wal.write(sizeBytes, data); err != nil {
		return 0, fmt.Errorf("writing to WAL file: %w", err)
	}

	return offset, nil
}

func (wal *WriteAheadLog) Reader(offset uint64) (iter.Seq2[Entry, error], error) {
	file, err := os.Open(wal.filePath)

	if err != nil {
		return nil, fmt.Errorf("opening WAL file for reading: %w", err)
	}

	return func(yield func(Entry, error) bool) {
		defer file.Close()

		for {
			var entry Entry
			if offset >= wal.offset {
				yield(entry, io.EOF)
				return
			}

			sizeBytes, err := wal.read(file, int64(offset), 4)
			if err != nil {
				yield(entry, err)
				return
			}
			offset += uint64(len(sizeBytes))

			size := binary.LittleEndian.Uint16(sizeBytes)
			if size == 0 {
				yield(entry, fmt.Errorf("invalid size in WAL file"))
				return
			}

			entry.Data, err = wal.read(file, int64(offset), int(size))
			if err != nil {
				if err == io.EOF {
					err = fmt.Errorf("corrupted WAL file: %w", err)
				}
				yield(entry, err)
				return
			}
			offset += uint64(size)
			entry.Offset = offset

			if !yield(entry, nil) {
				return
			}
		}
	}, nil
}

func (wal *WriteAheadLog) Offset() uint64 {
	wal.mutex.RLock()
	defer wal.mutex.RUnlock()

	return wal.offset
}

func (wal *WriteAheadLog) SetOffset(offset uint64) error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if offset > wal.offset {
		return fmt.Errorf("cannot set offset greater than current offset")
	}

	ret, err := wal.file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return fmt.Errorf("seeking in WAL file: %w", err)
	}
	if ret != int64(offset) {
		return fmt.Errorf("seek to offset %d failed, got %d", offset, ret)
	}
	wal.offset = offset

	return nil
}

func (wal *WriteAheadLog) Delete() error {
	if wal.open {
		return fmt.Errorf("cannot delete open WAL")
	}

	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	err := os.Remove(wal.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // File already deleted
		}
		return fmt.Errorf("deleting WAL file: %w", err)
	}

	return nil
}

func (wal *WriteAheadLog) Close() error {
	if !wal.open {
		return fmt.Errorf("WAL is not open")
	}

	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	wal.open = false
	if err := wal.file.Close(); err != nil {
		return fmt.Errorf("closing WAL file: %w", err)
	}
	return nil
}

func (wal *WriteAheadLog) loadOffset() (err error) {
	offset := int64(0)
	sizeBytes := make([]byte, 2)
	for {
		offset, err = wal.file.Seek(offset, io.SeekStart)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("seeking in WAL file: %w", err)
		}

		n, err := wal.file.Read(sizeBytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if n != len(sizeBytes) {
			return fmt.Errorf("incomplete read from WAL file: %d/%d", n, len(sizeBytes))
		}

		size := binary.LittleEndian.Uint16(sizeBytes)
		if size == 0 {
			return fmt.Errorf("invalid size in WAL file")
		}

		offset += int64(size) + 2
	}
	wal.offset = uint64(offset)
	return nil
}

func (wal *WriteAheadLog) write(chunks ...[]byte) error {
	for _, chunk := range chunks {
		n, err := wal.file.Write(chunk)
		if err != nil {
			return fmt.Errorf("writing to WAL file: %w", err)
		}
		if n != len(chunk) {
			return fmt.Errorf("incomplete write to WAL file: %d/%d", n, len(chunk))
		}
		wal.offset += uint64(n)
	}
	if err := wal.file.Sync(); err != nil {
		return fmt.Errorf("syncing WAL file: %w", err)
	}
	return nil
}

func (wal *WriteAheadLog) read(file *os.File, offset int64, size int) ([]byte, error) {
	data := make([]byte, size)
	n, err := file.ReadAt(data, offset)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("reading size from WAL file: %w", err)
	}
	if n != size {
		return nil, fmt.Errorf("incomplete read from WAL file: %d/%d", n, size)
	}
	return data, nil
}
