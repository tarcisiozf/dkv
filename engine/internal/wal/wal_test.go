package wal_test

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/internal/wal"
	"io"
	"os"
	"testing"
)

func TestNewWriteAheadLog(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	writeAheadLog, err := wal.NewWriteAheadLog(dir)
	if err != nil {
		t.Fatalf("Failed to create WriteAheadLog: %v", err)
	}

	n := 10

	for i := 0; i < n; i++ {
		data := fmt.Sprintf("some data %d", i)
		off, err := writeAheadLog.Write([]byte(data))
		if err != nil {
			t.Errorf("Failed to write to WriteAheadLog: %v", err)
		}
		if i == 0 && off != 0 {
			t.Errorf("Expected offset 0 for first entry, got %d", off)
		} else if i > 0 && off == 0 {
			t.Errorf("Expected non-zero offset for entry %d, got %d", i, off)
		}
	}

	reader, err := writeAheadLog.Reader(0)
	if err != nil {
		t.Fatalf("Failed to create reader for WriteAheadLog: %v", err)
	}

	i := 0
	prevOffset := uint64(0)
	for entry, err := range reader {
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Errorf("Failed to read from WriteAheadLog: %v", err)
			break
		}
		if entry.Offset <= prevOffset {
			t.Errorf("Expected offset greater than previous entry, got %d (prev: %d)", entry.Offset, prevOffset)
		}
		if entry.Data == nil {
			t.Errorf("Expected entry, got nil")
			break
		}
		if len(entry.Data) == 0 {
			t.Errorf("Expected non-empty entry, got empty")
			break
		}
		if string(entry.Data) != fmt.Sprintf("some data %d", i) {
			t.Errorf("Expected entry %d, got %s", i, entry.Data)
		}
		prevOffset = entry.Offset
		i++
	}
	if i != n {
		t.Errorf("Expected %d entries, got %d", n, i)
	}

	err = writeAheadLog.Close()
	if err != nil {
		t.Errorf("Failed to close WriteAheadLog: %v", err)
	}

	err = writeAheadLog.Delete()
	if err != nil {
		t.Errorf("Failed to delete WriteAheadLog: %v", err)
	}
}
