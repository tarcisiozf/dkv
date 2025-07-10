package tests

import (
	"context"
	"fmt"
	"github.com/tarcisiozf/dkv/engine"
	"github.com/tarcisiozf/dkv/engine/test"
	"testing"
)

func TestDbEngine_Rollback_Prepare(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	fs := test.NewInMemoryFileSystem()
	testEnv := test.NewTestEnv(
		test.WithAutoStart(),
	)
	defer testEnv.Destroy(ctx)
	defer cancel()

	const numWriters = 1
	const numReaders = 2
	const numInstances = numWriters + numReaders
	options := []engine.ConfigOption{
		engine.WithWriteQuorum(numInstances),
		engine.WithReplicationFactor(numInstances),
		engine.WithMode("cluster"),
		engine.WithFileSystem(fs),
	}

	_, err := testEnv.CreateInstancesInParallel(ctx, numInstances, options...)
	if err != nil {
		t.Fatalf("Failed to create instances: %v", err)
	}

	err = testEnv.EnsureClusterSetup(numWriters, numInstances, numInstances)
	if err != nil {
		t.Fatalf("Failed to ensure cluster setup: %v", err)
	}

	writer := testEnv.FindAnyWriter()
	dirPath := writer.DirPath()
	walPath := dirPath + "/WAL"

	if _, err := fs.Exists(walPath); err != nil {
		t.Fatalf("[FATAL] Error checking WAL file: %v", err)
	}

	// write works
	key := testEnv.FakeKey()
	value := testEnv.FakeValue()
	if err := writer.Set(key, value); err != nil {
		t.Fatalf("[FATAL] Error setting value: %v", err)
	}

	// kill disk
	wal := fs.Find(walPath)
	offset := wal.Offset
	_ = wal.Close()

	// write again and it should fail
	err = writer.Set(key, value)
	if err == nil {
		t.Fatalf("[FATAL] Expected error when setting value after WAL removal, got nil")
	}
	fmt.Println("[INFO] Successfully detected WAL removal, error:", err)

	for _, file := range fs.Files() {
		if file.Offset != offset {
			t.Errorf("[FATAL] Expected file offset to be %d, got %d", offset, file.Offset)
		}
	}
}

func TestDbEngine_Rollback_Commit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	fs := test.NewInMemoryFileSystem()
	testEnv := test.NewTestEnv(
		test.WithAutoStart(),
	)
	defer testEnv.Destroy(ctx)
	defer cancel()

	const numWriters = 1
	const numReaders = 2
	const numInstances = numWriters + numReaders
	options := []engine.ConfigOption{
		engine.WithWriteQuorum(numInstances),
		engine.WithReplicationFactor(numInstances),
		engine.WithMode("cluster"),
		engine.WithFileSystem(fs),
	}

	_, err := testEnv.CreateInstancesInParallel(ctx, numInstances, options...)
	if err != nil {
		t.Fatalf("Failed to create instances: %v", err)
	}

	err = testEnv.EnsureClusterSetup(numWriters, numInstances, numInstances)
	if err != nil {
		t.Fatalf("Failed to ensure cluster setup: %v", err)
	}

	writer := testEnv.FindAnyWriter()
	dirPath := writer.DirPath()
	walPath := dirPath + "/WAL"

	if _, err := fs.Exists(walPath); err != nil {
		t.Fatalf("[FATAL] Error checking WAL file: %v", err)
	}

	// write works
	key := testEnv.FakeKey()
	value := testEnv.FakeValue()
	if err := writer.Set(key, value); err != nil {
		t.Fatalf("[FATAL] Error setting value: %v", err)
	}

	// kill disk
	wal := fs.Find(walPath)
	offset := wal.Offset

	files := fs.Files()
	if len(files) != numInstances {
		t.Fatalf("[FATAL] Expected %d files, got %d", numInstances, len(files))
	}
	for i := 0; i < numInstances; i++ {
		file := files[i]
		if file == wal { // skip writer WAL
			continue
		}
		// detect changes to writer WAL and stop read replicas
		go func() {
			for wal.Offset == offset {
			}
			file.Close()
		}()
	}

	// write again and it should fail
	err = writer.Set(key, value)
	if err == nil {
		t.Fatalf("[FATAL] Expected error when setting value after WAL removal, got nil")
	}
	fmt.Println("[INFO] Successfully detected WAL removal, error:", err)

	for _, file := range fs.Files() {
		if file.Offset != offset {
			t.Errorf("[FATAL] Expected file offset to be %d, got %d", offset, file.Offset)
		}
	}
}
