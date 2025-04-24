package tests

import (
	"context"
	"errors"
	"github.com/tarcisiozf/dkv/engine"
	"github.com/tarcisiozf/dkv/engine/test"
	"testing"
)

func TestDbEngine_Standalone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	testEnv := test.NewTestEnv()
	defer testEnv.Destroy(ctx)
	defer cancel()

	instance, err := testEnv.CreateInstance(ctx)
	if err != nil {
		t.Fatalf("Error creating instance: %v", err)
	}

	if err := instance.Start(ctx); err != nil {
		t.Fatalf("Error starting instance: %v", err)
	}

	key := []byte("test_key")
	value := []byte("test_value")

	if err := instance.Set(key, value); err != nil {
		t.Fatalf("Error setting value: %v", err)
	}

	result, err := instance.Get(key)
	if err != nil {
		t.Fatalf("Error getting value: %v", err)
	}

	if string(result) != string(value) {
		t.Fatalf("Expected value %s, got %s", value, result)
	}

	err = instance.Delete(key)
	if err != nil {
		t.Fatalf("Error deleting value: %v", err)
	}

	result, err = instance.Get(key)
	if err == nil {
		t.Fatalf("Expected error getting deleted key, got value: %s", result)
	}
	if !errors.Is(err, engine.ErrKeyNotFound) {
		t.Fatalf("Expected key not found error, got: %v", err)
	}
	if result != nil {
		t.Fatalf("Expected nil result for deleted key, got: %s", result)
	}
}
