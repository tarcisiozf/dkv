package tests

import (
	"context"
	"fmt"
	"github.com/tarcisiozf/dkv/engine"
	"github.com/tarcisiozf/dkv/engine/test"
	"github.com/tarcisiozf/dkv/internal/retry"
	"github.com/tarcisiozf/dkv/slots"
	"testing"
	"time"
)

func TestDbEngine_Replication(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	testEnv := test.NewTestEnv(
		test.WithAutoStart(),
	)
	defer testEnv.Destroy(ctx)
	defer cancel()

	const numInstances = 5
	options := []engine.ConfigOption{
		engine.WithWriteQuorum(numInstances),
		engine.WithReplicationFactor(numInstances),
		engine.WithMode("cluster"),
	}

	instances, err := testEnv.CreateInstancesInParallel(ctx, numInstances, options...)
	if err != nil {
		t.Fatalf("Failed to create instances: %v", err)
	}

	t.Run("coordinate instances", func(t *testing.T) {
		err = retry.NewRetry(
			retry.WithTimeout(time.Minute),
			retry.WithInterval(time.Second),
		).
			Do(func() error {
				for _, instance := range instances {
					if !instance.Node().Status.IsReady() {
						return fmt.Errorf("instance %s is not ready", instance.NodeID())
					}
				}
				return nil
			})
		if err != nil {
			t.Fatalf("[FATAL] Error waiting for instances to be ready: %v", err)
		}
	})

	t.Run("write and read from replicas", func(t *testing.T) {
		time.Sleep(5 * time.Second)

		entries, err := testEnv.PopulateWriters(10, 1)
		if err != nil {
			t.Fatalf("Error populating writers: %v", err)
		}

		for _, entry := range entries {
			slot := slots.GetSlotId(entry.Key)
			instances := testEnv.FindInstancesForSlot(slot)
			if len(instances) == 0 {
				t.Fatalf("no instances found for slot %d", slot)
			}
			for _, instance := range instances {
				result, err := instance.Get(entry.Key)
				if err != nil {
					t.Errorf("Error getting value: %v", err)
					continue
				}
				if string(result) != string(entry.Value) {
					var fromKey []byte
					for _, e := range entries {
						if string(result) == string(e.Value) {
							fromKey = e.Key
							break
						}
					}
					t.Errorf("Key %s expected value %s, got %s (from key: %s)", entry.Key, entry.Value, result, fromKey)
				}
			}
		}
	})
}

func TestDbEngine_NewReadReplica(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	testEnv := test.NewTestEnv(
		test.WithAutoStart(),
	)
	defer testEnv.Destroy(ctx)
	defer cancel()

	const numInstances = 2
	const replicationFactor = numInstances * 2
	options := []engine.ConfigOption{
		engine.WithReplicationFactor(replicationFactor),
		engine.WithMode("cluster"),
	}

	instances, err := testEnv.CreateInstancesInParallel(ctx, numInstances, options...)
	if err != nil {
		t.Fatalf("Failed to create instances: %v", err)
	}

	t.Run("coordinate instances", func(t *testing.T) {
		err = retry.NewRetry(
			retry.WithTimeout(time.Minute),
			retry.WithInterval(time.Second),
		).
			Do(func() error {
				for _, instance := range instances {
					if !instance.Node().Status.IsReady() {
						return fmt.Errorf("instance %s is not ready", instance.NodeID())
					}
				}
				return nil
			})
		if err != nil {
			t.Fatalf("[FATAL] Error waiting for instances to be ready: %v", err)
		}
	})

	var writer *engine.DbEngine
	entries := make(map[string]string)

	t.Run("write and read from replicas", func(t *testing.T) {
		time.Sleep(5 * time.Second)

		var readReplica *engine.DbEngine
		for _, instance := range instances {
			if instance.Node().Role.IsWriter() {
				writer = instance
			} else if instance.Node().Role.IsReadReplica() {
				readReplica = instance
			}
		}

		for i := 0; i < 1; i++ {
			key := testEnv.FakeKey()
			value := testEnv.FakeValue()
			err := writer.Set(key, value)
			if err != nil {
				t.Fatalf("Error setting value: %v", err)
			}
			entries[string(key)] = string(value)
		}

		for key, value := range entries {
			result, err := readReplica.Get([]byte(key))
			if err != nil {
				t.Fatalf("Error getting value: %v", err)
			}
			if string(result) != value {
				t.Fatalf("Expected value %s, got %s", value, result)
			}
		}
	})

	t.Run("add new read replica and sync", func(t *testing.T) {
		time.Sleep(5 * time.Second)

		newInstance, err := testEnv.CreateInstance(ctx, options...)
		if err != nil {
			t.Fatalf("Failed to create new instance: %v", err)
		}

		err = retry.NewRetry(
			retry.WithTimeout(time.Minute),
			retry.WithInterval(time.Second),
		).
			Do(func() error {
				if !newInstance.Node().Status.IsReady() {
					return fmt.Errorf("instance %s is not ready", newInstance.NodeID())
				}
				return nil
			})
		if err != nil {
			t.Fatalf("[FATAL] Error waiting for instances to be ready: %v", err)
		}

		for key, value := range entries {
			result, err := newInstance.Get([]byte(key))
			if err != nil {
				t.Errorf("Error getting value: %v", err)
			}
			if string(result) != value {
				t.Errorf("Expected value %s, got %s", value, result)
			}
		}
	})
}

func TestDbEngine_FaultTolerance(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	testEnv := test.NewTestEnv(
		test.WithAutoStart(),
	)
	defer testEnv.Destroy(ctx)
	defer cancel()

	const numInstances = 3
	const writeQuorum = numInstances - 1
	options := []engine.ConfigOption{
		engine.WithReplicationFactor(numInstances),
		engine.WithWriteQuorum(writeQuorum),
		engine.WithMode("cluster"),
	}

	_, err := testEnv.CreateInstancesInParallel(ctx, numInstances, options...)
	if err != nil {
		t.Fatalf("Failed to create instances: %v", err)
	}

	err = testEnv.WaitForAllInstancesToBeReady()
	if err != nil {
		t.Fatalf("[FATAL] Error waiting for instances to be ready: %v", err)
	}

	var writer *engine.DbEngine
	entries := make(map[string]string)

	t.Run("write and read from replicas", func(t *testing.T) {
		time.Sleep(5 * time.Second)

		writer = testEnv.FindAnyWriter()
		if writer == nil {
			t.Fatalf("Expected writer not to be nil")
		}

		for i := 0; i < 100; i++ {
			key := testEnv.FakeKey()
			value := testEnv.FakeValue()
			err := writer.Set(key, value)
			if err != nil {
				t.Fatalf("Error setting value: %v", err)
			}
			entries[string(key)] = string(value)
		}

		readReplicas := testEnv.FindReadReplicasOf(writer)
		if len(readReplicas) == 0 {
			t.Fatalf("no read-replicas found")
		}
		for _, rr := range readReplicas {
			for key, value := range entries {
				result, err := rr.Get([]byte(key))
				if err != nil {
					t.Fatalf("Error getting value: %v", err)
				}
				if string(result) != value {
					t.Fatalf("Expected value %s, got %s", value, result)
				}
			}
		}
	})

	t.Run("simulate instance failure", func(t *testing.T) {
		if err := writer.Close(); err != nil {
			t.Fatalf("Error closing writer instance: %v", err)
		}
	})

	t.Run("find new writer and check if instances are following", func(t *testing.T) {
		time.Sleep(5 * time.Second)

		if err := testEnv.WaitForQuorumOfInstancesToBeReady(writeQuorum); err != nil {
			t.Fatalf("[FATAL] Error waiting for instances to be ready: %v", err)
		}

		newWriter := testEnv.FindAnyWriter()
		if newWriter == nil {
			t.Fatalf("Expected new writer not to be nil")
		}
		if writer.NodeID() == newWriter.NodeID() {
			t.Fatalf("Expected new writer to be different from old writer")
		}

		readReplicas := testEnv.FindReadReplicasOf(newWriter)
		if len(readReplicas) == 0 {
			t.Fatalf("no read-replicas found")
		}

		key := testEnv.FakeKey()
		value := testEnv.FakeValue()

		if err := newWriter.Set(key, value); err != nil {
			t.Fatalf("failed to write to new writer: %v", err)
		}

		for _, rr := range readReplicas {
			result, err := rr.Get(key)
			if err != nil {
				t.Fatalf("Error getting value: %v", err)
			}
			if string(result) != string(value) {
				t.Fatalf("Expected value %s, got %s", value, result)
			}
		}
	})
}
