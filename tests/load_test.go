package tests

import (
	"context"
	"github.com/tarcisiozf/dkv/engine"
	"github.com/tarcisiozf/dkv/engine/test"
	"testing"
	"time"
)

func TestDbEngine_Load(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	testEnv := test.NewTestEnv(
		test.WithAutoStart(),
	)
	defer testEnv.Destroy(ctx)
	defer cancel()

	const nWriters = 3
	const replicationFactor = 3
	const writeQuorum = replicationFactor - 1
	const numInstances = nWriters * replicationFactor
	options := []engine.ConfigOption{
		engine.WithReplicationFactor(replicationFactor),
		engine.WithWriteQuorum(writeQuorum),
		engine.WithMode("cluster"),
	}

	t.Run("setup env", func(t *testing.T) {
		_, err := testEnv.CreateInstancesInParallel(ctx, numInstances, options...)
		if err != nil {
			t.Fatalf("[FATAL] Error creating instances: %v", err)
		}

		if err := testEnv.WaitForAllInstancesToBeReady(); err != nil {
			t.Fatalf("[FATAL] Error waiting for instances to be ready: %v", err)
		}

		if err := testEnv.EnsureClusterSetup(nWriters, replicationFactor, writeQuorum); err != nil {
			t.Fatalf("[FATAL] Error ensuring cluster setup: %v", err)
		}
	})

	const numEntries = 1000
	const concurrency = 10

	startTime := time.Now()
	entries, err := testEnv.PopulateWriters(numEntries, concurrency)
	elapsedTime := time.Since(startTime)

	if err != nil {
		t.Fatalf("[FATAL] Error populating writers: %v", err)
	}
	if len(entries) != numEntries {
		t.Errorf("[FATAL] Expected %d entries, got %d", numEntries, len(entries))
	}

	t.Logf("Populated %d entries in %s", len(entries), elapsedTime)
	t.Logf("Average time per entry: %s", elapsedTime/time.Duration(numEntries))
}
