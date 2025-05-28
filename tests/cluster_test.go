package tests

import (
	"context"
	"github.com/tarcisiozf/dkv/engine"
	"github.com/tarcisiozf/dkv/engine/db"
	"github.com/tarcisiozf/dkv/engine/test"
	"log"
	"testing"
)

func TestDbEngine_Cluster_NewWriterWithReplicas(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	testEnv := test.NewTestEnv(
		test.WithAutoStart(),
	)
	defer testEnv.Destroy(ctx)
	defer cancel()

	const nWriters = 3
	const replicationFactor = 3
	const writeQuorum = replicationFactor
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

		err = testEnv.WaitForAllInstancesToBeReady()
		if err != nil {
			t.Fatalf("[FATAL] Error waiting for instances to be ready: %v", err)
		}

		err = testEnv.EnsureClusterSetup(nWriters, replicationFactor, writeQuorum)
		if err != nil {
			t.Fatalf("[FATAL] Error ensuring cluster setup: %v", err)
		}
	})

	const numEntries = 1000
	var entries []db.Entry

	t.Run("populate cluster", func(t *testing.T) {
		var err error
		entries, err = testEnv.PopulateWriters(numEntries, 10)
		if err != nil {
			t.Fatalf("[FATAL] Error populating writers: %v", err)
		}
	})

	t.Run("expand cluster and ensure resharding", func(t *testing.T) {
		newInstances, err := testEnv.CreateInstancesInParallel(ctx, replicationFactor, options...)
		if err != nil {
			t.Fatalf("[FATAL] Error creating instances: %v", err)
		}

		err = testEnv.WaitForAllInstancesToBeReady()
		if err != nil {
			t.Fatalf("[FATAL] Error waiting for instances to be ready: %v", err)
		}

		var newWriter *engine.DbEngine
		for _, instance := range newInstances {
			if instance.Node().Role.IsWriter() {
				newWriter = instance
				break
			}
		}
		if newWriter == nil {
			t.Fatalf("failed to find writed")
		}
		readReplicas := testEnv.FindReadReplicasOf(newWriter)
		log.Printf("new writer %s on range %s", newWriter.NodeID(), newWriter.Node().SlotRange)
		for _, rr := range readReplicas {
			log.Printf("new read replica %s on range %s", rr.NodeID(), newWriter.Node().SlotRange)
		}

		entriesInRange := make([]db.Entry, 0)
		slotRange := newWriter.Node().SlotRange
		for _, entry := range entries {
			if slotRange.Contains(entry.SlotId) {
				entriesInRange = append(entriesInRange, entry)
			}
		}
		if len(entriesInRange) == 0 {
			t.Fatalf("no entries found in range")
		}

		for _, instance := range newInstances {
			for _, entry := range entriesInRange {
				result, err := instance.Get(entry.Key)
				if err != nil {
					t.Errorf("failed to get value for key %s on instance: %s, writer: %s: %v", entry.Key, instance.NodeID(), newWriter.NodeID(), err)
					break
				}
				if string(result) != string(entry.Value) {
					t.Errorf("expected value to be %s got %s", entry.Value, result)
				}
			}
		}
	})
}
