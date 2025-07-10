package tests

import (
	"context"
	"fmt"
	"github.com/tarcisiozf/dkv/engine"
	"github.com/tarcisiozf/dkv/engine/test"
	"github.com/tarcisiozf/dkv/internal/retry"
	"github.com/tarcisiozf/dkv/slots"
	"golang.org/x/sync/errgroup"
	"sort"
	"testing"
	"time"
)

func TestDbEngine_Coordinator_MultiWriters(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	testEnv := test.NewTestEnv(
		test.WithAutoStart(),
	)
	defer testEnv.Destroy(ctx)
	defer cancel()

	const numInstances = 5
	options := []engine.ConfigOption{
		engine.WithReplicationFactor(1),
		engine.WithMode("cluster"),
	}
	instances, err := testEnv.CreateInstancesInParallel(ctx, numInstances, options...)
	if err != nil {
		t.Fatalf("[FATAL] Error creating instances: %v", err)
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
					if !instance.Node().Role.IsWriter() {
						return fmt.Errorf("instance %s is not a writer", instance.NodeID())
					}
				}
				return nil
			})
		if err != nil {
			t.Fatalf("[FATAL] Error waiting for instances to be ready: %v", err)
		}
	})

	time.Sleep(5 * time.Second)

	t.Run("check slot ranges for cluster", func(t *testing.T) {
		ranges := make([]slots.SlotRange, numInstances)
		for i := 0; i < numInstances; i++ {
			ranges[i] = instances[i].Node().SlotRange
		}
		sort.Slice(ranges, func(i, j int) bool {
			return ranges[i][0] < ranges[j][0]
		})

		start := uint16(0)
		for i := 0; i < numInstances-1; i++ {
			if ranges[i].Start() != start {
				t.Fatalf("[FATAL] Slot range mismatch: expected %d, got %d", start, ranges[i].Start())
			}
			if ranges[i+1].Start()-ranges[i].End() != 1 {
				t.Fatalf("[FATAL] Slot range mismatch: expected %d, got %d", ranges[i].End()+1, ranges[i+1].Start())
			}
			start = ranges[i].End() + 1
		}
		if ranges[numInstances-1].End() != slots.NumSlots {
			t.Fatalf("[FATAL] Slot range mismatch: expected %d, got %d", slots.NumSlots, ranges[numInstances-1].End())
		}
	})

	t.Run("set keys in parallel", func(t *testing.T) {
		value := testEnv.FakeValue()
		keys := make(map[int][]byte)

		for len(keys) < numInstances {
			key := testEnv.FakeKey()
			slotId := slots.GetSlotId(key)
			idx := -1
			for i, instance := range instances {
				if _, ok := keys[i]; ok {
					continue
				}
				if instance.Node().SlotRange.Contains(slotId) {
					idx = i
					break
				}
			}
			if idx == -1 {
				continue
			}
			keys[idx] = key
		}

		eg := errgroup.Group{}
		for idx, key := range keys {
			instance := instances[idx]
			eg.Go(func() error {
				return instance.Set(key, value)
			})
		}
		if err := eg.Wait(); err != nil {
			t.Fatalf("[FATAL] Error setting key: %v", err)
		}
	})
}
