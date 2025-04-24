package tests

import (
	"context"
	"github.com/tarcisiozf/dkv/engine"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"github.com/tarcisiozf/dkv/engine/nodes/types/status"
	"github.com/tarcisiozf/dkv/engine/test"
	"testing"
)

func TestDbEngine_Registry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	testEnv := test.NewTestEnv(
		test.WithAutoStart(),
	)

	var instance *engine.DbEngine
	var node *nodes.Node
	t.Run("setup env", func(t *testing.T) {
		var err error
		instance, err = testEnv.CreateInstance(ctx)
		if err != nil {
			t.Fatalf("[FATAL] Error creating instance: %v", err)
		}
		node = instance.Node()
	})

	t.Run("stop instance", func(t *testing.T) {
		cancel()
		if err := instance.Close(); err != nil {
			t.Fatalf("[FATAL] Error stopping instance: %v", err)
		}
	})

	t.Run("run instance again", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := instance.Start(ctx); err != nil {
			t.Fatalf("[FATAL] Error starting instance: %v", err)
		}
		loadedNode := instance.Node()

		if !node.ID.Equal(loadedNode.ID) {
			t.Errorf("[FATAL] Node ID mismatch: expected %s, got %s", node.ID, loadedNode.ID)
		}
		if node.Address != loadedNode.Address {
			t.Errorf("[FATAL] Node address mismatch: expected %s, got %s", node.Address, loadedNode.Address)
		}
		if !node.Mode.Equal(loadedNode.Mode) {
			t.Errorf("[FATAL] Node mode mismatch: expected %s, got %s", node.Mode, loadedNode.Mode)
		}
		if !node.Role.Equal(loadedNode.Role) {
			t.Errorf("[FATAL] Node role mismatch: expected %s, got %s", node.Role, loadedNode.Role)
		}
		if !node.Status.IsDown() {
			t.Errorf("[FATAL] Node status mismatch: expected %s, got %s", node.Status, loadedNode.Status)
		}
		if !loadedNode.Status.Equal(status.Ready) {
			t.Errorf("[FATAL] Node status mismatch: expected %s, got %s", status.Ready, loadedNode.Status)
		}
		if !node.SlotRange.Equal(loadedNode.SlotRange) {
			t.Errorf("[FATAL] Node slot range mismatch: expected %s, got %s", node.SlotRange, loadedNode.SlotRange)
		}
		if !node.NewSlotRange.Equal(loadedNode.NewSlotRange) {
			t.Errorf("[FATAL] Node new slot range mismatch: expected %s, got %s", node.NewSlotRange, loadedNode.NewSlotRange)
		}
		if node.Address != loadedNode.Address {
			t.Errorf("[FATAL] Node address mismatch: expected %s, got %s", node.Address, loadedNode.Address)
		}
		if node.HttpPort != loadedNode.HttpPort {
			t.Errorf("[FATAL] Node HTTP port mismatch: expected %s, got %s", node.HttpPort, loadedNode.HttpPort)
		}
		if node.RpcPort != loadedNode.RpcPort {
			t.Errorf("[FATAL] Node RPC port mismatch: expected %s, got %s", node.RpcPort, loadedNode.RpcPort)
		}
		if node.ReplicationFactor != loadedNode.ReplicationFactor {
			t.Errorf("[FATAL] Node replication factor mismatch: expected %d, got %d", node.ReplicationFactor, loadedNode.ReplicationFactor)
		}
		if node.WriteQuorum != loadedNode.WriteQuorum {
			t.Errorf("[FATAL] Node write quorum mismatch: expected %d, got %d", node.WriteQuorum, loadedNode.WriteQuorum)
		}
		if !node.ReplicaOf.Equal(loadedNode.ReplicaOf) {
			t.Errorf("[FATAL] Node replica of mismatch: expected %s, got %s", node.ReplicaOf, loadedNode.ReplicaOf)
		}
	})

	_ = testEnv.Destroy(ctx)
}
