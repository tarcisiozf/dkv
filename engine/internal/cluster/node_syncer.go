package cluster

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/db"
	"github.com/tarcisiozf/dkv/engine/internal/registries"
	"github.com/tarcisiozf/dkv/engine/internal/storage"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

type NodeSyncer struct {
	rpcClients    *registries.RpcClientManager
	storageManger *storage.Manager
}

func NewNodeSyncer(rpcClients *registries.RpcClientManager, storageManger *storage.Manager) *NodeSyncer {
	return &NodeSyncer{
		rpcClients:    rpcClients,
		storageManger: storageManger,
	}
}

func (ns *NodeSyncer) syncWithWriter(node, writer *nodes.Node) error {
	if !node.ReplicaOf.Equal(writer.ID) {
		return fmt.Errorf("node %s is not a replica of writer %s", node.ID, writer.ID)
	}
	log.Printf("[INFO] Read replica %s is now syncing with writer %s", node.ID, writer.ID)

	rpcClient, err := ns.rpcClients.GetClient(writer)
	if err != nil {
		return fmt.Errorf("failed to get RPC client for writer node %s: %w", writer.ID, err)
	}

	for {
		stat, err := rpcClient.Status()
		if err != nil {
			return fmt.Errorf("failed to get writer status: %w", err)
		}
		if !stat.IsReady() {
			log.Printf("[INFO] Node %s: writer node %s is not ready: %s", node.ID, writer.ID, stat)
			time.Sleep(1 * time.Second)
			continue
		}

		writerOffset, err := rpcClient.CurrentOffset()
		if err != nil {
			return fmt.Errorf("failed to get writer offset: %w", err)
		}
		if ns.storageManger.WalOffset() >= writerOffset {
			break
		}
		entries, offset, err := rpcClient.EntriesFromOffset(ns.storageManger.WalOffset())
		if err != nil {
			return fmt.Errorf("failed to get entries from offset: %w", err)
		}
		for _, e := range entries {
			if err := ns.storageManger.ApplyWalEntry(e); err != nil {
				return fmt.Errorf("failed to apply entry: %w", err)
			}
		}
		if ns.storageManger.WalOffset() != offset {
			return fmt.Errorf("offset mismatch: %d != %d", ns.storageManger.WalOffset(), offset)
		}
	}

	log.Printf("[INFO] Read replica %s is now in sync with writer %s", node.ID, writer.ID)

	return nil
}

func (ns *NodeSyncer) syncSlotRange(node *nodes.Node, slotOwners []*nodes.Node) error {
	log.Printf("[INFO] Node %s is now syncing with slot %s", node.ID, node.SlotRange)

	ch := make(chan db.Entry, len(slotOwners))
	done := make(chan bool)
	var eg errgroup.Group
	var foundNodes bool

	for _, slotOwner := range slotOwners {
		if slotOwner.ID.Equal(node.ID) {
			continue
		}
		foundNodes = true

		eg.Go(func() error {
			log.Printf("[INFO] Node %s: syncing with slot owner %s", node.ID, slotOwner.ID)

			rpcClient, err := ns.rpcClients.GetClient(slotOwner)
			if err != nil {
				return fmt.Errorf("failed to get RPC client for node %s: %w", slotOwner.ID, err)
			}
			entries, err := rpcClient.EntriesFromSlot(slotOwner.SlotRange)
			if err != nil {
				return fmt.Errorf("failed to get entries from %s: %w", slotOwner.ID, err)
			}

			log.Printf("[INFO] Node %s: received %d entries from slot owner %s", node.ID, len(entries), slotOwner.ID)

			for _, e := range entries {
				ch <- e
			}
			return nil
		})
	}

	if !foundNodes {
		return fmt.Errorf("no nodes found for slot range %s", node.SlotRange)
	}

	go func() {
		for entry := range ch {
			if err := ns.storageManger.ApplyDbEntry(entry); err != nil {
				log.Printf("[ERROR] Failed to apply entry: %v", err) // TODO: handle error
				panic(err)
			}
		}
		done <- true
	}()

	err := eg.Wait()
	close(ch)
	<-done

	if err != nil {
		return fmt.Errorf("failed to sync slot: %w", err)
	}

	log.Printf("[INFO] Sync completed for node %s", node.ID)
	return nil
}
