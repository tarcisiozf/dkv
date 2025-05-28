package cluster

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/internal/comm"
	"github.com/tarcisiozf/dkv/engine/internal/conf"
	"github.com/tarcisiozf/dkv/engine/internal/registries"
	"github.com/tarcisiozf/dkv/engine/internal/registries/nodereg"
	"github.com/tarcisiozf/dkv/engine/internal/storage"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"github.com/tarcisiozf/dkv/engine/nodes/types/id"
	"github.com/tarcisiozf/dkv/engine/nodes/types/role"
	"github.com/tarcisiozf/dkv/engine/nodes/types/status"
	"github.com/tarcisiozf/dkv/internal/faults"
	"github.com/tarcisiozf/dkv/internal/retry"
	"github.com/tarcisiozf/dkv/slots"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

var waitForAllNodes = 3 * time.Second

type UpdateParamsRequest struct {
	Status       status.Status
	Role         role.Role
	SlotRange    slots.SlotRange
	NewSlotRange slots.SlotRange
	ReplicaOf    id.ID
}

type Coordinator struct {
	node           *nodes.Node
	nodes          *nodes.Set
	config         conf.Config
	rpcClients     *registries.RpcClientManager
	storageManager *storage.Manager
	nodeRegistry   *nodereg.NodeRegistry
	nodeSyncer     *NodeSyncer
}

func NewCoordinator(
	node *nodes.Node,
	nodes *nodes.Set,
	config conf.Config,
	rpcClients *registries.RpcClientManager,
	storageManager *storage.Manager,
	nodeRegistry *nodereg.NodeRegistry,
	nodeSyncer *NodeSyncer,
) *Coordinator {
	return &Coordinator{
		node:           node,
		nodes:          nodes,
		config:         config,
		rpcClients:     rpcClients,
		storageManager: storageManager,
		nodeRegistry:   nodeRegistry,
		nodeSyncer:     nodeSyncer,
	}
}

func (c *Coordinator) RebalanceCluster() error {
	_, err := c.nodeRegistry.WithCoordinatorLock(c.node.ID, func() error {
		// wait for all nodes to register
		time.Sleep(waitForAllNodes)

		distributionStrategy := selectDistributionStrategy(c.config, c.nodes)
		if distributionStrategy == nil {
			log.Printf("[INFO] No distribution strategy found")
			return nil
		}

		updates, err := distributionStrategy.execute(c.nodes)
		if err != nil {
			return fmt.Errorf("failed to distribute nodes in cluster: %w", err)
		}
		if len(updates) == 0 {
			log.Printf("[INFO] No updates to apply")
			return nil
		}

		if err := c.broadcastUpdates(updates); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to rebalance cluster: %w", err)
	}
	return nil
}

func (c *Coordinator) electNewWriter(lostWriter *nodes.Node) error {
	readReplicas := c.nodes.ReadReplicasOf(lostWriter.ID)

	// check replicas with most up-to-date offset
	var mostUpToDateOffset uint64
	var newWriter *nodes.Node

	if c.node.SlotRange.Equal(lostWriter.SlotRange) {
		mostUpToDateOffset = c.storageManager.WalOffset()
		newWriter = c.node
	}

	for _, node := range readReplicas {
		if !node.SlotRange.Equal(lostWriter.SlotRange) {
			continue
		}

		rpcClient, err := c.rpcClients.GetClient(node)
		if err != nil {
			log.Printf("[ERROR] Failed to get RPC client for node %s: %v\n", node.ID, err)
			continue
		}

		offset, err := rpcClient.CurrentOffset()
		if err != nil {
			log.Printf("[ERROR] Error getting current offset of node %s: %v\n", node.ID, err)
			continue
		}
		if offset > mostUpToDateOffset {
			mostUpToDateOffset = offset
			newWriter = node
		}
	}

	if newWriter == nil {
		return fmt.Errorf("[ERROR] Failed to promote new writer for range %s", lostWriter.SlotRange)
	}

	// promote node as writer
	updates := map[string]*UpdateParamsRequest{
		newWriter.ID.String(): {
			Status:    status.Ready,
			Role:      role.Writer,
			ReplicaOf: id.Null(),
		},
	}

	// notify read-replicas about new leader
	for _, node := range readReplicas {
		if node.ID.Equal(newWriter.ID) {
			continue
		}
		updates[node.ID.String()] = &UpdateParamsRequest{
			Status:    status.Sync,
			Role:      role.ReadReplica,
			ReplicaOf: newWriter.ID,
		}
	}

	// broadcast updates
	if err := c.broadcastUpdates(updates); err != nil {
		return fmt.Errorf("failed to broadcast updates: %w", err)
	}

	log.Println("[INFO] New writer promoted:", newWriter.ID)
	return nil
}

func (c *Coordinator) broadcastUpdates(updates map[string]*UpdateParamsRequest) error {
	eg := errgroup.Group{}
	for strId, args := range updates {
		strId, args := strId, args
		eg.Go(func() error {
			nodeId, err := id.Parse(strId)
			if err != nil {
				return fmt.Errorf("failed to parse node ID %s: %w", strId, err)
			}
			if nodeId.Equal(c.node.ID) {
				return c.UpdateOwnParams(args)
			}
			node := c.nodes.GetById(nodeId)
			rpcClient, err := c.rpcClients.GetClient(node)
			if err != nil {
				return fmt.Errorf("failed to get RPC client for node %s: %w", nodeId, err)
			}
			return rpcClient.UpdateParams(&comm.UpdateParamsArgs{
				Status:       args.Status.String(),
				Role:         args.Role.String(),
				SlotRange:    args.SlotRange,
				NewSlotRange: args.NewSlotRange,
				ReplicaOf:    args.ReplicaOf.String(),
			})
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to update node params: %w", err)
	}
	return nil
}

func (c *Coordinator) UpdateOwnParams(update *UpdateParamsRequest) error {
	el := faults.ErrList{}
	node := c.node

	if !update.Role.IsNull() && !node.Role.Equal(update.Role) {
		node.Role = update.Role
	}
	if !update.SlotRange.IsEmpty() && !node.SlotRange.Equal(update.SlotRange) {
		node.SlotRange = update.SlotRange
	}
	if !update.ReplicaOf.IsNull() && !node.ReplicaOf.Equal(update.ReplicaOf) {
		node.ReplicaOf = update.ReplicaOf
	}
	if !update.NewSlotRange.IsEmpty() && !node.NewSlotRange.Equal(update.NewSlotRange) {
		node.NewSlotRange = update.NewSlotRange
	}
	if !update.Status.IsNull() && !node.Status.Equal(update.Status) {
		node.Status = update.Status
		if update.Status.IsSyncing() && update.Role.IsReadReplica() {
			if err := c.syncWithWriter(); err != nil {
				el.Add(fmt.Errorf("failed to sync with writer: %v", err))
			}
		}
		if update.Status.IsSyncing() && node.Role.IsWriter() {
			if err := c.syncSlot(); err != nil {
				el.Add(fmt.Errorf("failed to sync slot: %v", err))
			}
		}
	}

	if err := c.nodeRegistry.NotifyUpdate(node); err != nil {
		el.Add(fmt.Errorf("failed to notify node update: %w", err))
	}

	return el.Err()
}

func (c *Coordinator) OnNodeRemoved(node *nodes.Node) {
	if !c.node.ReplicaOf.Equal(node.ID) {
		return
	}

	_, err := c.nodeRegistry.WithCoordinatorLock(c.node.ID, func() error {
		return c.electNewWriter(node)
	})
	if err != nil {
		log.Printf("[ERROR] Failed to elect new writer: %v", err)
	}
}

func (c *Coordinator) syncWithWriter() error {
	node := c.node
	writer := c.nodes.GetById(node.ReplicaOf)
	if writer == nil {
		return fmt.Errorf("writer node %s not found", node.ReplicaOf)
	}

	if err := c.nodeSyncer.syncWithWriter(node, writer); err != nil {
		return fmt.Errorf("failed to sync with writer: %w", err)
	}

	node.Status = status.Ready
	if err := c.nodeRegistry.NotifyUpdate(node); err != nil {
		return fmt.Errorf("failed to notify node update: %w", err)
	}

	return nil
}

func (c *Coordinator) syncSlot() error {
	node := c.node
	slotOwners := c.nodes.IntersectingRange(node.SlotRange)

	if err := c.nodeSyncer.syncSlotRange(node, slotOwners); err != nil {
		return fmt.Errorf("failed to sync slot range: %w", err)
	}

	node.Status = status.Ready
	if err := c.nodeRegistry.NotifyUpdate(node); err != nil {
		return fmt.Errorf("failed to notify node update: %w", err)
	}

	err := retry.NewRetry(
		retry.WithTimeout(30*time.Second),
		retry.WithInterval(time.Second),
	).Do(func() error {
		for _, rr := range c.nodes.ReadReplicasOf(node.ID) {
			if !rr.Status.IsReady() {
				return fmt.Errorf("node %s is not ready", rr.ID)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to wait for read replicas to be ready: %w", err)
	}

	updates := make(map[string]*UpdateParamsRequest)
	for _, node := range c.nodes.All() {
		if node.NewSlotRange.IsEmpty() {
			continue
		}
		updates[node.ID.String()] = &UpdateParamsRequest{
			SlotRange: node.NewSlotRange,
		}
	}
	if err := c.broadcastUpdates(updates); err != nil {
		return fmt.Errorf("failed to broadcast updates: %w", err)
	}

	return nil
}
