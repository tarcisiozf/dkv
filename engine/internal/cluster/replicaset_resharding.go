package cluster

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/internal/conf"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"github.com/tarcisiozf/dkv/engine/nodes/types/role"
	"github.com/tarcisiozf/dkv/engine/nodes/types/status"
	"github.com/tarcisiozf/dkv/slots"
	"log"
)

type ReplicaSetReshardingStrategy struct {
	config conf.Config
}

func newReplicaSetReshardingStrategy(config conf.Config) *ReplicaSetReshardingStrategy {
	return &ReplicaSetReshardingStrategy{
		config: config,
	}
}

func (s *ReplicaSetReshardingStrategy) execute(nodeSet *nodes.Set) (map[string]*UpdateParamsRequest, error) {
	idleNodes := nodeSet.WaitingForOrders()
	numWriters := nodeSet.Count() / s.config.ReplicationFactor
	numReadReplicasPerWriter := s.config.ReplicationFactor - 1

	if nodeSet.Count() < numWriters+(numWriters*numReadReplicasPerWriter) {
		return nil, fmt.Errorf("not enough idle nodes to assign slots: %d/%d", len(idleNodes), numWriters+(numWriters*numReadReplicasPerWriter))
	}

	ranges, err := slots.FullRange().Partition(numWriters)
	if err != nil {
		return nil, fmt.Errorf("failed to partition slot range: %w", err)
	}

	writers := nodeSet.Writers()

	updates := make(map[string]*UpdateParamsRequest)
	for i := 0; i < len(writers); i++ {
		writer := writers[i]
		slotRange := ranges[i]
		updates[writer.ID.String()] = &UpdateParamsRequest{
			NewSlotRange: slotRange,
		}
		log.Printf("[INFO] Assigning writer to node %s", writer.ID)

		for _, readReplica := range nodeSet.ReadReplicasOf(writer.ID) {
			updates[readReplica.ID.String()] = &UpdateParamsRequest{
				NewSlotRange: slotRange,
			}
			log.Printf("[INFO] Assigning read replica to node %s", readReplica.ID)
		}
	}

	numNewWriters := len(idleNodes) / numReadReplicasPerWriter

	// Assign new slots to idle nodes
	for i := 0; i < numNewWriters; i++ {
		wi := i * (numReadReplicasPerWriter + 1)
		writer := idleNodes[wi]
		slotRange := ranges[len(writers)+i]
		updates[writer.ID.String()] = &UpdateParamsRequest{
			SlotRange: slotRange,
			Status:    status.Sync,
			Role:      role.Writer,
		}
		log.Printf("[INFO] Assigning new writer to node %s", writer.ID)

		for j := 0; j < numReadReplicasPerWriter; j++ {
			readReplica := idleNodes[wi+j+1]
			updates[readReplica.ID.String()] = &UpdateParamsRequest{
				Role:      role.ReadReplica,
				Status:    status.Sync,
				ReplicaOf: writer.ID,
				SlotRange: slotRange,
			}
			log.Printf("[INFO] Assigning read replica to node %s", readReplica.ID)
		}
	}

	return updates, nil
}
