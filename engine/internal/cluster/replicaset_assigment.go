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

type ReplicaSetAssignmentStrategy struct {
	config conf.Config
}

func newReplicaSetAssignmentStrategy(config conf.Config) *ReplicaSetAssignmentStrategy {
	return &ReplicaSetAssignmentStrategy{
		config: config,
	}
}

func (s *ReplicaSetAssignmentStrategy) execute(nodeSet *nodes.Set) (map[string]*UpdateParamsRequest, error) {
	idleNodes := nodeSet.WaitingForOrders()
	if len(idleNodes) == 0 {
		return nil, fmt.Errorf("no idle nodes available for assignment")
	}

	numWriters := len(idleNodes) / s.config.ReplicationFactor
	numReadReplicasPerWriter := s.config.ReplicationFactor - 1
	if len(idleNodes) < s.config.ReplicationFactor {
		numWriters = 1
		numReadReplicasPerWriter = len(idleNodes) - 1
	}

	if len(idleNodes) < numWriters+(numWriters*numReadReplicasPerWriter) {
		return nil, fmt.Errorf("not enough idle nodes to assign slots: %d/%d", len(idleNodes), numWriters+(numWriters*numReadReplicasPerWriter))
	}

	ranges, err := slots.FullRange().Partition(numWriters)
	if err != nil {
		return nil, fmt.Errorf("failed to partition slot range: %w", err)
	}

	updates := make(map[string]*UpdateParamsRequest)
	for i := 0; i < numWriters; i++ {
		wi := i * (numReadReplicasPerWriter + 1)
		writer := idleNodes[wi]
		slotRange := ranges[i]
		updates[writer.ID.String()] = &UpdateParamsRequest{
			SlotRange: slotRange,
			Status:    status.Ready,
			Role:      role.Writer,
		}
		log.Printf("[INFO] Assigning writer to node %s", writer.ID)

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
