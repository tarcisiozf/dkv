package cluster

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/internal/conf"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"github.com/tarcisiozf/dkv/engine/nodes/types/role"
	"github.com/tarcisiozf/dkv/engine/nodes/types/status"
	"log"
)

type ReplicaSetNewReplicaStrategy struct {
	config conf.Config
}

func newReplicaSetNewReplicaStrategy(config conf.Config) *ReplicaSetNewReplicaStrategy {
	return &ReplicaSetNewReplicaStrategy{
		config: config,
	}
}

func (r *ReplicaSetNewReplicaStrategy) execute(nodes *nodes.Set) (map[string]*UpdateParamsRequest, error) {
	writers := nodes.Writers()
	if len(writers) == 0 {
		return nil, fmt.Errorf("no writers available for assignment")
	}
	idleNodes := nodes.WaitingForOrders()
	if len(idleNodes) == 0 {
		return nil, fmt.Errorf("no idle nodes available for assignment")
	}

	updates := make(map[string]*UpdateParamsRequest)

	offset := 0
	for _, idleNode := range idleNodes {
		for i := offset; i < len(writers); i++ {
			writer := writers[i]
			readReplicas := nodes.ReadReplicasOf(writer.ID)
			if len(readReplicas) >= r.config.ReplicationFactor-1 {
				continue
			}
			updates[idleNode.ID.String()] = &UpdateParamsRequest{
				Status:    status.Sync,
				Role:      role.ReadReplica,
				SlotRange: writer.SlotRange,
				ReplicaOf: writer.ID,
			}
			offset = i + 1
			log.Printf("[INFO] Assigning read replica %s to node %s", idleNode.ID, writer.ID)
			break
		}
	}

	return updates, nil
}
