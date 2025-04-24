package cluster

import (
	"github.com/tarcisiozf/dkv/engine/internal/conf"
	"github.com/tarcisiozf/dkv/engine/nodes"
)

type DistributionStrategy interface {
	execute(nodes *nodes.Set) (map[string]*UpdateParamsRequest, error)
}

func selectDistributionStrategy(config conf.Config, nodes *nodes.Set) DistributionStrategy {
	if config.ReplicationFactor == 1 {
		return newStandaloneWritersStrategy()
	}

	writers := nodes.Writers()
	if len(writers) == 0 {
		return newReplicaSetAssignmentStrategy(config)
	}

	idleNodes := nodes.WaitingForOrders()
	if len(idleNodes) == 0 {
		return nil
	}

	for _, writer := range writers {
		readReplicas := nodes.ReadReplicasOf(writer.ID)
		if len(readReplicas) < config.ReplicationFactor-1 {
			return newReplicaSetNewReplicaStrategy(config)
		}
	}

	if len(idleNodes) >= config.ReplicationFactor {
		return newReplicaSetReshardingStrategy(config)
	}

	return nil
}
