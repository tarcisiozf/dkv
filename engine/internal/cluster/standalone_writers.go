package cluster

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"github.com/tarcisiozf/dkv/engine/nodes/types/role"
	"github.com/tarcisiozf/dkv/engine/nodes/types/status"
	"github.com/tarcisiozf/dkv/slots"
)

type StandaloneWritersStrategy struct{}

func newStandaloneWritersStrategy() *StandaloneWritersStrategy {
	return &StandaloneWritersStrategy{}
}

func (s *StandaloneWritersStrategy) execute(nodeSet *nodes.Set) (map[string]*UpdateParamsRequest, error) {
	idleNodes := nodeSet.WaitingForOrders()
	ranges, err := slots.FullRange().Partition(len(idleNodes))
	if err != nil {
		return nil, fmt.Errorf("failed to partition slot range: %w", err)
	}

	updates := make(map[string]*UpdateParamsRequest)
	for i, node := range idleNodes {
		updates[node.ID.String()] = &UpdateParamsRequest{
			SlotRange: ranges[i],
			Status:    status.Ready,
			Role:      role.Writer,
		}
	}

	return updates, nil
}
