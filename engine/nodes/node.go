package nodes

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/nodes/types/id"
	"github.com/tarcisiozf/dkv/engine/nodes/types/mode"
	"github.com/tarcisiozf/dkv/engine/nodes/types/role"
	"github.com/tarcisiozf/dkv/engine/nodes/types/status"
	"github.com/tarcisiozf/dkv/slots"
)

type Node struct {
	ID                id.ID
	Mode              mode.Mode
	Role              role.Role
	Status            status.Status
	SlotRange         slots.SlotRange
	NewSlotRange      slots.SlotRange
	Address           string
	HttpPort          string
	RpcPort           string
	ReplicationFactor int
	WriteQuorum       int
	ReplicaOf         id.ID
}

func (n Node) String() string {
	return fmt.Sprintf(
		"Node{\n\tID: %s,\n\tMode: %s,\n\tRole: %s,\n\tStatus: %s,\n\tSlotRange: %s,\n\tNewSlotRange: %s,\n\tAddress: %s,\n\tHttpPort: %s,\n\tRpcPort: %s,\n\tReplicationFactor: %d,\n\tWriteQuorum: %d,\n\tReplicaOf: %s\n}",
		n.ID,
		n.Mode,
		n.Role,
		n.Status,
		n.SlotRange,
		n.NewSlotRange,
		n.Address,
		n.HttpPort,
		n.RpcPort,
		n.ReplicationFactor,
		n.WriteQuorum,
		n.ReplicaOf,
	)
}
