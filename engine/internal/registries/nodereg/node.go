package nodereg

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"github.com/tarcisiozf/dkv/engine/nodes/types/id"
	"github.com/tarcisiozf/dkv/engine/nodes/types/mode"
	"github.com/tarcisiozf/dkv/engine/nodes/types/role"
	"github.com/tarcisiozf/dkv/engine/nodes/types/status"
	"os"
	"path/filepath"
	"strings"
)

type NodeData struct {
	ID                string    `json:"id"`
	Mode              string    `json:"mode"`
	Role              string    `json:"role"`
	Status            string    `json:"status"`
	SlotRange         [2]uint16 `json:"slot_range"`
	NewSlotRange      [2]uint16 `json:"new_slot_range"`
	Address           string    `json:"address"`
	HttpPort          string    `json:"http_port"`
	RpcPort           string    `json:"rpc_port"`
	ReplicationFactor int       `json:"replication_factor"`
	WriteQuorum       int       `json:"write_quorum"`
	ReplicaOf         string    `json:"replica_of"`
}

func (n *NodeData) ToNode(node *nodes.Node) (err error) {
	node.ID, err = id.Parse(n.ID)
	if err != nil {
		return fmt.Errorf("parsing node ID: %w", err)
	}
	node.Mode, err = mode.Parse(n.Mode)
	if err != nil {
		return fmt.Errorf("parsing node mode: %w", err)
	}
	node.Role, err = role.Parse(n.Role)
	if err != nil {
		return fmt.Errorf("parsing node role: %w", err)
	}
	node.Status, err = status.Parse(n.Status)
	if err != nil {
		return fmt.Errorf("parsing node status: %w", err)
	}
	if n.ReplicaOf != "" {
		node.ReplicaOf, err = id.Parse(n.ReplicaOf)
		if err != nil {
			return fmt.Errorf("parsing replica of ID: %w", err)
		}
	} else {
		node.ReplicaOf = id.Null()
	}

	node.SlotRange = n.SlotRange
	node.NewSlotRange = n.NewSlotRange
	node.Address = n.Address
	node.HttpPort = n.HttpPort
	node.RpcPort = n.RpcPort
	node.ReplicationFactor = n.ReplicationFactor
	node.WriteQuorum = n.WriteQuorum
	return nil
}

func toNodeData(node *nodes.Node) *NodeData {
	return &NodeData{
		ID:                node.ID.String(),
		Mode:              node.Mode.String(),
		Role:              node.Role.String(),
		Status:            node.Status.String(),
		SlotRange:         node.SlotRange,
		NewSlotRange:      node.NewSlotRange,
		Address:           node.Address,
		HttpPort:          node.HttpPort,
		RpcPort:           node.RpcPort,
		ReplicationFactor: node.ReplicationFactor,
		WriteQuorum:       node.WriteQuorum,
		ReplicaOf:         node.ReplicaOf.String(),
	}
}

func GetOrCreateNodeID(nodeId id.ID, path string) (id.ID, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		trimmed := strings.TrimSpace(string(data))
		nid, err := id.Parse(trimmed)
		if err != nil {
			return id.Null(), fmt.Errorf("parsing NODEID: %w", err)
		}
		if nodeId.IsNull() || nid.Equal(nodeId) {
			return nid, nil
		}
	}

	if !os.IsNotExist(err) {
		return id.Null(), fmt.Errorf("reading NODEID: %w", err)
	}

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return id.Null(), fmt.Errorf("creating directory %q: %w", dir, err)
	}

	// Generate a random uint64
	if nodeId.IsNull() {
		nodeId, err = id.Generate()
		if err != nil {
			return id.Null(), fmt.Errorf("generating random node ID: %w", err)
		}
	}
	if err := os.WriteFile(path, []byte(nodeId.String()), 0600); err != nil {
		return id.Null(), fmt.Errorf("writing NODEID file: %w", err)
	}

	return nodeId, nil
}
