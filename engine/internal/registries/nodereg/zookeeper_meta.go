package nodereg

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/nodes/types/id"
)

type ZookeeperMeta struct {
	Version int32
	Path    string
}

type ZooKeeperMetaManager struct {
	nodes map[string]ZookeeperMeta
}

func NewZooKeeperMetaManager() *ZooKeeperMetaManager {
	return &ZooKeeperMetaManager{
		nodes: make(map[string]ZookeeperMeta),
	}
}

func (m *ZooKeeperMetaManager) Register(nodeId id.ID, path string, version int32) {
	m.nodes[nodeId.String()] = ZookeeperMeta{
		Path:    path,
		Version: version,
	}
}

func (m *ZooKeeperMetaManager) Get(nodeId id.ID) (ZookeeperMeta, error) {
	meta, ok := m.nodes[nodeId.String()]
	if !ok {
		return ZookeeperMeta{}, fmt.Errorf("meta for node %s not found", nodeId)
	}
	return meta, nil
}
