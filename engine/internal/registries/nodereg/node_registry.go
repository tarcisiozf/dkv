package nodereg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tarcisiozf/dkv/engine/internal/svcdiscovery"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"github.com/tarcisiozf/dkv/engine/nodes/types/id"
	"log"
	"strings"
	"sync"
)

type NodeCallback func(*nodes.Node)

type NodeRegistry struct {
	serviceDiscovery       *svcdiscovery.ServiceDiscovery
	zkMeta                 *ZooKeeperMetaManager
	nodes                  *nodes.Set
	updateMutex            *sync.Mutex
	removalListeners       []NodeCallback
	addressChangeListeners []NodeCallback
	nodeDownListeners      []NodeCallback
	zkBasePath             string
	zkNodePath             string
}

func NewNodeRegistry(
	zkBasePath string,
	serviceDiscovery *svcdiscovery.ServiceDiscovery,
	nodeSet *nodes.Set,
) (*NodeRegistry, error) {
	if !strings.HasPrefix(zkBasePath, "/dkv") {
		return nil, fmt.Errorf("invalid zk base path: %s", zkBasePath)
	}
	zkNodePath := zkBasePath + "/nodes"

	return &NodeRegistry{
		zkBasePath:             zkBasePath,
		zkNodePath:             zkNodePath,
		serviceDiscovery:       serviceDiscovery,
		zkMeta:                 NewZooKeeperMetaManager(),
		nodes:                  nodeSet,
		updateMutex:            &sync.Mutex{},
		removalListeners:       make([]NodeCallback, 0),
		addressChangeListeners: make([]NodeCallback, 0),
		nodeDownListeners:      make([]NodeCallback, 0),
	}, nil
}

func (nr *NodeRegistry) WithCoordinatorLock(nodeId id.ID, fn func() error) (bool, error) {
	nodePath, err := nr.serviceDiscovery.RegisterEphemeral(nr.zkBasePath, "coordinator", []byte(nodeId.String()))
	if err != nil {
		if errors.Is(err, svcdiscovery.ErrNodeExists) {
			log.Printf("[INFO] Another node is already coordinator")
			return false, nil
		}
		return false, fmt.Errorf("failed to register as coordinator: %w", err)
	}
	fnErr := fn()
	if err := nr.serviceDiscovery.Remove(nodePath); err != nil {
		log.Printf("[ERROR] Failed to unregister as coordinator: %v", err)
	}
	return true, fnErr
}

func (nr *NodeRegistry) NotifyUpdate(node *nodes.Node) error {
	data, err := json.Marshal(toNodeData(node))
	if err != nil {
		return fmt.Errorf("failed to marshal node info: %w", err)
	}
	meta, err := nr.zkMeta.Get(node.ID)
	if err != nil {
		return fmt.Errorf("failed to get node metadata: %w", err)
	}
	if err := nr.serviceDiscovery.UpdateData(meta.Path, data); err != nil {
		return fmt.Errorf("failed to update node info: %w", err)
	}
	return nil
}

func (nr *NodeRegistry) RegisterOrLoad(node *nodes.Node) (err error) {
	path := nr.zkNodePath + "/" + node.ID.String()
	exists, err := nr.serviceDiscovery.Exists(path)
	if err != nil {
		return fmt.Errorf("failed to check if node exists: %w", err)
	}

	version := int32(-1)
	if exists {
		version, err = nr.load(node, path)
		if err != nil {
			return fmt.Errorf("failed to load node data: %w", err)
		}
	} else {
		if err := nr.register(node); err != nil {
			return fmt.Errorf("failed to register node: %w", err)
		}
	}

	nr.nodes.Set(node)
	nr.zkMeta.Register(node.ID, path, version)

	return nil
}

func (nr *NodeRegistry) load(node *nodes.Node, path string) (int32, error) {
	data, stat, err := nr.serviceDiscovery.GetData(path)
	if err != nil {
		return -1, fmt.Errorf("[ERROR] Error getting node data for %s: %v", path, err)
	}
	nodeData := &NodeData{}
	if err := json.Unmarshal(data, nodeData); err != nil {
		return -1, fmt.Errorf("[ERROR] Error unmarshalling node data for %s: %v - data: %s", node.ID, err, data)
	}
	if err := nodeData.ToNode(node); err != nil {
		return -1, fmt.Errorf("[ERROR] Error converting node data to node for %s: %v - data: %s", node.ID, err, data)
	}
	return stat.Version, nil
}

func (nr *NodeRegistry) register(node *nodes.Node) error {
	data, err := json.Marshal(toNodeData(node))
	if err != nil {
		return fmt.Errorf("failed to marshal node info: %w", err)
	}

	err = nr.serviceDiscovery.Register(nr.zkNodePath, node.ID.String(), data)
	if err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	return nil
}

func (nr *NodeRegistry) WatchForNewNodes(ctx context.Context) {
	updates := nr.serviceDiscovery.Watch(ctx, nr.zkNodePath)
	for updatedNodes := range updates {
		nr.RefreshNodesFromZk(ctx, updatedNodes)
	}
	log.Println("[INFO] Closing nodes watcher")
}

func (nr *NodeRegistry) WatchDataChanges(ctx context.Context, path string, lastVersion int32) {
	updates := nr.serviceDiscovery.WatchData(ctx, path, lastVersion)
	for update := range updates {
		_, err := nr.syncNodeFromZkData(path, update.Data, update.Version)
		if err != nil {
			log.Printf("[ERROR] Error processing node metadata update for %s: %v", path, err)
			continue
		}
	}
	log.Println("[INFO] Closing node data watcher")
}

func (nr *NodeRegistry) RefreshNodesFromZk(ctx context.Context, updateNodes []string) {
	paths := make(map[string]bool)

	var wg sync.WaitGroup
	wg.Add(len(updateNodes))

	for _, path := range updateNodes {
		if !strings.HasPrefix(path, nr.zkNodePath) {
			path = nr.zkNodePath + "/" + path
		}
		paths[path] = true

		go func() {
			defer wg.Done()

			data, stat, err := nr.serviceDiscovery.GetData(path)
			if err != nil {
				log.Printf("[ERROR] Error getting node data for %s: %v", path, err)
				return
			}

			inserted, err := nr.syncNodeFromZkData(path, data, stat.Version)
			if err != nil {
				log.Printf("[ERROR] Error processing node metadata for %s: %v", path, err)
				return
			}
			if inserted {
				log.Printf("[INFO] New node discovered: %s", path)
				go nr.WatchDataChanges(ctx, path, stat.Version)
			}
		}()
	}

	wg.Wait()

	// check for removed nodes
	for _, node := range nr.nodes.All() {
		meta, err := nr.zkMeta.Get(node.ID)
		if err != nil {
			log.Printf("[ERROR] Node metadata not found for %v", err)
			continue
		}
		if _, ok := paths[meta.Path]; ok {
			continue
		}
		log.Println("[INFO] Node not found:", meta.Path)

		nr.nodes.Remove(node)

		for _, listener := range nr.removalListeners {
			go listener(node)
		}
	}
}

func (nr *NodeRegistry) syncNodeFromZkData(path string, data []byte, version int32) (bool, error) {
	nodeData := &NodeData{}
	if err := json.Unmarshal(data, nodeData); err != nil {
		return false, fmt.Errorf("[ERROR] Error unmarshalling node data for %s: %v - data: %s", path, err, data)
	}
	updatedNode := &nodes.Node{}
	if err := nodeData.ToNode(updatedNode); err != nil {
		return false, fmt.Errorf("[ERROR] Error converting node data to node for %s: %v - data: %s", path, err, data)
	}

	nr.updateMutex.Lock()
	defer nr.updateMutex.Unlock()

	prevNode := nr.nodes.GetById(updatedNode.ID)
	exists := prevNode != nil
	if exists {
		prevMeta, err := nr.zkMeta.Get(prevNode.ID)
		if err == nil && prevMeta.Version == version {
			return false, nil
		}
		if !prevNode.ID.Equal(updatedNode.ID) {
			panic(fmt.Sprintf("Node ID mismatch for %s: %s != %s", path, prevNode.ID, updatedNode.ID))
		}
		if prevNode.Address != updatedNode.Address {
			for _, listener := range nr.addressChangeListeners {
				go listener(updatedNode)
			}
		}
	}
	if updatedNode.Status.IsDown() {
		for _, listener := range nr.nodeDownListeners {
			go listener(updatedNode)
		}
	}

	nr.nodes.Set(updatedNode)
	nr.zkMeta.Register(updatedNode.ID, path, version)

	return !exists, nil
}

func (nr *NodeRegistry) AddRemovalListener(fn NodeCallback) {
	nr.removalListeners = append(nr.removalListeners, fn)
}

func (nr *NodeRegistry) AddAddressChangeListener(fn NodeCallback) {
	nr.addressChangeListeners = append(nr.addressChangeListeners, fn)
}

func (nr *NodeRegistry) AddNodeDownListener(fn NodeCallback) {
	nr.nodeDownListeners = append(nr.nodeDownListeners, fn)
}

func (nr *NodeRegistry) GetMeta(nodeId id.ID) (ZookeeperMeta, error) {
	return nr.zkMeta.Get(nodeId)
}

func (nr *NodeRegistry) Discover() ([]string, error) {
	return nr.serviceDiscovery.Discover(nr.zkNodePath)
}
