package registries

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/internal/comm"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"github.com/tarcisiozf/dkv/internal/faults"
	"sync"
)

type RpcClientManager struct {
	clients map[string]*comm.RpcClient
	mutex   *sync.RWMutex
}

func NewRpcClientManager() *RpcClientManager {
	return &RpcClientManager{
		clients: make(map[string]*comm.RpcClient),
		mutex:   &sync.RWMutex{},
	}
}

func (cm *RpcClientManager) GetClient(node *nodes.Node) (*comm.RpcClient, error) {
	for {
		cm.mutex.RLock()
		client, ok := cm.clients[node.ID.String()]
		if !ok {
			cm.mutex.RUnlock()
			if err := cm.register(node); err != nil {
				return nil, fmt.Errorf("registering client for %s: %w", node.ID.String(), err)
			}
			continue
		}
		cm.mutex.RUnlock()
		return client, nil
	}
}

func (cm *RpcClientManager) Close(node *nodes.Node) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	client, ok := cm.clients[node.ID.String()]
	if !ok {
		return nil
	}
	if err := client.Close(); err != nil {
		return fmt.Errorf("closing client for %s: %w", node.ID.String(), err)
	}
	delete(cm.clients, node.ID.String())
	return nil
}

func (cm *RpcClientManager) CloseAll() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	el := faults.ErrList{}

	for _, client := range cm.clients {
		if err := client.Close(); err != nil {
			el.Add(fmt.Errorf("closing client: %w", err))
		}
	}
	cm.clients = make(map[string]*comm.RpcClient)

	return el.Err()
}

func (cm *RpcClientManager) register(node *nodes.Node) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	if _, ok := cm.clients[node.ID.String()]; ok {
		return nil
	}

	client := comm.NewRpcClient(node)
	cm.clients[node.ID.String()] = client

	if err := client.Connect(); err != nil {
		return fmt.Errorf("connecting to %s: %w", node.ID.String(), err)
	}
	return nil
}
