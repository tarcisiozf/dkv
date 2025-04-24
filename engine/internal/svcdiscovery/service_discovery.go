package svcdiscovery

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"log"
	"strings"
	"time"
)

var (
	ErrNodeExists   = errors.New("node already exists")
	ErrNodeNotFound = errors.New("node not found")
)

type MetadataUpdate struct {
	Data    []byte
	Version int32
}

type ServiceDiscovery struct {
	zookeeper *zk.Conn
}

func NewServiceDiscovery(zookeeper *zk.Conn) *ServiceDiscovery {
	return &ServiceDiscovery{
		zookeeper: zookeeper,
	}
}

func (sd *ServiceDiscovery) Discover(path string) ([]string, error) {
	exists, _, err := sd.zookeeper.Exists(path)
	if err != nil {
		return nil, fmt.Errorf("failed to check existence of path %s: %w", path, err)
	}
	if !exists {
		return []string{}, nil
	}

	children, _, err := sd.zookeeper.Children(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get children of path %s: %w", path, err)
	}

	return children, nil
}

func (sd *ServiceDiscovery) Watch(ctx context.Context, path string) <-chan []string {
	updates := make(chan []string)
	lastCversion := int32(-1)

	go func() {
		defer close(updates)

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}

			children, stat, err := sd.zookeeper.Children(path)
			if err != nil {
				if errors.Is(err, zk.ErrConnectionClosed) {
					log.Printf("[INFO] SD - watcher: connection closed")
					return
				}
				if errors.Is(err, zk.ErrNoNode) {
					log.Printf("[INFO] SD - watcher: node %s does not exist", path)
					return
				}
				log.Printf("[ERROR] SD - watcher: failed to discover services: %v", err)
				continue
			}
			// check if the children version has changed (added or removed)
			if stat.Cversion == lastCversion {
				continue
			}

			lastCversion = stat.Cversion
			updates <- children
		}
	}()

	return updates
}

func (sd *ServiceDiscovery) WatchData(ctx context.Context, path string, lastVersion int32) <-chan *MetadataUpdate {
	updates := make(chan *MetadataUpdate)

	go func() {
		defer close(updates)

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}

			data, stat, err := sd.zookeeper.Get(path)
			if err != nil {
				if errors.Is(err, zk.ErrNoNode) {
					log.Printf("[INFO] SD - watcher: node %s does not exist", path)
					return
				}
				if errors.Is(err, zk.ErrConnectionClosed) {
					log.Printf("[INFO] SD - watcher: connection closed")
					return
				}
				log.Printf("[ERROR] SD - watcher: failed to get data for path %s: %v", path, err)
				continue
			}
			if stat.Version == lastVersion {
				continue
			}

			lastVersion = stat.Version
			updates <- &MetadataUpdate{data, stat.Version}
		}
	}()

	return updates
}

func (sd *ServiceDiscovery) Register(path string, name string, data []byte) error {
	if err := sd.ensurePathExists(path); err != nil {
		return fmt.Errorf("failed to ensure path exists: %w", err)
	}

	nodePath := path + "/" + name
	createdPath, err := sd.zookeeper.Create(nodePath, data, zk.FlagPersistent, zk.WorldACL(zk.PermAll))
	if err != nil {
		if errors.Is(err, zk.ErrNodeExists) {
			log.Printf("[INFO] SD - node already exists: %s", nodePath)
			return ErrNodeExists
		}
		return fmt.Errorf("failed to create node %s: %w", nodePath, err)
	}
	if createdPath != nodePath {
		log.Printf("[INFO] SD - node created at %s, expected %s", createdPath, nodePath)
		return fmt.Errorf("node created at %s, expected %s", createdPath, nodePath)
	}

	log.Printf("[INFO] SD - registered service: %s at %s with %s", name, createdPath, data)
	return nil
}

func (sd *ServiceDiscovery) RegisterEphemeral(path string, name string, data []byte) (string, error) {
	if err := sd.ensurePathExists(path); err != nil {
		return "", fmt.Errorf("failed to ensure path exists: %w", err)
	}

	nodePath := path + "/" + name
	createdPath, err := sd.zookeeper.Create(nodePath, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		if errors.Is(err, zk.ErrNodeExists) {
			log.Printf("[INFO] SD - ephemeral node already exists: %s", nodePath)
			return "", ErrNodeExists
		}
		return "", fmt.Errorf("failed to create ephemeral node %s: %w", nodePath, err)
	}

	log.Printf("[INFO] SD - registered ephemeral node: %s at %s with %s", name, createdPath, data)
	return createdPath, nil
}

func (sd *ServiceDiscovery) Remove(path string) error {
	err := sd.zookeeper.Delete(path, -1)
	if err != nil {
		return fmt.Errorf("failed to delete node %s: %w", path, err)
	}
	log.Printf("[INFO] SD - removed node: %s", path)
	return nil
}

func (sd *ServiceDiscovery) ensurePathExists(path string) error {
	parts := strings.Split(path, "/")
	currentPath := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		currentPath += "/" + part
		exists, _, err := sd.zookeeper.Exists(currentPath)
		if err != nil {
			return fmt.Errorf("failed to check existence of path %s: %w", currentPath, err)
		}
		if exists {
			continue
		}
		_, err = sd.zookeeper.Create(currentPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil && !errors.Is(err, zk.ErrNodeExists) {
			return fmt.Errorf("failed to create path %s: %w", currentPath, err)
		}
		log.Printf("[INFO] SD - created path: %s", currentPath)
	}
	return nil
}

func (sd *ServiceDiscovery) UpdateData(path string, data []byte) error {
	_, err := sd.zookeeper.Set(path, data, -1)
	if err != nil {
		return fmt.Errorf("failed to update data for node %s: %w", path, err)
	}
	log.Printf("[INFO] SD - updated data for node: %s with %d", path, len(data))
	return nil
}

func (sd *ServiceDiscovery) GetData(path string) ([]byte, *zk.Stat, error) {
	data, stat, err := sd.zookeeper.Get(path)
	if err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			return nil, nil, ErrNodeNotFound
		}
		return nil, nil, fmt.Errorf("failed to get data for path %s: %w", path, err)
	}
	return data, stat, nil
}

func (sd *ServiceDiscovery) Exists(path string) (bool, error) {
	exists, _, err := sd.zookeeper.Exists(path)
	if err != nil {
		return false, fmt.Errorf("failed to check existence of path %s: %w", path, err)
	}
	return exists, nil
}
