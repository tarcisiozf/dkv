package engine

import (
	"errors"
	"fmt"
	"github.com/tarcisiozf/dkv/engine/internal/conf"
	"github.com/tarcisiozf/dkv/engine/internal/filesys"
	"github.com/tarcisiozf/dkv/engine/nodes/types/id"
	"github.com/tarcisiozf/dkv/engine/nodes/types/mode"
	"github.com/tarcisiozf/dkv/internal/net"
	"strings"
	"time"
)

type ConfigOption func(conf.Config) (conf.Config, error)

var defaultConfig = conf.Config{
	NodeId:             id.Null(),
	Zookeeper:          []string{},
	DirPath:            "./db",
	NodeMode:           mode.Standalone,
	ReplicationFactor:  1,
	WriteQuorum:        1,
	AllowReadOnWriter:  true,
	ZNodeBasePath:      "/dkv",
	TransactionTimeout: 60 * time.Second,
	FileSystem:         filesys.NewDiskFileSystem(),
}

func WithZookeeper(zookeeper ...string) ConfigOption {
	return func(config conf.Config) (conf.Config, error) {
		if len(zookeeper) == 0 {
			return config, errors.New("zookeeper address is required")
		}
		config.Zookeeper = zookeeper
		return config, nil
	}
}

func WithDirPath(dirPath string) ConfigOption {
	return func(config conf.Config) (conf.Config, error) {
		config.DirPath = strings.TrimSuffix(dirPath, "/")
		return config, nil
	}
}

func WithAddress(address string) ConfigOption {
	return func(config conf.Config) (conf.Config, error) {
		if !net.IsValidIP(address) {
			return config, fmt.Errorf("invalid IP address: %s", address)
		}
		config.Address = address
		return config, nil
	}
}

func WithHttpPort(port string) ConfigOption {
	return func(config conf.Config) (conf.Config, error) {
		if !net.IsValidPort(port) {
			return config, fmt.Errorf("invalid port: %s", port)
		}
		config.HttpPort = port
		return config, nil
	}
}

func WithRpcPort(port string) ConfigOption {
	return func(config conf.Config) (conf.Config, error) {
		if !net.IsValidPort(port) {
			return config, fmt.Errorf("invalid port: %s", port)
		}
		config.RpcPort = port
		return config, nil
	}
}

func WithReplicationFactor(factor int) ConfigOption {
	return func(config conf.Config) (conf.Config, error) {
		if factor < 1 {
			return config, errors.New("replication factor must be at least 1")
		}
		config.ReplicationFactor = factor
		return config, nil
	}
}

func WithWriteQuorum(quorum int) ConfigOption {
	return func(config conf.Config) (conf.Config, error) {
		if quorum < 1 {
			return config, errors.New("write quorum must be at least 1")
		}
		config.WriteQuorum = quorum
		return config, nil
	}
}

func WithMode(m string) ConfigOption {
	return func(config conf.Config) (conf.Config, error) {
		nodeMode, err := mode.Parse(m)
		if err != nil {
			return config, err
		}
		config.NodeMode = nodeMode
		return config, nil
	}
}

func WithNodeID(nodeId string) ConfigOption {
	return func(config conf.Config) (conf.Config, error) {
		nid, err := id.Parse(nodeId)
		if err != nil {
			return config, fmt.Errorf("invalid node ID: %w", err)
		}
		config.NodeId = nid
		return config, nil
	}
}

func WithDisableReadOnWriter() ConfigOption {
	return func(config conf.Config) (conf.Config, error) {
		config.AllowReadOnWriter = false
		return config, nil
	}
}

func WithZNodeBasePath(path string) ConfigOption {
	return func(config conf.Config) (conf.Config, error) {
		if !strings.HasPrefix(path, "/dkv") {
			return config, fmt.Errorf("invalid zk base path: %s", path)
		}
		config.ZNodeBasePath = strings.TrimSuffix(path, "/")
		return config, nil
	}
}

func WithFileSystem(fs filesys.FileSystem) ConfigOption {
	return func(config conf.Config) (conf.Config, error) {
		if fs == nil {
			return config, errors.New("file system cannot be nil")
		}
		config.FileSystem = fs
		return config, nil
	}
}

func validateConfig(config conf.Config) (conf.Config, error) {
	if len(config.Zookeeper) == 0 {
		return config, errors.New("zookeeper address is required")
	}

	if config.DirPath == "" {
		return config, errors.New("dirPath is required")
	}

	if config.Address == "" {
		ip, err := net.GetLocalIP()
		if err != nil {
			return config, fmt.Errorf("failed to get local IP address: %w", err)
		}
		config.Address = ip
	}

	if config.HttpPort == "" {
		return config, errors.New("http port is required")
	}

	if config.RpcPort == "" {
		return config, errors.New("rpc port is required")
	}

	if config.ReplicationFactor < config.WriteQuorum {
		return config, fmt.Errorf("replication factor %d is less than write quorum %d, quorum will never be reach", config.ReplicationFactor, config.WriteQuorum)
	}

	return config, nil
}
