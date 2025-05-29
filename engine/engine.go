package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"github.com/tarcisiozf/dkv/engine/internal/cluster"
	"github.com/tarcisiozf/dkv/engine/internal/conf"
	"github.com/tarcisiozf/dkv/engine/internal/disttx"
	"github.com/tarcisiozf/dkv/engine/internal/kv"
	"github.com/tarcisiozf/dkv/engine/internal/registries"
	"github.com/tarcisiozf/dkv/engine/internal/registries/nodereg"
	"github.com/tarcisiozf/dkv/engine/internal/storage"
	"github.com/tarcisiozf/dkv/engine/internal/svcdiscovery"
	"github.com/tarcisiozf/dkv/engine/internal/wal"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"github.com/tarcisiozf/dkv/engine/nodes/types/id"
	"github.com/tarcisiozf/dkv/engine/nodes/types/role"
	"github.com/tarcisiozf/dkv/engine/nodes/types/status"
	"github.com/tarcisiozf/dkv/internal/faults"
	"github.com/tarcisiozf/dkv/internal/flows"
	"github.com/tarcisiozf/dkv/slots"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type DbEngine struct {
	zookeeper          *zk.Conn
	config             conf.Config
	serviceDiscovery   *svcdiscovery.ServiceDiscovery
	nodes              *nodes.Set
	nodeId             id.ID
	rpcServerListener  net.Listener
	writeMutex         *sync.Mutex
	rpcClients         *registries.RpcClientManager
	storageManager     *storage.Manager
	nodeSyncer         *cluster.NodeSyncer
	nodeRegistry       *nodereg.NodeRegistry
	clusterCoordinator *cluster.Coordinator
	wal                *wal.WriteAheadLog
	kv                 *kv.RoseDbKeyValueStore
}

func NewDbEngine(options ...ConfigOption) (db *DbEngine, err error) {
	config := defaultConfig
	for _, option := range options {
		config, err = option(config)
		if err != nil {
			return nil, fmt.Errorf("failed to apply config option: %w", err)
		}
	}

	config, err = validateConfig(config)
	if err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	nodeId, err := nodereg.GetOrCreateNodeID(config.NodeId, config.DirPath+"/NODE_ID")
	if err != nil {
		return nil, fmt.Errorf("failed to get or create node NodeID: %w", err)
	}

	db = &DbEngine{
		config:     config,
		nodeId:     nodeId,
		writeMutex: &sync.Mutex{},
		rpcClients: registries.NewRpcClientManager(),
		nodes:      nodes.NewSet(),
	}
	return db, nil
}

func (db *DbEngine) Start(ctx context.Context) error {
	return flows.Pipeline(
		db.setupZookeeper,
		db.setupWal,
		db.setupKvStore,
		db.setupRpcServer,
		db.setupCoreComponents,
		db.registerAndSyncNodes(ctx),
		db.setupCoordinator,
		db.setupWatchers(ctx),
		db.maybeRebalanceCluster,
	)
}

func (db *DbEngine) setupZookeeper() error {
	conn, _, err := zk.Connect(db.config.Zookeeper, 10*time.Second) // TODO: config session timeout
	if err != nil {
		return fmt.Errorf("failed to connect to zookeeper: %w", err)
	}
	db.zookeeper = conn
	return nil
}

func (db *DbEngine) setupKvStore() (err error) {
	path := db.config.DirPath + "/kv"
	kvs, err := kv.NewRoseDbKeyValueStore(path)
	if err != nil {
		return fmt.Errorf("failed to create kv store: %w", err)
	}
	db.kv = kvs
	return nil
}

func (db *DbEngine) setupWal() error {
	writeAheadLog, err := wal.NewWriteAheadLog(db.config.FileSystem, db.config.DirPath)
	if err != nil {
		return fmt.Errorf("failed to open wal: %w", err)
	}
	db.wal = writeAheadLog
	return nil
}

func (db *DbEngine) setupRpcServer() error {
	nodeRpc := NewRpcServer(db)

	r := rpc.NewServer()
	if err := r.Register(nodeRpc); err != nil {
		return fmt.Errorf("failed to register RPC server: %w", err)
	}

	rpcListener, err := net.Listen("tcp", ":"+db.config.RpcPort)
	if err != nil {
		return fmt.Errorf("failed to listen on RPC port %s: %w", db.config.RpcPort, err)
	}

	go func() {
		r.Accept(rpcListener)
	}()

	db.rpcServerListener = rpcListener

	return nil
}

func (db *DbEngine) setupCoreComponents() (err error) {
	db.storageManager = storage.NewStorageManager(db.kv, db.wal)
	db.nodeSyncer = cluster.NewNodeSyncer(db.rpcClients, db.storageManager)
	db.serviceDiscovery = svcdiscovery.NewServiceDiscovery(db.zookeeper)
	db.nodeRegistry, err = nodereg.NewNodeRegistry(db.config.ZNodeBasePath, db.serviceDiscovery, db.nodes)
	if err != nil {
		return fmt.Errorf("failed to create node registry: %w", err)
	}
	return nil
}

func (db *DbEngine) registerAndSyncNodes(ctx context.Context) func() error {
	return func() error {
		node := db.newBlankNode()
		if err := db.nodeRegistry.RegisterOrLoad(node); err != nil {
			return fmt.Errorf("failed to register node: %w", err)
		}

		if err := db.initializeNodeState(node); err != nil {
			return fmt.Errorf("failed to initialize node state: %w", err)
		}

		discoveredNodes, err := db.nodeRegistry.Discover()
		if err != nil {
			return fmt.Errorf("failed to discover services: %w", err)
		}

		db.nodeRegistry.RefreshNodesFromZk(ctx, discoveredNodes)
		return nil
	}
}

func (db *DbEngine) initializeNodeState(node *nodes.Node) error {
	var hasUpdates bool

	if node.Status.IsDown() {
		hasUpdates = true
		node.Status = status.Ready
		if node.Mode.IsCluster() {
			node.Status = status.WaitingForOrders
		}
	}
	if node.Mode.IsStandalone() {
		hasUpdates = true
		node.Role = role.Writer
	}

	if hasUpdates {
		if err := db.nodeRegistry.NotifyUpdate(node); err != nil {
			return fmt.Errorf("failed to notify node update: %w", err)
		}
	}

	return nil
}

func (db *DbEngine) setupCoordinator() error {
	node := db.Node()
	db.clusterCoordinator = cluster.NewCoordinator(
		node,
		db.nodes,
		db.config,
		db.rpcClients,
		db.storageManager,
		db.nodeRegistry,
		db.nodeSyncer,
	)
	return nil
}

func (db *DbEngine) setupWatchers(ctx context.Context) func() error {
	return func() error {
		db.nodeRegistry.AddRemovalListener(db.clusterCoordinator.OnNodeRemoved)
		db.nodeRegistry.AddNodeDownListener(db.clusterCoordinator.OnNodeRemoved)
		db.nodeRegistry.AddAddressChangeListener(func(node *nodes.Node) {
			_ = db.rpcClients.Close(node)
		})

		meta, err := db.nodeRegistry.GetMeta(db.nodeId)
		if err != nil {
			return fmt.Errorf("failed to get node meta: %w", err)
		}

		go db.nodeRegistry.WatchDataChanges(ctx, meta.Path, meta.Version)
		go db.nodeRegistry.WatchForNewNodes(ctx)
		return nil
	}
}

func (db *DbEngine) maybeRebalanceCluster() error {
	node := db.Node()
	if node.Status.IsWaitingForOrders() {
		if err := db.clusterCoordinator.RebalanceCluster(); err != nil {
			return fmt.Errorf("failed to coordinate cluster: %w", err)
		}
	}
	return nil
}

func (db *DbEngine) Get(key []byte) ([]byte, error) {
	if !db.Node().Status.IsReady() {
		return nil, fmt.Errorf("node is not ready")
	}

	if !db.config.AllowReadOnWriter && db.Node().Role.IsWriter() {
		return nil, fmt.Errorf("write operation not allowed on writer node")
	}

	slotKey, err := db.getSlotKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get slot key: %w", err)
	}
	value, err := db.storageManager.Get(slotKey)
	if err == nil {
		return value, nil
	}
	if errors.Is(err, kv.ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	}
	return nil, fmt.Errorf("failed to get key: %w", err)
}

func (db *DbEngine) Set(key, value []byte) error {
	if !db.Node().Status.IsReady() {
		return fmt.Errorf("node is not ready")
	}

	if !db.Node().Role.IsWriter() {
		return fmt.Errorf("write operation not allowed on non-writer node")
	}

	slotKey, err := db.getSlotKey(key)
	if err != nil {
		return fmt.Errorf("failed to get slot key: %w", err)
	}

	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()

	if err := db.commit(wal.OpSet, slotKey, value); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	err = db.storageManager.Set(slotKey, value)
	if err == nil {
		return nil
	}
	return fmt.Errorf("failed to set key: %w", err)
}

func (db *DbEngine) Delete(key []byte) error {
	if !db.Node().Status.IsReady() {
		return fmt.Errorf("node is not ready")
	}

	if !db.Node().Role.IsWriter() {
		return fmt.Errorf("write operation not allowed on non-writer node")
	}

	slotKey, err := db.getSlotKey(key)
	if err != nil {
		return fmt.Errorf("failed to get slot key: %w", err)
	}

	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()

	if err := db.commit(wal.OpDelete, slotKey, nil); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	err = db.storageManager.Delete(slotKey)
	if err == nil {
		return nil
	}
	return fmt.Errorf("failed to delete key: %w", err)
}

func (db *DbEngine) Close() error {
	el := faults.ErrList{}

	if db.nodes.Has(db.nodeId) {
		node := db.Node()
		node.Status = status.Down
		if err := db.nodeRegistry.NotifyUpdate(node); err != nil {
			el.Add(fmt.Errorf("failed to notify node update: %w", err))
		}
	}

	if db.zookeeper != nil {
		db.zookeeper.Close()
		db.zookeeper = nil
	}

	if db.rpcServerListener != nil {
		if err := db.rpcServerListener.Close(); err != nil {
			el.Add(fmt.Errorf("closing RPC listener: %v", err))
		}
		db.rpcServerListener = nil
	}

	if db.kv != nil {
		if err := db.kv.Close(); err != nil {
			el.Add(fmt.Errorf("closing KV store: %v", err))
		}
		db.kv = nil
	}

	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			el.Add(fmt.Errorf("closing WAL: %v", err))
		}
		db.wal = nil
	}

	if db.rpcClients != nil {
		if err := db.rpcClients.CloseAll(); err != nil {
			el.Add(fmt.Errorf("closing RPC clients: %v", err))
		}
		db.rpcClients = nil
	}

	return el.Err()
}

func (db *DbEngine) getSlotKey(key []byte) ([]byte, error) {
	slotId := slots.GetSlotId(key)
	nodeId, err := db.nodeForSlot(slotId)
	if err != nil {
		return nil, fmt.Errorf("failed to get node for slot %d: %w", slotId, err)
	}
	if nodeId != db.nodeId {
		return nil, ErrRedirectSlot{nodeId: nodeId} // TODO: handle this error
	}

	const slotHeaderSize = 2
	newKey := make([]byte, len(key)+slotHeaderSize)
	binary.LittleEndian.PutUint16(newKey, slotId)
	copy(newKey[slotHeaderSize:], key)

	return newKey, nil
}

func (db *DbEngine) nodeForSlot(slotId uint16) (id.ID, error) {
	node := db.Node()
	if node.SlotRange.Contains(slotId) {
		return node.ID, nil
	}
	for _, n := range db.nodes.All() {
		if n.ID.Equal(node.ID) {
			continue
		}
		if n.SlotRange.Contains(slotId) {
			return node.ID, nil
		}
	}
	log.Printf("[CRITICAL] No node found for slot %d", slotId)
	return id.Null(), errors.New("no node found for slot")
}

func (db *DbEngine) commit(op wal.Operation, key, data []byte) (err error) {
	entry, err := wal.EncodeEntry(op, key, data)
	if err != nil {
		return fmt.Errorf("failed to encode wal entry: %w", err)
	}

	offset := db.storageManager.WalOffset()
	shouldReplicate := db.config.NodeMode.IsCluster() && db.config.ReplicationFactor > 1

	var tx *disttx.DistributedTransaction
	if shouldReplicate {
		tx = disttx.NewDistributedTransaction(
			db.config,
			db.rpcClients,
		)
		readReplicas := db.nodes.ReadReplicasOf(db.nodeId)

		if err = tx.Prepare(readReplicas, offset, entry); err != nil {
			return fmt.Errorf("failed to prepare replicas: %w", err)
		}
	}

	writeOffset, err := db.storageManager.WriteToWal(entry)
	if err != nil {
		if shouldReplicate {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return ErrRollback{RollbackErr: rollbackErr, OriginErr: err}
			}
		}
		return fmt.Errorf("failed to write to wal: %w", err)
	}
	if writeOffset != offset {
		return fmt.Errorf("wal write offset mismatch: %d != %d", writeOffset, offset)
	}
	if shouldReplicate {
		el := faults.ErrList{}
		if err := tx.Commit(); err != nil {
			el.Add(fmt.Errorf("commit failed: %v", err))
			if err := db.storageManager.SetWalOffset(offset); err != nil {
				el.Add(fmt.Errorf("set WAL offset failed: %v", err))
			}
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				el.Add(ErrRollback{RollbackErr: rollbackErr, OriginErr: err})
			}
			return fmt.Errorf("could not reach quorum: %v", el.Err())
		}
	}

	return nil
}

func (db *DbEngine) newBlankNode() *nodes.Node {
	return &nodes.Node{
		ID:                db.nodeId,
		Mode:              db.config.NodeMode,
		Role:              role.Idle,
		Status:            status.Down,
		Address:           db.config.Address,
		HttpPort:          db.config.HttpPort,
		RpcPort:           db.config.RpcPort,
		ReplicationFactor: db.config.ReplicationFactor,
		WriteQuorum:       db.config.WriteQuorum,
		SlotRange:         slots.FullRange(),
		ReplicaOf:         id.Null(),
	}
}

func (db *DbEngine) DirPath() string {
	return db.config.DirPath
}

func (db *DbEngine) Node() *nodes.Node {
	node := db.nodes.GetById(db.nodeId)
	if node == nil {
		panic("node not found")
	}
	return node
}

func (db *DbEngine) Nodes() *nodes.Set {
	return db.nodes
}

func (db *DbEngine) NodeID() id.ID {
	return db.nodeId
}
