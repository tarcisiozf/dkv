package test

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"github.com/tarcisiozf/dkv/engine"
	"github.com/tarcisiozf/dkv/engine/db"
	"github.com/tarcisiozf/dkv/engine/internal/filesys"
	"github.com/tarcisiozf/dkv/engine/nodes/types/id"
	"github.com/tarcisiozf/dkv/internal/env"
	"github.com/tarcisiozf/dkv/internal/faults"
	"github.com/tarcisiozf/dkv/internal/net"
	"github.com/tarcisiozf/dkv/internal/retry"
	"github.com/tarcisiozf/dkv/slots"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

const testDbPath = "./testdb"
const testZkPath = "/dkv/test"

var runtimeSequence int

type Option func(te *Environment) *Environment

func WithAutoStart() Option {
	return func(te *Environment) *Environment {
		te.autoStart = true
		return te
	}
}

type Environment struct {
	instances []*engine.DbEngine
	autoStart bool
	zBasePath string
	zkAddr    string
	mutex     *sync.Mutex
}

func NewTestEnv(options ...Option) *Environment {
	te := &Environment{
		zBasePath: fmt.Sprintf("%s/%d", testZkPath, rand.Int()),
		mutex:     &sync.Mutex{},
	}
	for _, option := range options {
		option(te)
	}
	return te
}

func (te *Environment) CreateInstance(ctx context.Context, options ...engine.ConfigOption) (*engine.DbEngine, error) {
	zookeeperHost := env.Env("ZOOKEEPER_HOST", "localhost")
	zookeeperPort := env.Env("ZOOKEEPER_PORT", "22181")
	te.zkAddr = fmt.Sprintf("%s:%s", zookeeperHost, zookeeperPort)

	nodeId, err := id.Generate()
	if err != nil {
		return nil, fmt.Errorf("failed to generate node ID: %w", err)
	}
	defaultOptions := []engine.ConfigOption{
		engine.WithZookeeper(te.zkAddr),
		engine.WithHttpPort(strconv.Itoa(te.nextPort())),
		engine.WithRpcPort(strconv.Itoa(te.nextPort())),
		engine.WithNodeID(nodeId.String()),
		engine.WithDirPath(testDbPath + "/" + nodeId.String()),
		engine.WithZNodeBasePath(te.zBasePath),
	}

	instance, err := engine.NewDbEngine(append(defaultOptions, options...)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create instance: %w", err)
	}

	te.mutex.Lock()
	te.instances = append(te.instances, instance)
	te.mutex.Unlock()

	if te.autoStart {
		if err := instance.Start(ctx); err != nil {
			return nil, err
		}
	}

	return instance, nil
}

func (te *Environment) CreateInstancesInParallel(ctx context.Context, n int, options ...engine.ConfigOption) ([]*engine.DbEngine, error) {
	var mutex sync.Mutex
	instances := make([]*engine.DbEngine, 0)
	eg := errgroup.Group{}

	for i := 0; i < n; i++ {
		eg.Go(func() error {
			instance, err := te.CreateInstance(ctx, options...)
			if err != nil {
				return err
			}

			mutex.Lock()
			instances = append(instances, instance)
			mutex.Unlock()

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("failed to create instances in parallel: %w", err)
	}

	if len(instances) != n {
		return nil, fmt.Errorf("expected %d instances, got %d", n, len(instances))
	}

	return instances, nil
}

func (te *Environment) Destroy(ctx context.Context) error {
	<-ctx.Done()
	log.Printf("TEST ENV - Shutting down...")

	// shutdown instances
	var wg sync.WaitGroup
	wg.Add(len(te.instances))
	for _, instance := range te.instances {
		go func(n *engine.DbEngine) {
			defer wg.Done()
			if err := n.Close(); err != nil {
				log.Printf("TEST ENV - Error shutting down instance %s: %v", n.NodeID(), err)
			} else {
				log.Printf("TEST ENV - Instance %s shut down successfully", n.NodeID())
			}

			te.removeDbDir(n.DirPath())
		}(instance)
	}
	wg.Wait()

	te.cleanupZookeeper()
	return nil
}

func (te *Environment) cleanupZookeeper() {
	if te.zkAddr == "" {
		return
	}

	conn, _, err := zk.Connect([]string{te.zkAddr}, 10*time.Second)
	if err != nil {
		log.Printf("TEST ENV - Error connecting to Zookeeper: %v", err)
		return
	}
	defer conn.Close()

	te.deleteZookeeperPath(conn, te.zBasePath)

	log.Printf("TEST ENV - Zookeeper base path %s cleaned up", te.zBasePath)
}

func (te *Environment) deleteZookeeperPath(conn *zk.Conn, path string) {
	_, stat, err := conn.Get(path)
	if errors.Is(err, zk.ErrNoNode) {
		return
	}

	children, stat, err := conn.Children(path)
	if err != nil {
		log.Printf("TEST ENV - Error getting children of Zookeeper path %s: %v", path, err)
	}

	for _, child := range children {
		te.deleteZookeeperPath(conn, path+"/"+child)
	}

	if err := conn.Delete(path, stat.Version); err != nil {
		log.Printf("TEST ENV - Error deleting Zookeeper path %s: %v", path, err)
	}
}

func (te *Environment) WaitForAllInstancesToBeReady() error {
	return retry.NewRetry(
		retry.WithTimeout(time.Minute),
		retry.WithInterval(time.Second),
	).
		Do(func() error {
			for _, instance := range te.instances {
				status := instance.Node().Status
				if !status.IsReady() && !status.IsDown() {
					return fmt.Errorf("instance %s is not ready", instance.NodeID())
				}
			}
			return nil
		})
}

func (te *Environment) WaitForQuorumOfInstancesToBeReady(quorum int) error {
	return retry.NewRetry(
		retry.WithTimeout(time.Minute),
		retry.WithInterval(time.Second),
	).
		Do(func() error {
			ready := 0
			for range te.instancesReady() {
				ready++
				if ready == quorum {
					return nil
				}
			}
			return fmt.Errorf("quorum not reached yet")
		})
}

func (te *Environment) FakeKey() []byte {
	return []byte(fmt.Sprintf("key-%d", sequence()))
}

func (te *Environment) FakeValue() []byte {
	return []byte(fmt.Sprintf("value-%d", sequence()))
}

func (te *Environment) FindWriters() []*engine.DbEngine {
	w := make([]*engine.DbEngine, 0)
	for _, instance := range te.instancesReady() {
		if instance.Node().Role.IsWriter() {
			w = append(w, instance)
		}
	}
	return w
}

func (te *Environment) FindAnyWriter() *engine.DbEngine {
	writers := te.FindWriters()
	if len(writers) == 0 {
		return nil
	}
	return writers[rand.Intn(len(writers))]
}

func (te *Environment) FindReadReplicasOf(writer *engine.DbEngine) []*engine.DbEngine {
	instances := make([]*engine.DbEngine, 0)
	for _, instance := range te.instancesReady() {
		if instance.Node().Role.IsReadReplica() && instance.Node().ReplicaOf.Equal(writer.NodeID()) {
			instances = append(instances, instance)
		}
	}
	return instances
}

func (te *Environment) FindWriterForSlot(slot uint16) *engine.DbEngine {
	var writer *engine.DbEngine
	for _, instance := range te.FindInstancesForSlot(slot) {
		if !instance.Node().Role.IsWriter() {
			continue
		}
		if writer != nil {
			panic("multiple writers found for slot")
		}
		writer = instance
	}
	if writer == nil {
		panic("no writer found for slot")
	}
	return writer
}

func (te *Environment) PopulateWriters(n, concurrency int) ([]db.Entry, error) {
	if n < concurrency {
		concurrency = n
	}

	el := faults.ErrList{}
	var wg sync.WaitGroup
	wg.Add(concurrency)
	ch := make(chan db.Entry, n)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for e := range ch {
				writer := te.FindWriterForSlot(e.SlotId)
				if writer == nil {
					el.Add(fmt.Errorf("failed to find writer for slot id: %d", e.SlotId))
					return
				}
				err := writer.Set(e.Key, e.Value)
				if err != nil {
					el.Add(fmt.Errorf("failed to write: %v", err))
					return
				}
			}
		}()
	}

	entries := make([]db.Entry, n)
	for i := 0; i < n; i++ {
		key := te.FakeKey()
		entry := db.Entry{
			Key:    key,
			Value:  te.FakeValue(),
			SlotId: slots.GetSlotId(key),
		}
		entries[i] = entry
		ch <- entry
	}
	close(ch)

	wg.Wait()

	return entries, el.Err()
}

func (te *Environment) FindInstancesForSlot(slot uint16) []*engine.DbEngine {
	instances := make([]*engine.DbEngine, 0)
	for _, instance := range te.instancesReady() {
		if instance.Node().SlotRange.Contains(slot) {
			instances = append(instances, instance)
		}
	}
	return instances
}

func sequence() int {
	s := runtimeSequence
	runtimeSequence++
	return s
}

func (te *Environment) nextPort() int {
	for {
		port := 17000 + sequence()
		if !net.IsPortInUse(port) {
			return port
		}
	}
}

func (te *Environment) instancesReady() []*engine.DbEngine {
	instances := make([]*engine.DbEngine, 0)
	for _, instance := range te.instances {
		if instance.Node().Status.IsReady() {
			instances = append(instances, instance)
		}
	}
	return instances
}

func (te *Environment) EnsureClusterSetup(numWriters, replicationFactor, writeQuorum int) error {
	numInstances := numWriters + (numWriters * (replicationFactor - 1))
	if len(te.instances) != numInstances {
		return fmt.Errorf("expected %d instances, got %d", numInstances, len(te.instances))
	}

	if replicationFactor < writeQuorum {
		return fmt.Errorf("replication factor %d is less than write quorum %d", replicationFactor, writeQuorum)
	}

	instances := make(map[string]*engine.DbEngine)
	writers := make([]*engine.DbEngine, 0)
	readReplicas := make([]*engine.DbEngine, 0)

	for _, instance := range te.instances {
		instances[instance.NodeID().String()] = instance
		if instance.Node().Role.IsWriter() {
			writers = append(writers, instance)
		} else if instance.Node().Role.IsReadReplica() {
			readReplicas = append(readReplicas, instance)
		}
	}

	if len(writers) != numWriters {
		return fmt.Errorf("expected %d writers, got %d", numWriters, len(writers))
	}

	if len(readReplicas) != numInstances-len(writers) {
		return fmt.Errorf("expected %d read replicas, got %d", numInstances-len(writers), len(readReplicas))
	}

	for _, writer := range writers {
		if !writer.Node().ReplicaOf.IsNull() {
			return fmt.Errorf("writer %s should not have a replica", writer.NodeID())
		}
		countReplicas := 0
		for _, replica := range readReplicas {
			if replica.Node().ReplicaOf.Equal(writer.NodeID()) {
				countReplicas++
			}
		}
		if countReplicas != replicationFactor-1 {
			return fmt.Errorf("writer %s should have %d replicas, got %d", writer.NodeID(), replicationFactor-1, countReplicas)
		}
	}

	time.Sleep(10 * time.Second)

	for _, instance := range te.instances {
		for _, node := range instance.Nodes().All() {
			expected := instances[node.ID.String()].Node()
			if expected == nil {
				return fmt.Errorf("node %s not found in instances", node.ID)
			}

			if !expected.Status.Equal(node.Status) {
				return fmt.Errorf("node %s status mismatch: expected %s, got %s", node.ID, expected.Status, node.Status)
			}
			if !expected.Role.Equal(node.Role) {
				return fmt.Errorf("node %s type mismatch: expected %s, got %s", node.ID, expected.Role, node.Role)
			}
			if !expected.ReplicaOf.Equal(node.ReplicaOf) {
				return fmt.Errorf("node %s replica mismatch: expected %s, got %s", node.ID, expected.ReplicaOf, node.ReplicaOf)
			}
			if !expected.SlotRange.Equal(node.SlotRange) {
				return fmt.Errorf("node %s slot range mismatch: expected %s, got %s", node.ID, expected.SlotRange, node.SlotRange)
			}
		}
	}

	return nil
}

func (te *Environment) Instances() []*engine.DbEngine {
	return te.instances
}

func (te *Environment) removeDbDir(dirPath string) {
	log.Printf("TEST ENV - Removing test database directory...")
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		log.Printf("TEST ENV - Directory %s does not exist, skipping removal", dirPath)
		return
	}
	if err := os.RemoveAll(dirPath); err != nil {
		log.Printf("TEST ENV - Error removing directory %s: %v", dirPath, err)
	} else {
		log.Printf("TEST ENV - Directory %s removed successfully", dirPath)
	}
}

func NewInMemoryFileSystem() *filesys.InMemoryFileSystem {
	return filesys.NewInMemoryFileSystem()
}
