package disttx

import (
	"context"
	"fmt"
	"github.com/tarcisiozf/dkv/engine/internal/conf"
	"github.com/tarcisiozf/dkv/engine/internal/registries"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"github.com/tarcisiozf/dkv/internal/conc"
	"golang.org/x/sync/errgroup"
	"log"
	"sync"
)

type DistributedTransaction struct {
	config     conf.Config
	rpcClients *registries.RpcClientManager
	offset     uint64
	mutex      *sync.RWMutex
	prepared   []*nodes.Node
	commited   map[string]bool
	taskQuorum *conc.TaskQuorum
}

func NewDistributedTransaction(
	config conf.Config,
	rpcClients *registries.RpcClientManager,
) *DistributedTransaction {
	return &DistributedTransaction{
		config:     config,
		rpcClients: rpcClients,
		mutex:      &sync.RWMutex{},
		prepared:   make([]*nodes.Node, 0),
		commited:   make(map[string]bool),
	}
}

func (tx *DistributedTransaction) Prepare(readReplicas []*nodes.Node, offset uint64, entry []byte) error {
	quorum := tx.config.WriteQuorum - 1
	numTasks := tx.config.ReplicationFactor - 1

	if len(readReplicas) < quorum {
		return fmt.Errorf("not enough replicas to reach quorum: %d < %d", len(readReplicas), quorum)
	}
	if len(readReplicas) > numTasks {
		return fmt.Errorf("too many replicas to reach quorum: %d > %d", len(readReplicas), numTasks)
	}

	tx.offset = offset

	taskQuorum, err := conc.NewTaskQuorum(quorum, quorum)
	if err != nil {
		return fmt.Errorf("failed to create task quorum: %w", err)
	}

	for _, node := range readReplicas {
		taskQuorum.Go(func() error {
			rpcClient, err := tx.rpcClients.GetClient(node)
			if err != nil {
				log.Printf("[ERROR] Failed to get RPC client for node %s: %v", node.ID, err)
				return err
			}
			if err := rpcClient.Prepare(offset, entry); err != nil {
				log.Printf("[ERROR] Failed to call Prepare on node %s: %v", node.ID, err)
				return err
			}

			tx.mutex.Lock()
			defer tx.mutex.Unlock()

			tx.prepared = append(tx.prepared, node)
			return nil
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), tx.config.TransactionTimeout)
	defer cancel()

	tx.taskQuorum = taskQuorum
	if err := taskQuorum.WaitQuorum(ctx); err != nil {
		return fmt.Errorf("failed to reach quorum: %w", err)
	}

	return nil
}

func (tx *DistributedTransaction) Commit() error {
	return tx.commit(false)
}

func (tx *DistributedTransaction) commit(isRetry bool) error {
	eg := errgroup.Group{}
	for _, node := range tx.prepared {
		tx.mutex.RLock()
		nodeHasCommitted := tx.commited[node.ID.String()]
		tx.mutex.RUnlock()

		if nodeHasCommitted {
			continue
		}

		eg.Go(func() error {
			rpcClient, err := tx.rpcClients.GetClient(node)
			if err != nil {
				return fmt.Errorf("failed to get RPC client for node %s: %w", node.ID, err)
			}
			if err := rpcClient.Commit(tx.offset); err != nil {
				return fmt.Errorf("failed to call Commit on node %s: %w", node.ID, err)
			}

			tx.mutex.Lock()
			defer tx.mutex.Unlock()

			tx.commited[node.ID.String()] = true
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to commit replicas: %w", err)
	}

	go func() {
		if isRetry {
			return
		}
		tx.taskQuorum.WaitAll()
		if err := tx.commit(true); err != nil {
			log.Printf("[ERROR] Failed to commit: %v", err)
			return
		}
	}()

	return nil
}

func (tx *DistributedTransaction) Rollback() error {
	return tx.rollback(false)
}

func (tx *DistributedTransaction) rollback(isRetry bool) error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	eg := errgroup.Group{}
	for _, node := range tx.prepared {
		eg.Go(func() error {
			rpcClient, err := tx.rpcClients.GetClient(node)
			if err != nil {
				return fmt.Errorf("failed to get RPC client for node %s: %w", node.ID, err)
			}
			if err := rpcClient.Rollback(tx.offset); err != nil {
				return fmt.Errorf("failed to call Rollback on node %s: %w", node.ID, err)
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to rollback replicas: %w", err)
	}

	go func() {
		if isRetry {
			return
		}
		tx.taskQuorum.WaitAll()
		if err := tx.rollback(true); err != nil {
			log.Printf("[ERROR] Failed to rollback: %v", err)
			return
		}
	}()

	return nil
}
