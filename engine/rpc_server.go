package engine

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/internal/cluster"
	"github.com/tarcisiozf/dkv/engine/internal/comm"
	"github.com/tarcisiozf/dkv/engine/nodes/types/id"
	"github.com/tarcisiozf/dkv/engine/nodes/types/role"
	"github.com/tarcisiozf/dkv/engine/nodes/types/status"
	"io"
	"sync"
)

type RpcServer struct {
	db      *DbEngine
	mutex   *sync.RWMutex
	prepare *comm.PrepareArgs
}

func NewRpcServer(db *DbEngine) *RpcServer {
	return &RpcServer{
		db:    db,
		mutex: &sync.RWMutex{},
	}
}

func (rs *RpcServer) Prepare(args *comm.PrepareArgs, reply *comm.EmptyParam) error {
	if args == nil || len(args.Entry) == 0 {
		return fmt.Errorf("invalid arguments: %v", args)
	}

	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	if rs.prepare != nil {
		return fmt.Errorf("already prepared: %v", rs.prepare)
	}

	if args.Offset < rs.db.storageManager.WalOffset() {
		return fmt.Errorf("invalid offset: %d, wal offset: %d", args.Offset, rs.db.storageManager.WalOffset())
	}

	rs.prepare = args
	return nil
}

func (rs *RpcServer) Commit(args *comm.OffsetArgs, reply *comm.EmptyParam) (err error) {
	if args == nil {
		return fmt.Errorf("invalid arguments: %v", args)
	}

	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	if rs.prepare == nil {
		return fmt.Errorf("no prepare found: %v", rs.prepare)
	}

	prepare := rs.prepare
	rs.prepare = nil

	if prepare.Offset > args.Offset {
		return fmt.Errorf("invalid offset: %d, commit offset: %d", prepare.Offset, args.Offset)
	}

	if prepare.Offset != rs.db.storageManager.WalOffset() {
		return fmt.Errorf("invalid offset: %d, wal offset: %d", prepare.Offset, rs.db.storageManager.WalOffset())
	}

	if err := rs.db.storageManager.ApplyWalEntry(prepare.Entry); err != nil {
		return fmt.Errorf("failed to apply entry: %v", err)
	}

	return err
}

func (rs *RpcServer) CurrentOffset(args *comm.EmptyParam, reply *comm.CurrentOffsetReply) error {
	rs.mutex.RLock()
	defer rs.mutex.RUnlock()

	reply.Offset = rs.db.storageManager.WalOffset()
	return nil
}

func (rs *RpcServer) EntriesFromOffset(args *comm.OffsetArgs, reply *comm.EntriesReply) error {
	if args == nil {
		return fmt.Errorf("invalid arguments: %v", args)
	}

	if args.Offset > rs.db.storageManager.WalOffset() {
		return fmt.Errorf("invalid offset: %d, wal offset: %d", args.Offset, rs.db.storageManager.WalOffset())
	}

	reader, err := rs.db.storageManager.WalReader(args.Offset)
	if err != nil {
		return fmt.Errorf("failed to create WAL reader: %v", err)
	}

	const limitPerPage = 10 // TODO: configurable

	offset := args.Offset
	entries := make([][]byte, 0)

	count := 0
	for entry, err := range reader {
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read WAL entry: %v", err)
		}
		offset = entry.Offset
		entries = append(entries, entry.Data)
		count++
		if count >= limitPerPage {
			break
		}
	}

	*reply = comm.EntriesReply{
		Entries: entries,
		Offset:  offset,
	}

	return nil
}

func (rs *RpcServer) UpdateParams(args *comm.UpdateParamsArgs, reply *comm.EmptyParam) (err error) {
	if args == nil {
		return fmt.Errorf("invalid arguments: %v", args)
	}

	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	update := &cluster.UpdateParamsRequest{}
	if args.Status != "" {
		update.Status, err = status.Parse(args.Status)
		if err != nil {
			return fmt.Errorf("failed to parse status: %v", err)
		}
	}
	if args.Role != "" {
		update.Role, err = role.Parse(args.Role)
		if err != nil {
			return fmt.Errorf("failed to parse role: %v", err)
		}
	}
	if args.ReplicaOf != "" {
		update.ReplicaOf, err = id.Parse(args.ReplicaOf)
		if err != nil {
			return fmt.Errorf("failed to parse replica of: %v", err)
		}
	}
	update.SlotRange = args.SlotRange
	update.NewSlotRange = args.NewSlotRange

	err = rs.db.clusterCoordinator.UpdateOwnParams(update)
	if err != nil {
		return fmt.Errorf("failed to update node params: %v", err)
	}

	return nil
}

func (rs *RpcServer) Status(args *comm.EmptyParam, reply *comm.NodeStatusReply) error {
	reply.Status = rs.db.Node().Status.String()
	return nil
}

func (rs *RpcServer) EntriesFromSlot(args *comm.EntriesFromSlotArgs, reply *comm.EntriesFromSlotReply) error {
	if args == nil || args.SlotRange.IsEmpty() || !args.SlotRange.IsValid() {
		return fmt.Errorf("invalid arguments: %v", args)
	}
	if !rs.db.Node().SlotRange.Intersects(args.SlotRange) {
		return fmt.Errorf("slot range %s does not intersect with node's slot range %s", args.SlotRange, rs.db.Node().SlotRange)
	}

	entries, err := rs.db.storageManager.EntriesForSlot(args.SlotRange)
	if err != nil {
		return fmt.Errorf("failed to get entries for slot range %s: %v", args.SlotRange, err)
	}

	*reply = comm.EntriesFromSlotReply{
		Entries: entries,
	}
	return nil
}

func (rs *RpcServer) Rollback(args *comm.OffsetArgs, reply *comm.EmptyParam) error {
	if args == nil {
		return fmt.Errorf("invalid arguments: %v", args)
	}

	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	if rs.prepare == nil {
		return nil
	}
	if rs.prepare.Offset != args.Offset {
		return fmt.Errorf("invalid offset: %d, prepare offset: %d", args.Offset, rs.prepare.Offset)
	}
	rs.prepare = nil

	if rs.db.storageManager.WalOffset() > args.Offset {
		if err := rs.db.storageManager.SetWalOffset(args.Offset); err != nil {
			return fmt.Errorf("failed to set wal offset: %v", err)
		}
	}

	return nil
}
