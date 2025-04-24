package comm

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/db"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"github.com/tarcisiozf/dkv/engine/nodes/types/status"
	"github.com/tarcisiozf/dkv/slots"
	"net/rpc"
)

type PrepareArgs struct {
	Offset uint64
	Entry  []byte
}

type OffsetArgs struct {
	Offset uint64
}

type CurrentOffsetReply struct {
	Offset uint64
}

type EntriesReply struct {
	Entries [][]byte
	Offset  uint64
}

type EmptyParam struct{}

type UpdateParamsArgs struct {
	Status       string
	Role         string
	SlotRange    [2]uint16
	NewSlotRange [2]uint16
	ReplicaOf    string
}

type EntriesFromSlotArgs struct {
	SlotRange slots.SlotRange
}

type EntriesFromSlotReply struct {
	Entries []db.Entry
}

type NodeStatusReply struct {
	Status string
}

type RpcClient struct {
	client *rpc.Client
	node   *nodes.Node
}

func NewRpcClient(node *nodes.Node) *RpcClient {
	return &RpcClient{
		node: node,
	}
}

func (rc *RpcClient) Connect() error {
	if rc.client != nil {
		return nil
	}
	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%s", rc.node.Address, rc.node.RpcPort))
	if err != nil {
		return fmt.Errorf("dialing %s:%s: %w", rc.node.Address, rc.node.RpcPort, err)
	}
	rc.client = client
	return nil
}

func (rc *RpcClient) Close() error {
	if rc.client == nil {
		return nil
	}
	err := rc.client.Close()
	rc.client = nil
	return err
}

func (rc *RpcClient) Prepare(offset uint64, entry []byte) error {
	args := &PrepareArgs{offset, entry}
	reply := &EmptyParam{}
	if err := rc.client.Call("RpcServer.Prepare", args, reply); err != nil {
		return fmt.Errorf("calling Prepare: %w", err)
	}
	return nil
}

func (rc *RpcClient) Commit(offset uint64) error {
	args := &OffsetArgs{offset}
	reply := &EmptyParam{}
	if err := rc.client.Call("RpcServer.Commit", args, reply); err != nil {
		return fmt.Errorf("calling Commit: %w", err)
	}

	return nil
}

func (rc *RpcClient) EntriesFromOffset(offset uint64) ([][]byte, uint64, error) {
	args := &OffsetArgs{offset}
	reply := &EntriesReply{}
	err := rc.client.Call("RpcServer.EntriesFromOffset", args, reply)
	if err != nil {
		return nil, 0, fmt.Errorf("calling EntriesFromOffset: %w", err)
	}
	return reply.Entries, reply.Offset, nil
}

func (rc *RpcClient) CurrentOffset() (uint64, error) {
	args := &EmptyParam{}
	reply := &CurrentOffsetReply{}
	err := rc.client.Call("RpcServer.CurrentOffset", args, reply)
	if err != nil {
		return 0, fmt.Errorf("calling CurrentOffset: %w", err)
	}
	return reply.Offset, nil
}

func (rc *RpcClient) UpdateParams(update *UpdateParamsArgs) error {
	reply := &EmptyParam{}
	err := rc.client.Call("RpcServer.UpdateParams", update, reply)
	if err != nil {
		return fmt.Errorf("calling UpdateParams: %w", err)
	}
	return nil
}

func (rc *RpcClient) Status() (status.Status, error) {
	args := &EmptyParam{}
	reply := &NodeStatusReply{}
	err := rc.client.Call("RpcServer.Status", args, reply)
	if err != nil {
		return status.Null(), fmt.Errorf("calling Status: %w", err)
	}
	return status.Parse(reply.Status)
}

func (rc *RpcClient) EntriesFromSlot(slotRange slots.SlotRange) ([]db.Entry, error) {
	args := &EntriesFromSlotArgs{
		SlotRange: slotRange,
	}
	reply := &EntriesFromSlotReply{}
	err := rc.client.Call("RpcServer.EntriesFromSlot", args, reply)
	if err != nil {
		return nil, fmt.Errorf("calling EntriesFromSlot: %w", err)
	}
	return reply.Entries, nil
}

func (rc *RpcClient) Rollback(offset uint64) error {
	args := &OffsetArgs{offset}
	reply := &EmptyParam{}
	if err := rc.client.Call("RpcServer.Rollback", args, reply); err != nil {
		return fmt.Errorf("calling Rollback: %w", err)
	}
	return nil
}
