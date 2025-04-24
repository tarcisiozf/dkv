package engine

import (
	"github.com/tarcisiozf/dkv/engine/internal/kv"
	"github.com/tarcisiozf/dkv/engine/nodes/types/id"
)

var (
	ErrKeyNotFound = kv.ErrKeyNotFound
)

type ErrRedirectSlot struct {
	nodeId id.ID
}

func (e ErrRedirectSlot) Error() string {
	return "redirect to node " + e.nodeId.String()
}

type ErrRollback struct {
	RollbackErr, OriginErr error
}

func (e ErrRollback) Error() string {
	return "rollback error: " + e.RollbackErr.Error() + ", origin: " + e.OriginErr.Error()
}
