package kv

import (
	"errors"
	"github.com/tarcisiozf/dkv/engine/db"
	"io"
	"iter"
)

var ErrKeyNotFound = errors.New("key not found")

type KeyValueStore interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Iterator() iter.Seq2[db.Entry, error]
	io.Closer
}
