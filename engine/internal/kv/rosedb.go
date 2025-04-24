package kv

import (
	"errors"
	"fmt"
	"github.com/rosedblabs/rosedb/v2"
	"github.com/tarcisiozf/dkv/engine/db"
	"iter"
)

type RoseDbKeyValueStore struct {
	db *rosedb.DB
}

func NewRoseDbKeyValueStore(path string) (*RoseDbKeyValueStore, error) {
	options := rosedb.DefaultOptions
	options.DirPath = path
	db, err := rosedb.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to open rosedb: %w", err)
	}

	return &RoseDbKeyValueStore{
		db: db,
	}, nil
}

func (r *RoseDbKeyValueStore) Set(key, value []byte) error {
	return r.db.Put(key, value)
}

func (r *RoseDbKeyValueStore) Get(key []byte) ([]byte, error) {
	value, err := r.db.Get(key)
	if errors.Is(err, rosedb.ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get value from rosedb: %w", err)
	}
	return value, nil
}

func (r *RoseDbKeyValueStore) Delete(key []byte) error {
	return r.db.Delete(key)
}

func (r *RoseDbKeyValueStore) Iterator() iter.Seq2[db.Entry, error] {
	return func(yield func(db.Entry, error) bool) {
		opts := rosedb.DefaultIteratorOptions
		opts.ContinueOnError = true
		it := r.db.NewIterator(opts)

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			entry := db.NewEntryFromSlotKey(item.Key, item.Value)
			if !yield(entry, it.Err()) {
				break
			}
		}
		if err := it.Err(); err != nil {
			yield(db.Entry{}, err)
		}
		it.Close()
	}
}

func (r *RoseDbKeyValueStore) Close() error {
	if err := r.db.Close(); err != nil {
		return fmt.Errorf("failed to close rosedb: %w", err)
	}
	return nil
}
