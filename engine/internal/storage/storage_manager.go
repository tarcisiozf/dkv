package storage

import (
	"fmt"
	"github.com/tarcisiozf/dkv/engine/db"
	"github.com/tarcisiozf/dkv/engine/internal/kv"
	"github.com/tarcisiozf/dkv/engine/internal/wal"
	"github.com/tarcisiozf/dkv/slots"
	"iter"
)

type Manager struct {
	kv  kv.KeyValueStore
	wal *wal.WriteAheadLog
}

func NewStorageManager(kv kv.KeyValueStore, wal *wal.WriteAheadLog) *Manager {
	return &Manager{
		kv:  kv,
		wal: wal,
	}
}

func (s *Manager) Get(slotKey []byte) ([]byte, error) {
	return s.kv.Get(slotKey)
}

func (s *Manager) Set(slotKey, value []byte) error {
	return s.kv.Set(slotKey, value)
}

func (s *Manager) Delete(slotKey []byte) error {
	return s.kv.Delete(slotKey)
}

func (s *Manager) ApplyWalEntry(entry []byte) error {
	op, key, value, err := wal.DecodeEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to decode wal entry: %w", err)
	}

	_, err = s.wal.Write(entry)
	if err != nil {
		return fmt.Errorf("failed to write wal entry: %w", err)
	}

	if op == wal.OpSet {
		return s.kv.Set(key, value)
	}
	if op == wal.OpDelete {
		return s.kv.Delete(key)
	}
	return fmt.Errorf("invalid operation in WAL entry: %d", op)
}

func (s *Manager) ApplyDbEntry(entry db.Entry) error {
	slotKey := entry.SlotKey()
	walEntry, err := wal.EncodeEntry(wal.OpSet, slotKey, entry.Value)
	if err != nil {
		return fmt.Errorf("failed to encode wal entry: %w", err)
	}

	if _, err := s.wal.Write(walEntry); err != nil {
		return fmt.Errorf("failed to write wal entry: %w", err)
	}

	if err := s.kv.Set(slotKey, entry.Value); err != nil {
		return fmt.Errorf("failed to put entry in kv store: %w", err)
	}
	return nil
}

func (s *Manager) WalOffset() uint64 {
	return s.wal.Offset()
}

func (s *Manager) WriteToWal(entry []byte) (uint64, error) {
	return s.wal.Write(entry)
}

func (s *Manager) EntriesForSlot(slotRange slots.SlotRange) ([]db.Entry, error) {
	var entries []db.Entry
	for entry, err := range s.kv.Iterator() {
		if err != nil {
			return nil, fmt.Errorf("failed to iterate over entries: %w", err)
		}
		if slotRange.Contains(entry.SlotId) {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

func (s *Manager) WalReader(offset uint64) (iter.Seq2[wal.Entry, error], error) {
	return s.wal.Reader(offset)
}

func (s *Manager) SetWalOffset(offset uint64) error {
	return s.wal.SetOffset(offset)
}
