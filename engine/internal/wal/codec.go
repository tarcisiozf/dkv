package wal

import (
	"fmt"
	"math"
)

func EncodeEntry(op Operation, key []byte, data []byte) ([]byte, error) {
	if op == 0 {
		return nil, fmt.Errorf("invalid operation")
	}
	lenKey := len(key)
	if lenKey == 0 {
		return nil, fmt.Errorf("invalid empty key")
	}
	if lenKey > 255 {
		return nil, fmt.Errorf("key too long")
	}
	lenData := len(data)
	if lenData > math.MaxUint16 {
		return nil, fmt.Errorf("data too long")
	}

	it := NewBinaryIterator().
		PutByte(op).
		PutByte(byte(lenKey)).
		PutBytes(key)
	if lenData > 0 {
		it.PutUint16(uint16(lenData)).
			PutBytes(data)
	}
	return it.Data(), nil
}

func DecodeEntry(entry []byte) (Operation, []byte, []byte, error) {
	it := NewBinaryIteratorFrom(entry)
	op := it.Byte()
	lenKey := it.Byte()
	if lenKey == 0 {
		return 0, nil, nil, fmt.Errorf("invalid empty key in WAL entry")
	}
	key := it.Bytes(int(lenKey))
	lenData := it.Uint16()
	if lenData == 0 {
		return 0, nil, nil, fmt.Errorf("invalid empty data in WAL entry")
	}
	data := it.Bytes(int(lenData))
	if len(data) != int(lenData) {
		return 0, nil, nil, fmt.Errorf("invalid data length in WAL entry: %d/%d", len(data), lenData)
	}
	return op, key, data, nil
}
