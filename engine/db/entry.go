package db

import "encoding/binary"

const slotHeaderSize = 2 // 2 bytes = uint16

type Entry struct {
	Key    []byte
	Value  []byte
	SlotId uint16
}

func (e Entry) SlotKey() []byte {
	key := make([]byte, len(e.Key)+slotHeaderSize)
	binary.LittleEndian.PutUint16(key, e.SlotId)
	copy(key[slotHeaderSize:], e.Key)
	return key
}

func NewEntryFromSlotKey(key, value []byte) Entry {
	slotId := binary.LittleEndian.Uint16(key[:slotHeaderSize])
	return Entry{
		Key:    key[slotHeaderSize:],
		Value:  value,
		SlotId: slotId,
	}
}
