package wal

import "encoding/binary"

type BinaryIterator struct {
	offset int
	data   []byte
}

func NewBinaryIterator() *BinaryIterator {
	return &BinaryIterator{
		data: make([]byte, 0),
	}
}

func NewBinaryIteratorFrom(data []byte) *BinaryIterator {
	return &BinaryIterator{
		data: data,
	}
}

func (it *BinaryIterator) PutByte(b byte) *BinaryIterator {
	it.put(b)
	return it
}

func (it *BinaryIterator) PutBytes(b []byte) *BinaryIterator {
	it.put(b...)
	return it
}

func (it *BinaryIterator) Size() int {
	return len(it.data)
}

func (it *BinaryIterator) PutUint16(u uint16) *BinaryIterator {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, u)
	it.put(b...)
	return it
}

func (it *BinaryIterator) Data() []byte {
	return it.data
}

func (it *BinaryIterator) Byte() byte {
	b := it.data[it.offset]
	it.offset++
	return b
}

func (it *BinaryIterator) Bytes(n int) []byte {
	b := it.data[it.offset : it.offset+n]
	it.offset += n
	return b
}

func (it *BinaryIterator) Uint16() uint16 {
	b := it.Bytes(2)
	return binary.LittleEndian.Uint16(b)
}

func (it *BinaryIterator) put(b ...byte) {
	it.data = append(it.data, b...)
	it.offset += len(b)
}
