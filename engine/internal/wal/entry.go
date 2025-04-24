package wal

type Entry struct {
	Offset uint64
	Data   []byte
}
