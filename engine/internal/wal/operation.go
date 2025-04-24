package wal

type Operation = byte

const (
	OpSet    Operation = 0x01
	OpDelete Operation = 0x02
)
