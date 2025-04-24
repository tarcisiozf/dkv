package id

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

type ID struct {
	id string
}

func Generate() (id ID, err error) {
	var buf [8]byte
	if _, err = rand.Read(buf[:]); err != nil {
		return id, fmt.Errorf("generating random node ID: %w", err)
	}
	return ID{id: hex.EncodeToString(buf[:])}, nil
}

func Parse(id string) (ID, error) {
	if len(id) != 16 { // 8 bytes in hex
		return ID{}, fmt.Errorf("invalid ID length: %d", len(id))
	}
	return ID{id: id}, nil
}

func Null() ID {
	return ID{id: ""}
}

func (id ID) String() string {
	return id.id
}

func (id ID) Equal(other ID) bool {
	return id.id == other.id
}

func (id ID) IsNull() bool {
	return id.id == ""
}
