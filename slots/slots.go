package slots

import "hash/crc32"

const NumSlots = 1024

func GetSlotId(data []byte) uint16 {
	hash := crc32.ChecksumIEEE(data)
	return uint16(int(hash) % NumSlots)
}
