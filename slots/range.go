package slots

import "fmt"

type SlotRange [2]uint16

func FullRange() SlotRange {
	return SlotRange{0, NumSlots}
}

func (r SlotRange) Contains(slot uint16) bool {
	return slot >= r[0] && slot <= r[1]
}

func (r SlotRange) Empty() SlotRange {
	return SlotRange{0, 0}
}

func (r SlotRange) IsEmpty() bool {
	return r[0] == 0 && r[1] == 0
}

func (r SlotRange) Partition(n int) ([]SlotRange, error) {
	if n <= 0 {
		return nil, fmt.Errorf("number of partitions must be greater than 0")
	}
	start, end := r[0], r[1]
	slots := end - start + 1
	if uint16(n) > slots {
		return nil, fmt.Errorf("cannot partition into more than %d slots", slots)
	}
	rangeSize := slots / uint16(n)
	ranges := make([]SlotRange, 0, n)
	for i := 0; i < n; i++ {
		newStart := start + uint16(i)*rangeSize
		newEnd := newStart + rangeSize - 1
		if newEnd > end || i == n-1 {
			newEnd = end
		}
		ranges = append(ranges, SlotRange{newStart, newEnd})
	}
	return ranges, nil
}

func (r SlotRange) String() string {
	return fmt.Sprintf("[%d, %d]", r[0], r[1])
}

func (r SlotRange) Start() uint16 {
	return r[0]
}

func (r SlotRange) End() uint16 {
	return r[1]
}

func (r SlotRange) Equal(other SlotRange) bool {
	return r[0] == other[0] && r[1] == other[1]
}

func (r SlotRange) Intersects(slotRange SlotRange) bool {
	return slotRange[0] >= r[0] && slotRange[0] <= r[1] ||
		slotRange[1] >= r[0] && slotRange[1] <= r[1]
}

func (r SlotRange) IsValid() bool {
	return r[0] <= r[1] && r[1] <= NumSlots
}
