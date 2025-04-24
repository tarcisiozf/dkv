package slots

import "testing"

func TestSlotRange_Partition(t *testing.T) {
	t.Run("from full range", func(t *testing.T) {
		r := FullRange()
		partitions, err := r.Partition(3)
		if err != nil {
			t.Fatalf("failed to partition slot range: %v", err)
		}
		if len(partitions) != 3 {
			t.Fatalf("expected 3 partitions, got %d", len(partitions))
		}
		if partitions[0][0] != 0 || partitions[0][1] != 340 {
			t.Fatalf("expected first partition to be [0, 340], got [%d, %d]", partitions[0][0], partitions[0][1])
		}
		if partitions[1][0] != 341 || partitions[1][1] != 681 {
			t.Fatalf("expected second partition to be [341, 681], got [%d, %d]", partitions[1][0], partitions[1][1])
		}
		if partitions[2][0] != 682 || partitions[2][1] != 1024 {
			t.Fatalf("expected third partition to be [682, 1024], got [%d, %d]", partitions[2][0], partitions[2][1])
		}
	})

	t.Run("single partition", func(t *testing.T) {
		r := FullRange()
		partitions, err := r.Partition(1)
		if err != nil {
			t.Fatalf("failed to partition slot range: %v", err)
		}
		if len(partitions) != 1 {
			t.Fatalf("expected 1 partition, got %d", len(partitions))
		}
		if partitions[0][0] != 0 || partitions[0][1] != 1024 {
			t.Fatalf("expected partition to be [0, 1024], got [%d, %d]", partitions[0][0], partitions[0][1])
		}
	})
}
