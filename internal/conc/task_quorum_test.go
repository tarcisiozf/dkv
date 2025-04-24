package conc_test

import (
	"context"
	"fmt"
	"github.com/tarcisiozf/dkv/internal/conc"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewTaskQuorum(t *testing.T) {
	n := 1000
	quorum := n / 2
	concurrency := n / 10
	ctx := context.Background()

	tq, err := conc.NewTaskQuorum(quorum, concurrency)
	if err != nil {
		t.Fatalf("failed to create TaskQuorum: %v", err)
	}
	if tq == nil {
		t.Fatalf("Expected TaskQuorum to be created, but got nil")
	}

	var invokes uint32

	for i := 0; i < n; i++ {
		tq.Go(func() error {
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			atomic.AddUint32(&invokes, 1)
			return nil
		})
	}

	err = tq.WaitQuorum(ctx)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	tq.WaitAll()
	if int(invokes) != n {
		t.Fatalf("Expected invokes to be equal to %d, but got %d", n, invokes)
	}
}

func TestNewTaskQuorum_SomeFail(t *testing.T) {
	tasks := 1000
	quorum := 10
	concurrency := 10
	ctx := context.Background()

	tq, err := conc.NewTaskQuorum(quorum, concurrency)
	if err != nil {
		t.Fatalf("failed to create TaskQuorum: %v", err)
	}
	if tq == nil {
		t.Fatalf("Expected TaskQuorum to be created, but got nil")
	}

	for i := 0; i < tasks; i++ {
		tq.Go(func() error {
			if i%(tasks/quorum) == 0 {
				return nil
			}
			return fmt.Errorf("error")
		})
	}

	err = tq.WaitQuorum(ctx)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	tq.WaitAll()
}

func TestNewTaskQuorum_Timeout(t *testing.T) {
	n := 10
	quorum := 10
	concurrency := 10
	ctx := context.Background()

	tq, err := conc.NewTaskQuorum(quorum, concurrency)
	if err != nil {
		t.Fatalf("failed to create TaskQuorum: %v", err)
	}
	if tq == nil {
		t.Fatalf("Expected TaskQuorum to be created, but got nil")
	}

	for i := 0; i < n; i++ {
		tq.Go(func() error {
			time.Sleep(10 * time.Second)
			return nil
		})
	}

	timeout, cancel := context.WithTimeout(ctx, time.Second)
	err = tq.WaitQuorum(timeout)
	if err == nil {
		t.Fatalf("Expected timeout error, but got nil")
	}
	if err.Error() != "context deadline exceeded" {
		t.Fatalf("Expected context deadline exceeded error, but got %v", err)
	}
	cancel()
}
