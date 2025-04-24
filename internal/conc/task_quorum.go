package conc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type Task func() error

type TaskQuorum struct {
	quorum    uint32
	tasks     uint32
	runs      uint32
	success   uint32
	pending   chan Task
	waitGroup *sync.WaitGroup
	open      bool
	mutex     sync.Mutex
}

func NewTaskQuorum(quorum, concurrency int) (*TaskQuorum, error) {
	if quorum < 0 {
		return nil, fmt.Errorf("quorum must be greater or equal to 0")
	}
	if concurrency < 1 {
		concurrency = 1
	}

	tq := &TaskQuorum{
		quorum:    uint32(quorum),
		pending:   make(chan Task, 1),
		waitGroup: &sync.WaitGroup{},
		open:      true,
		mutex:     sync.Mutex{},
	}

	for i := 0; i < concurrency; i++ {
		go tq.consume()
	}

	return tq, nil
}

func (tq *TaskQuorum) Go(fn Task) {
	if !tq.open {
		panic("TaskQuorum is closed")
	}

	tq.tasks++
	tq.waitGroup.Add(1)
	tq.pending <- fn
}

func (tq *TaskQuorum) WaitQuorum(ctx context.Context) error {
	tq.ensureClosed()

	if tq.tasks < tq.quorum {
		return fmt.Errorf("not enough tasks: %d/%d", tq.tasks, tq.quorum)
	}

	var abortErr error
	go func() {
		select {
		case <-ctx.Done():
			abortErr = ctx.Err()
		}
	}()

	for atomic.LoadUint32(&tq.runs) < tq.tasks {
		if abortErr != nil {
			return abortErr
		}
		if atomic.LoadUint32(&tq.success) >= tq.quorum {
			return nil
		}
	}

	if tq.success < tq.quorum {
		return fmt.Errorf("not enough successful tasks: %d/%d", tq.success, tq.quorum)
	}

	return nil
}

func (tq *TaskQuorum) WaitAll() {
	tq.ensureClosed()
	tq.waitGroup.Wait()
}

func (tq *TaskQuorum) consume() {
	for task := range tq.pending {
		if err := task(); err == nil {
			atomic.AddUint32(&tq.success, 1)
		}

		atomic.AddUint32(&tq.runs, 1)
		tq.waitGroup.Done()
	}
}

func (tq *TaskQuorum) ensureClosed() {
	if !tq.open {
		return
	}
	tq.mutex.Lock()
	defer tq.mutex.Unlock()
	if tq.open {
		close(tq.pending)
		tq.open = false
	}
}
