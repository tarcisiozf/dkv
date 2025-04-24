package retry

import (
	"fmt"
	"time"
)

type Option func(r *Retry) *Retry

func WithMaxRetries(maxRetries uint) Option {
	return func(r *Retry) *Retry {
		r.maxRetries = int(maxRetries)
		return r
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(r *Retry) *Retry {
		r.timeout = timeout
		return r
	}
}

func WithInterval(interval time.Duration) Option {
	return func(r *Retry) *Retry {
		r.interval = interval
		return r
	}
}

type Retry struct {
	maxRetries int
	tries      int
	timeout    time.Duration
	interval   time.Duration
}

func NewRetry(options ...Option) *Retry {
	r := &Retry{
		maxRetries: 0,
	}
	for _, opt := range options {
		opt(r)
	}
	if r.interval == 0 {
		r.interval = 100 * time.Millisecond
	}
	return r
}

func (r *Retry) Do(action func() error) error {
	var timeout <-chan time.Time
	if r.timeout > 0 {
		timeout = time.After(r.timeout)
	}
	for {
		if timeout != nil {
			select {
			case <-timeout:
				return fmt.Errorf("timeout reached")
			default:
			}
		}

		err := action()
		if err == nil {
			return nil
		}
		r.tries++

		if r.maxRetries > 0 && r.tries >= r.maxRetries {
			return fmt.Errorf("max retries reached")
		}

		time.Sleep(r.interval)
	}
}
