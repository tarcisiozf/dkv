package flows

import "fmt"

func Pipeline(pipeline ...func() error) error {
	for _, fn := range pipeline {
		if err := fn(); err != nil {
			return fmt.Errorf("failed to execute function: %w", err)
		}
	}
	return nil
}
