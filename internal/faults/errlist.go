package faults

import "strings"

type ErrList []error

func (el *ErrList) Add(err error) {
	if err == nil {
		return
	}
	*el = append(*el, err)
}

func (el *ErrList) Err() error {
	if len(*el) == 0 {
		return nil
	}
	return el
}

func (el *ErrList) Error() string {
	if len(*el) == 0 {
		panic("no errors")
	}
	if len(*el) == 1 {
		return (*el)[0].Error()
	}
	messages := make([]string, len(*el))
	for i, err := range *el {
		messages[i] = "\t* " + err.Error()
	}
	return "multiple errors occurred:\n" + strings.Join(messages, "\n")
}
