package status

import "fmt"

var mapStatus = map[string]Status{}

var (
	Ready            = addStatus("ready")
	WaitingForOrders = addStatus("waiting_for_orders")
	Sync             = addStatus("sync")
	Down             = addStatus("down")
)

func addStatus(status string) Status {
	s := Status{status}
	mapStatus[status] = s
	return s
}

type Status struct {
	status string
}

func (s Status) String() string {
	return s.status
}

func (s Status) Equal(other Status) bool {
	return s.status == other.status
}

func (s Status) IsWaitingForOrders() bool {
	return s.status == WaitingForOrders.status
}

func (s Status) IsReady() bool {
	return s.status == Ready.status
}

func (s Status) IsDown() bool {
	return s.status == Down.status
}

func (s Status) IsSyncing() bool {
	return s.status == Sync.status
}

func (s Status) IsNull() bool {
	return s.status == ""
}

func Parse(status string) (Status, error) {
	if s, ok := mapStatus[status]; ok {
		return s, nil
	}
	return Status{}, fmt.Errorf("invalid status: %s", status)
}

func Null() Status {
	return Status{""}
}
