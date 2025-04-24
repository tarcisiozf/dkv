package mode

import "fmt"

var mapMode = map[string]Mode{}

var (
	Cluster    = addMode("cluster")
	Standalone = addMode("standalone")
)

func addMode(mode string) Mode {
	m := Mode{mode}
	mapMode[mode] = m
	return m
}

type Mode struct {
	mode string
}

func (m Mode) String() string {
	return m.mode
}

func (m Mode) Equal(other Mode) bool {
	return m.mode == other.mode
}

func (m Mode) IsCluster() bool {
	return m.mode == Cluster.mode
}

func (m Mode) IsStandalone() bool {
	return m.mode == Standalone.mode
}

func Parse(m string) (Mode, error) {
	if mode, ok := mapMode[m]; ok {
		return mode, nil
	}
	return Mode{}, fmt.Errorf("invalid mode %q", m)
}
