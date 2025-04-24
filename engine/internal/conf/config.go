package conf

import (
	"github.com/tarcisiozf/dkv/engine/internal/filesys"
	"github.com/tarcisiozf/dkv/engine/nodes/types/id"
	"github.com/tarcisiozf/dkv/engine/nodes/types/mode"
	"time"
)

type Config struct {
	NodeId             id.ID
	Zookeeper          []string
	DirPath            string
	Address            string
	HttpPort           string
	RpcPort            string
	ReplicationFactor  int
	WriteQuorum        int
	NodeMode           mode.Mode
	AllowReadOnWriter  bool
	ZNodeBasePath      string
	TransactionTimeout time.Duration
	FileSystem         filesys.FileSystem
}
