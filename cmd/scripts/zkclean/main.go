package main

import (
	"errors"
	"github.com/go-zookeeper/zk"
	"log"
	"time"
)

var paths []string
var versions map[string]int32

func collectZookeeperPaths(conn *zk.Conn, parent string) {
	_, stat, err := conn.Get(parent)
	if err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			return
		}
		panic(err)
	}

	paths = append(paths, parent)
	versions[parent] = stat.Version

	children, _, err := conn.Children(parent)
	if err != nil {
		panic(err)
	}

	for _, child := range children {
		collectZookeeperPaths(conn, parent+"/"+child)
	}
}

func main() {
	addr := "localhost:22181"
	conn, _, err := zk.Connect([]string{addr}, 10*time.Second)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	collectZookeeperPaths(conn, "/dkv")

	for i := len(paths) - 1; i >= 0; i-- {
		path := paths[i]
		version := versions[path]
		log.Printf("Deleting Zookeeper path %s with version %d\n", path, version)
		if err = conn.Delete(path, version); err != nil {
			panic(err)
		}
	}
}
