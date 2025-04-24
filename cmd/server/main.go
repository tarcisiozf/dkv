package main

import (
	"context"
	"fmt"
	"github.com/tarcisiozf/dkv/engine"
	"github.com/tarcisiozf/dkv/httpsrv"
	"github.com/tarcisiozf/dkv/internal/env"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	ctx := context.Background()

	httpPort := env.Env("HTTP_PORT", "8090")
	rpcPort := env.Env("RPC_PORT", "9000")
	zookeeperHost := env.Env("ZOOKEEPER_HOST", "localhost")
	zookeeperPort := env.Env("ZOOKEEPER_PORT", "22181")
	nodeMode := env.Env("NODE_MODE", "standalone")
	replicationFactor, err := strconv.Atoi(env.Env("REPLICATION_FACTOR", "1"))
	if err != nil {
		log.Fatalf("[FATAL] Error parsing replication factor: %v", err)
	}
	writeQuorum, err := strconv.Atoi(env.Env("WRITE_QUORUM", "1"))
	if err != nil {
		log.Fatalf("[FATAL] Error parsing write quorum: %v", err)
	}

	db, err := engine.NewDbEngine(
		engine.WithZookeeper(fmt.Sprintf("%s:%s", zookeeperHost, zookeeperPort)),
		engine.WithHttpPort(httpPort),
		engine.WithRpcPort(rpcPort),
		engine.WithMode(nodeMode),
		engine.WithReplicationFactor(replicationFactor),
		engine.WithWriteQuorum(writeQuorum),
	)
	if err != nil {
		log.Fatalf("[FATAL] Error setting up database: %v", err)
	}
	if err = db.Start(ctx); err != nil {
		log.Fatalf("[FATAL] Error starting database: %v", err)
	}

	httpServer := httpsrv.NewHttpServer(db, httpPort)
	httpServer.Start()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM)
	signal.Notify(ch, syscall.SIGINT)

	<-ch
	log.Println("[INFO] Shutting down server...")
	if err := db.Close(); err != nil {
		log.Fatalf("[FATAL] Error shutting down database: %v", err)
	}
}
