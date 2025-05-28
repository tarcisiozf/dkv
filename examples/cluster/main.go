package main

import (
	"context"
	"github.com/tarcisiozf/dkv/engine"
	"github.com/tarcisiozf/dkv/engine/test"
	"github.com/tarcisiozf/dkv/httpsrv"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	numWriters := 1
	numReadReplicas := 1
	numInstances := numWriters + numReadReplicas

	ctx, cancel := context.WithCancel(context.Background())
	env := test.NewTestEnv(
		test.WithAutoStart(),
	)
	defer env.Destroy(ctx)

	options := []engine.ConfigOption{
		engine.WithReplicationFactor(numInstances),
		engine.WithWriteQuorum(numWriters + 1),
		engine.WithMode("cluster"),
	}

	log.Printf("creating %d instances...", numInstances)
	instances, err := env.CreateInstancesInParallel(ctx, numInstances, options...)
	if err != nil {
		log.Fatalf("failed to create instances: %v", err)
	}

	log.Printf("checking cluster status...")

	if err := env.WaitForAllInstancesToBeReady(); err != nil {
		log.Fatalf("failed to wait for all instances to be ready: %v", err)
	}

	log.Println("starting HTTP servers...")
	for _, instance := range instances {
		node := instance.Node()
		srv := httpsrv.NewHttpServer(instance, node.HttpPort)
		srv.Start()
	}

	log.Printf("cluster is ready")
	log.Println("instances:")
	for _, instance := range instances {
		node := instance.Node()
		log.Printf("  - %s:%s (ID: %s)", node.Address, node.HttpPort, node.ID)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	<-sigs
	cancel()
}
