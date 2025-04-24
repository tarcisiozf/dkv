package main

import (
	"github.com/tarcisiozf/dkv/cmd/server/internal"
	"github.com/tarcisiozf/dkv/engine"
	"log"
	"net/http"
)

func main() {
	db, err := engine.NewDbEngine()
	if err != nil {
		log.Fatalf("Error starting database: %v", err)
	}
	publicRouter := internal.NewPublicRouter(db)

	http.HandleFunc("/{key}", publicRouter.Handle)
	err = http.ListenAndServe(":8090", nil)
	if err != nil {
		log.Fatalf("Error starting HTTP server: %v", err)
	}
}
