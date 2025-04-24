package httpsrv

import (
	"github.com/tarcisiozf/dkv/engine"
	"github.com/tarcisiozf/dkv/httpsrv/internal/router"
	"net/http"
)

type HttpServer struct {
	db   *engine.DbEngine
	port string
}

func (s *HttpServer) Start() {
	mux := http.NewServeMux()
	s.addRoutes(mux)

	go func() {
		_ = http.ListenAndServe(":"+s.port, mux)
	}()
}

func (s *HttpServer) addRoutes(mux *http.ServeMux) {
	r := router.NewRouter(s.db)
	mux.HandleFunc("GET /info", r.HandleInfo)
	mux.HandleFunc("GET /{key}", r.HandleGetKey)
	mux.HandleFunc("POST /{key}", r.HandleSetKeyValue)
	mux.HandleFunc("DELETE /{key}", r.HandleDeleteKey)
}

func NewHttpServer(db *engine.DbEngine, port string) *HttpServer {
	return &HttpServer{
		db:   db,
		port: port,
	}
}
