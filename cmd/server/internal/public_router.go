package internal

import (
	"github.com/tarcisiozf/dkv/engine"
	"io"
	"net/http"
)

type PublicRouter struct {
	db *engine.DbEngine
}

func NewPublicRouter(db *engine.DbEngine) *PublicRouter {
	return &PublicRouter{
		db: db,
	}
}

func (r *PublicRouter) Handle(w http.ResponseWriter, rq *http.Request) {
	key := []byte(rq.PathValue("key"))
	if len(key) == 0 {
		http.Error(w, "Invalid key", http.StatusBadRequest)
		return
	}

	if rq.Method == http.MethodPost {
		r.set(w, rq, key)
	} else if rq.Method == http.MethodGet {
		r.get(w, rq, key)
	} else if rq.Method == http.MethodDelete {
		r.delete(w, rq, key)
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (r *PublicRouter) get(w http.ResponseWriter, rq *http.Request, key []byte) {
	value, err := r.db.Get(key)
	if err != nil {
		http.Error(w, "Error getting value", http.StatusInternalServerError)
		return
	}
	if len(value) == 0 {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	w.Write(value)
}

func (r *PublicRouter) set(w http.ResponseWriter, rq *http.Request, key []byte) {
	value, err := io.ReadAll(rq.Body)
	if err != nil {
		http.Error(w, "Error reading body", http.StatusInternalServerError)
		return
	}
	if len(value) == 0 {
		http.Error(w, "Invalid value", http.StatusBadRequest)
		return
	}
	err = r.db.Set(key, value)
	if err != nil {
		http.Error(w, "Error setting value", http.StatusInternalServerError)
		return
	}
}

func (r *PublicRouter) delete(w http.ResponseWriter, rq *http.Request, key []byte) {
	err := r.db.Delete(key)
	if err != nil {
		http.Error(w, "Error deleting key", http.StatusInternalServerError)
		return
	}
}
