package router

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tarcisiozf/dkv/engine"
	"github.com/tarcisiozf/dkv/engine/nodes"
	"github.com/tarcisiozf/dkv/slots"
	"io"
	"log"
	"net/http"
	"strconv"
)

type NodeInfo struct {
	ID        string          `json:"id"`
	Address   string          `json:"address"`
	Port      string          `json:"port"`
	SlotRange slots.SlotRange `json:"slot_range"`
	Nodes     []*NodeInfo     `json:"nodes"`
	Role      string          `json:"role"`
	Status    string          `json:"status"`
	Mode      string          `json:"mode"`
	ReplicaOf string          `json:"replica_of"`
}

type Router struct {
	db *engine.DbEngine
}

func NewRouter(db *engine.DbEngine) *Router {
	return &Router{
		db: db,
	}
}

func (r *Router) HandleGetKey(w http.ResponseWriter, rq *http.Request) {
	key := []byte(rq.PathValue("key"))
	if len(key) == 0 {
		http.Error(w, "Invalid key", http.StatusBadRequest)
		return
	}

	value, err := r.db.Get(key)
	if errors.Is(err, engine.ErrKeyNotFound) || (err == nil && len(value) == 0) {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	if err != nil {
		log.Printf("[ERROR] Error getting value for key %s: %v", key, err)
		http.Error(w, fmt.Sprintf("Error getting value: %v", err), http.StatusInternalServerError)
		return
	}

	if _, err = w.Write(value); err != nil {
		log.Printf("[ERROR] Error writing response: %v", err)
	}
}

func (r *Router) HandleSetKeyValue(w http.ResponseWriter, rq *http.Request) {
	key := []byte(rq.PathValue("key"))
	if len(key) == 0 {
		http.Error(w, "Invalid key", http.StatusBadRequest)
		return
	}

	value, err := io.ReadAll(rq.Body)
	if err != nil {
		http.Error(w, "Error reading body", http.StatusInternalServerError)
		return
	}
	defer rq.Body.Close()

	if len(value) == 0 {
		http.Error(w, "Invalid empty value", http.StatusBadRequest)
		return
	}
	err = r.db.Set(key, value)
	if err != nil {
		log.Printf("[ERROR] Error setting value for key %s: %v", key, err)
		http.Error(w, fmt.Sprintf("Error setting value: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("[INFO] Set key %s with value %s", key, value)
}

func (r *Router) HandleDeleteKey(w http.ResponseWriter, rq *http.Request) {
	key := []byte(rq.PathValue("key"))
	if len(key) == 0 {
		http.Error(w, "Invalid key", http.StatusBadRequest)
		return
	}

	err := r.db.Delete(key)
	if errors.Is(err, engine.ErrKeyNotFound) {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	if err != nil {
		log.Printf("[ERROR] Error deleting key %s: %v", key, err)
		http.Error(w, fmt.Sprintf("Error deleting key: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("[INFO] Deleted key %s", key)
}

func toNodeInfo(node *nodes.Node) *NodeInfo {
	return &NodeInfo{
		ID:        node.ID.String(),
		Address:   node.Address,
		Port:      node.HttpPort,
		SlotRange: node.SlotRange,
		Role:      node.Role.String(),
		Status:    node.Status.String(),
		Mode:      node.Mode.String(),
		ReplicaOf: node.ReplicaOf.String(),
	}
}

func (r *Router) HandleInfo(w http.ResponseWriter, rq *http.Request) {
	node := r.db.Node()
	info := toNodeInfo(node)

	for _, n := range r.db.Nodes().All() {
		info.Nodes = append(info.Nodes, toNodeInfo(n))
	}

	resp, err := json.Marshal(info)
	if err != nil {
		log.Printf("[ERROR] Error marshaling node info: %v", err)
		http.Error(w, "Error marshaling node info", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(resp)))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(resp); err != nil {
		log.Printf("[ERROR] Error writing response: %v", err)
		http.Error(w, "Error writing response", http.StatusInternalServerError)
		return
	}
}

func (r *Router) HandleShutdown(w http.ResponseWriter, rq *http.Request) {
	defer rq.Body.Close()

	err := r.db.Close()
	if err != nil {
		log.Printf("[ERROR] Error shutting down the server: %v", err)
		http.Error(w, fmt.Sprintf("Error shutting down: %v", err), http.StatusInternalServerError)
		return
	}
	log.Println("[INFO] Server shutdown successfully")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintln(w, "Server shutting down...")
}
