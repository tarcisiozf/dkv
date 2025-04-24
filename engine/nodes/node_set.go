package nodes

import (
	"github.com/tarcisiozf/dkv/engine/nodes/types/id"
	"github.com/tarcisiozf/dkv/slots"
	"sort"
)

type Set struct {
	nodes map[string]*Node
}

func NewSet() *Set {
	return &Set{
		nodes: make(map[string]*Node),
	}
}

func (ns *Set) Writers() []*Node {
	var writers []*Node
	for _, node := range ns.nodes {
		if node.Role.IsWriter() {
			writers = append(writers, node)
		}
	}

	sort.Slice(writers, func(i, j int) bool {
		return writers[i].SlotRange.Start() < writers[j].SlotRange.Start()
	})

	return writers
}

func (ns *Set) ReadReplicasOf(id id.ID) []*Node {
	var readReplicas []*Node
	for _, node := range ns.nodes {
		if node.Role.IsReadReplica() && node.ReplicaOf.Equal(id) {
			readReplicas = append(readReplicas, node)
		}
	}
	return readReplicas
}

func (ns *Set) WaitingForOrders() []*Node {
	var waitingForOrders []*Node
	for _, node := range ns.nodes {
		if node.Status.IsWaitingForOrders() {
			waitingForOrders = append(waitingForOrders, node)
		}
	}
	return waitingForOrders
}

func (ns *Set) Set(node *Node) {
	ns.nodes[node.ID.String()] = node
}

func (ns *Set) All() []*Node {
	all := make([]*Node, 0, len(ns.nodes))
	for _, node := range ns.nodes {
		all = append(all, node)
	}
	return all
}

func (ns *Set) Remove(node *Node) {
	delete(ns.nodes, node.ID.String())
}

func (ns *Set) GetById(id id.ID) *Node {
	return ns.nodes[id.String()]
}

func (ns *Set) Count() int {
	return len(ns.nodes)
}

func (ns *Set) IntersectingRange(slotRange slots.SlotRange) []*Node {
	var intersecting []*Node
	for _, node := range ns.nodes {
		if node.SlotRange.Intersects(slotRange) {
			intersecting = append(intersecting, node)
		}
	}
	return intersecting
}

func (ns *Set) Has(nodeId id.ID) bool {
	_, ok := ns.nodes[nodeId.String()]
	return ok
}
