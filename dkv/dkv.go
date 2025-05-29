package dkv

import (
	"encoding/json"
	"fmt"
	"github.com/tarcisiozf/dkv/slots"
	"io"
	"iter"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

func first[T any](it iter.Seq[T]) (elem T) {
	for elem = range it {
		break
	}
	return elem
}

const retryInterval = 30 * time.Second

type Option func(*Client) error

func WithBootstrap(nodes ...string) Option {
	return func(c *Client) error {
		if len(nodes) == 0 {
			return fmt.Errorf("no bootstrap bootstrap provided")
		}
		c.bootstrap = nodes
		return nil
	}
}

type NodeMeta struct {
	Healthy bool
	RetryIn time.Time
	Info    NodeInfo
}

func (m NodeMeta) HttpAddress() string {
	return m.Info.HttpAddress()
}

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

func (n NodeInfo) HttpAddress() string {
	return n.Address + ":" + n.Port
}

func (n NodeInfo) String() string {
	return fmt.Sprintf("ID: %s, Address: %s, Port: %s, SlotRange: %s", n.ID, n.Address, n.Port, n.SlotRange)
}

type Client struct {
	bootstrap  []string
	nodes      map[string]*NodeMeta
	httpClient *http.Client
	connected  bool
	debugMode  bool
}

func NewClient(options ...Option) (*Client, error) {
	client := &Client{
		bootstrap:  []string{},
		nodes:      make(map[string]*NodeMeta),
		httpClient: &http.Client{},
	}
	for _, opt := range options {
		if err := opt(client); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	return client, nil
}

func (c *Client) Connect() error {
	if c.connected {
		return nil
	}
	if len(c.bootstrap) == 0 {
		return fmt.Errorf("no bootstrap bootstrap provided")
	}
	for _, addr := range c.bootstrap {
		c.updateNodeInfo(addr)
	}
	go c.watchNodes()
	c.connected = true
	return nil
}

func (c *Client) Set(key, value string) error {
	if !c.connected {
		return fmt.Errorf("client not connected")
	}

	node := c.pickWriterForKey(key)
	if node == nil {
		return fmt.Errorf("no healthy nodes available")
	}

	body := strings.NewReader(value)
	resp, err := http.Post("http://"+node.HttpAddress()+"/"+key, "text/plain", body)
	if err != nil {
		return fmt.Errorf("failed to set value: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to set value, status code: %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) Get(key string) (string, bool, error) {
	if !c.connected {
		return "", false, fmt.Errorf("client not connected")
	}

	slotID := slots.GetSlotId([]byte(key))
	node := first(c.nodesForSlot(slotID))
	if node == nil {
		return "", false, fmt.Errorf("no healthy nodes available")
	}

	resp, err := http.Get("http://" + node.HttpAddress() + "/" + key)
	if err != nil {
		return "", false, fmt.Errorf("failed to get value: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", false, fmt.Errorf("failed to get value, status code: %d", resp.StatusCode)
	}
	value, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false, fmt.Errorf("failed to read response body: %w", err)
	}
	return string(value), true, nil
}

func (c *Client) Delete(key string) error {
	if !c.connected {
		return fmt.Errorf("client not connected")
	}

	node := c.pickWriterForKey(key)
	if node == nil {
		return fmt.Errorf("no healthy nodes available")
	}

	req, err := http.NewRequest(http.MethodDelete, "http://"+node.HttpAddress()+"/"+key, nil)
	if err != nil {
		return fmt.Errorf("failed to create delete request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete key, status code: %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) Close() {
	if !c.connected {
		return
	}
	c.connected = false
}

func (c *Client) watchNodes() {
	for {
		time.Sleep(time.Second)
		if !c.connected {
			return
		}

		for node := range c.nodeIterator() {
			if !node.Healthy && node.RetryIn.After(time.Now()) {
				continue
			}
			c.updateNodeInfo(node.HttpAddress())
			if node.Healthy {
				break
			}
		}
	}
}

func (c *Client) updateNodeInfo(addr string) {
	info, err := c.nodeInfo(addr)
	healthy := err == nil

	if err != nil && c.debugMode {
		fmt.Println("failed to get node info:", err)
	}

	node := c.getNodeByAddrOrID(addr, info.ID)

	node.Healthy = healthy
	if !healthy {
		node.RetryIn = time.Now().Add(retryInterval)
		return
	}

	node.Info = info
	for _, n := range info.Nodes {
		if n.ID == node.Info.ID {
			continue
		}
		addr := n.HttpAddress()
		if _, ok := c.nodes[addr]; ok {
			continue
		}
		c.updateNodeInfo(addr)
	}
}

func (c *Client) nodeInfo(node string) (info NodeInfo, err error) {
	resp, err := http.Get("http://" + node + "/info")
	if err != nil {
		return info, fmt.Errorf("failed to get node info: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return info, fmt.Errorf("failed to get node info, status code: %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return info, fmt.Errorf("failed to decode node info: %w", err)
	}
	return info, nil
}

func (c *Client) nodeIterator() iter.Seq[*NodeMeta] {
	numNodes := len(c.nodes)
	addrs := make([]string, 0, numNodes)
	for addr := range c.nodes {
		addrs = append(addrs, addr)
	}

	start := rand.Intn(numNodes)

	return func(yield func(*NodeMeta) bool) {
		for i := 0; i < numNodes; i++ {
			addr := addrs[(start+i)%numNodes]
			if !yield(c.nodes[addr]) {
				break
			}
		}
	}
}

func (c *Client) pickHealthyNode() *NodeMeta {
	return first(c.healthyNodes())
}

func (c *Client) healthyNodes() iter.Seq[*NodeMeta] {
	return func(yield func(*NodeMeta) bool) {
		for _, node := range c.nodes {
			if node.Healthy && !yield(node) {
				break
			}
		}
	}
}

func (c *Client) Nodes() []NodeMeta {
	nodes := make([]NodeMeta, 0)
	for _, node := range c.nodes {
		nodes = append(nodes, *node)
	}
	return nodes
}

func (c *Client) getNodeByAddrOrID(addr, id string) *NodeMeta {
	if id != "" {
		for _, node := range c.nodes {
			if node.Info.ID == id {
				return node
			}
		}
	}
	node := &NodeMeta{}
	c.nodes[addr] = node
	return node
}

func (c *Client) nodesForSlot(slotID uint16) iter.Seq[*NodeMeta] {
	return func(yield func(*NodeMeta) bool) {
		for node := range c.healthyNodes() {
			if node.Info.SlotRange.Contains(slotID) {
				if !yield(node) {
					break
				}
			}
		}
	}
}

func (c *Client) pickWriterForKey(key string) *NodeMeta {
	slotID := slots.GetSlotId([]byte(key))

	for node := range c.nodesForSlot(slotID) {
		if node.Info.Role == "writer" {
			return node
		}
	}
	return nil
}
