package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
	"github.com/nats-io/nats.go"
)

var (
	ErrShardUnavailable   = errors.New("shard unavailable")
	ErrNodeNotFound       = errors.New("node not found")
	ErrMessageTooLarge    = errors.New("message exceeds size limit")
	ErrPublishTimeout     = errors.New("publish timeout")
	ErrSubscriptionClosed = errors.New("subscription closed")
)

// Shard represents a logical partition of data
type Shard struct {
	ID          string        `json:"id"`
	NodeID      string        `json:"nodeId"`
	Status      ShardStatus   `json:"status"`
	LoadFactor  float64       `json:"loadFactor"` // 0-1 representing current load
	LastUpdated time.Time     `json:"lastUpdated"`
	Metadata    ShardMetadata `json:"metadata"`
}

type ShardStatus string

const (
	ShardActive    ShardStatus = "active"
	ShardMigrating ShardStatus = "migrating"
	ShardDraining  ShardStatus = "draining"
	ShardFailed    ShardStatus = "failed"
)

type ShardMetadata struct {
	Region        string        `json:"region"`
	AZ            string        `json:"availabilityZone"`
	CampaignCount int           `json:"campaignCount"`
	Throughput    float64       `json:"throughput"` // ops/sec
	ResourceUsage ResourceUsage `json:"resourceUsage"`
	Tags          []string      `json:"tags"`
}

type ResourceUsage struct {
	CPU     float64 `json:"cpu"`     // percentage
	Memory  float64 `json:"memory"`  // percentage
	Network float64 `json:"network"` // mbps
}

// ShardManager handles data distribution and rebalancing
type ShardManager struct {
	shards      []Shard
	consistent  *consistent.Consistent
	nodeMonitor *NodeMonitor
	shardMap    map[string]int // shardID -> index
	mu          sync.RWMutex
	rebalanceCh chan RebalanceEvent
	config      ShardManagerConfig
}

type ShardManagerConfig struct {
	RebalanceThreshold  float64       `json:"rebalanceThreshold"` // 0-1
	HealthCheckInterval time.Duration `json:"healthCheckInterval"`
	MaxShardsPerNode    int           `json:"maxShardsPerNode"`
}

type RebalanceEvent struct {
	Type    RebalanceType
	ShardID string
	NodeID  string
}

type RebalanceType string

const (
	RebalanceAdd    RebalanceType = "add"
	RebalanceRemove RebalanceType = "remove"
	RebalanceMove   RebalanceType = "move"
)

func NewShardManager(initialNodes []string, config ShardManagerConfig) *ShardManager {
	cfg := consistent.Config{
		PartitionCount:    271, // Large prime number
		ReplicationFactor: 20,
		Load:              config.RebalanceThreshold,
		Hasher:            hasher{},
	}

	sm := &ShardManager{
		consistent:  consistent.New(nil, cfg),
		nodeMonitor: NewNodeMonitor(config.HealthCheckInterval),
		shardMap:    make(map[string]int),
		rebalanceCh: make(chan RebalanceEvent, 100),
		config:      config,
	}

	// Add initial nodes
	for _, node := range initialNodes {
		sm.AddNode(node)
	}

	return sm
}

func (sm *ShardManager) AddNode(nodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.consistent.Add(nodeID)
	go sm.nodeMonitor.TrackNode(nodeID)
}

func (sm *ShardManager) RemoveNode(nodeID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Find all shards on this node
	var shardsToMove []string
	for _, shard := range sm.shards {
		if shard.NodeID == nodeID {
			shardsToMove = append(shardsToMove, shard.ID)
		}
	}

	if len(shardsToMove) > 0 {
		return errors.New("node still contains shards")
	}

	sm.consistent.Remove(nodeID)
	sm.nodeMonitor.UntrackNode(nodeID)
	return nil
}

func (sm *ShardManager) GetShard(campaignID string) (Shard, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	nodeID, err := sm.consistent.Get(campaignID)
	if err != nil {
		return Shard{}, ErrShardUnavailable
	}

	for _, shard := range sm.shards {
		if shard.NodeID == nodeID {
			return shard, nil
		}
	}

	return Shard{}, ErrShardUnavailable
}

func (sm *ShardManager) CreateShard(metadata ShardMetadata) (Shard, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Find least loaded node
	nodeID := sm.findOptimalNode()
	if nodeID == "" {
		return Shard{}, errors.New("no available nodes")
	}

	shard := Shard{
		ID:          generateShardID(),
		NodeID:      nodeID,
		Status:      ShardActive,
		LoadFactor:  0,
		LastUpdated: time.Now(),
		Metadata:    metadata,
	}

	sm.shards = append(sm.shards, shard)
	sm.shardMap[shard.ID] = len(sm.shards) - 1

	// Notify rebalance channel
	sm.rebalanceCh <- RebalanceEvent{
		Type:    RebalanceAdd,
		ShardID: shard.ID,
		NodeID:  nodeID,
	}

	return shard, nil
}

func (sm *ShardManager) findOptimalNode() string {
	// Implement logic to find node with:
	// 1. Fewest shards
	// 2. Lowest load
	// 3. Matching region/zone requirements
	return sm.consistent.GetLeastLoadedNode()
}

func (sm *ShardManager) StartRebalancer(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(sm.config.HealthCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case event := <-sm.rebalanceCh:
				sm.handleRebalance(event)
			case <-ticker.C:
				sm.checkShardHealth()
			}
		}
	}()
}

func (sm *ShardManager) handleRebalance(event RebalanceEvent) {
	// Implement shard migration logic based on event type
	// This would coordinate with the NodeMonitor to ensure smooth transitions
}

func (sm *ShardManager) checkShardHealth() {
	// Check shard status and trigger rebalances if needed
}

// NodeMonitor with enhanced capabilities
type NodeMonitor struct {
	nodes     map[string]NodeStatus
	mu        sync.RWMutex
	heartbeat time.Duration
	timeout   time.Duration
	eventCh   chan NodeEvent
	mlist     *memberlist.Memberlist
	config    NodeMonitorConfig
}

type NodeStatus struct {
	LastSeen     time.Time `json:"lastSeen"`
	State        NodeState `json:"state"`
	Load         float64   `json:"load"` // 0-1
	Capabilities []string  `json:"capabilities"`
	Stats        NodeStats `json:"stats"`
}

type NodeState string

const (
	NodeJoining NodeState = "joining"
	NodeActive  NodeState = "active"
	NodeLeaving NodeState = "leaving"
	NodeFailed  NodeState = "failed"
)

type NodeStats struct {
	CPUUsage    float64 `json:"cpuUsage"`
	MemoryUsage float64 `json:"memoryUsage"`
	NetworkIn   float64 `json:"networkIn"`  // MB/s
	NetworkOut  float64 `json:"networkOut"` // MB/s
}

type NodeEvent struct {
	NodeID string
	Type   NodeEventType
	Status NodeStatus
}

type NodeEventType string

const (
	NodeJoin    NodeEventType = "join"
	NodeLeave   NodeEventType = "leave"
	NodeFail    NodeEventType = "fail"
	NodeRecover NodeEventType = "recover"
)

type NodeMonitorConfig struct {
	GossipInterval time.Duration `json:"gossipInterval"`
	ProbeInterval  time.Duration `json:"probeInterval"`
	ProbeTimeout   time.Duration `json:"probeTimeout"`
	SuspicionMult  int           `json:"suspicionMult"` // For failure detection
}

func NewNodeMonitor(heartbeat time.Duration) *NodeMonitor {
	config := memberlist.DefaultLANConfig()
	config.GossipInterval = heartbeat
	config.ProbeInterval = heartbeat / 2
	config.ProbeTimeout = heartbeat / 4

	mlist, err := memberlist.Create(config)
	if err != nil {
		panic(err)
	}

	return &NodeMonitor{
		nodes:     make(map[string]NodeStatus),
		heartbeat: heartbeat,
		timeout:   heartbeat * 2,
		eventCh:   make(chan NodeEvent, 100),
		mlist:     mlist,
	}
}

func (nm *NodeMonitor) TrackNode(nodeID string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	nm.nodes[nodeID] = NodeStatus{
		LastSeen: time.Now(),
		State:    NodeJoining,
	}

	// Join the cluster
	_, err := nm.mlist.Join([]string{nodeID})
	if err != nil {
		nm.nodes[nodeID] = NodeStatus{
			State: NodeFailed,
		}
		return
	}

	go nm.monitorNode(nodeID)
}

func (nm *NodeMonitor) monitorNode(nodeID string) {
	ticker := time.NewTicker(nm.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			status, err := nm.checkNode(nodeID)
			if err != nil {
				nm.handleNodeFailure(nodeID)
				return
			}

			nm.mu.Lock()
			nm.nodes[nodeID] = status
			nm.mu.Unlock()
		}
	}
}

func (nm *NodeMonitor) checkNode(nodeID string) (NodeStatus, error) {
	// Implement actual health check logic
	// This could include:
	// - TCP/HTTP health checks
	// - Resource usage metrics
	// - Custom application health endpoints

	return NodeStatus{
		LastSeen: time.Now(),
		State:    NodeActive,
	}, nil
}

func (nm *NodeMonitor) handleNodeFailure(nodeID string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	status := nm.nodes[nodeID]
	status.State = NodeFailed
	nm.nodes[nodeID] = status

	nm.eventCh <- NodeEvent{
		NodeID: nodeID,
		Type:   NodeFail,
		Status: status,
	}
}

// NATSMessageQueue implements MessageQueue using NATS
type NATSMessageQueue struct {
	conn   *nats.Conn
	js     nats.JetStreamContext
	subs   map[string]*nats.Subscription
	mu     sync.Mutex
	config NATSConfig
}

type NATSConfig struct {
	URL            string        `json:"url"`
	MaxPayload     int           `json:"maxPayload"`
	ConnectTimeout time.Duration `json:"connectTimeout"`
	AckWait        time.Duration `json:"ackWait"`
}

func NewNATSMessageQueue(config NATSConfig) (*NATSMessageQueue, error) {
	nc, err := nats.Connect(config.URL,
		nats.Timeout(config.ConnectTimeout),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	return &NATSMessageQueue{
		conn:   nc,
		js:     js,
		subs:   make(map[string]*nats.Subscription),
		config: config,
	}, nil
}

func (mq *NATSMessageQueue) Publish(ctx context.Context, topic string, message interface{}) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	if len(data) > mq.config.MaxPayload {
		return ErrMessageTooLarge
	}

	select {
	case <-ctx.Done():
		return ErrPublishTimeout
	default:
		_, err := mq.js.Publish(topic, data)
		return err
	}
}

func (mq *NATSMessageQueue) Subscribe(ctx context.Context, topic string) (<-chan interface{}, error) {
	ch := make(chan interface{}, 1024)

	sub, err := mq.js.Subscribe(topic, func(msg *nats.Msg) {
		var data interface{}
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			return
		}

		select {
		case ch <- data:
			msg.Ack()
		case <-ctx.Done():
			return
		}
	}, nats.AckWait(mq.config.AckWait))

	if err != nil {
		return nil, err
	}

	mq.mu.Lock()
	mq.subs[topic] = sub
	mq.mu.Unlock()

	go func() {
		<-ctx.Done()
		mq.unsubscribe(topic)
		close(ch)
	}()

	return ch, nil
}

func (mq *NATSMessageQueue) unsubscribe(topic string) {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if sub, ok := mq.subs[topic]; ok {
		_ = sub.Unsubscribe()
		delete(mq.subs, topic)
	}
}

// Helper functions
func generateShardID() string {
	return "shard_" + uuid.New().String()
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// Implement a good hash function
	return 0 // Placeholder
}
