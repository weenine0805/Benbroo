package cluster

import (
	"sort"
	"sync"
	"time"

	"github.com/benbroo/benbroo/pkg/model"
	"github.com/benbroo/benbroo/pkg/storage"
	"go.uber.org/zap"
)

// Config holds cluster settings.
type Config struct {
	SelfAddr string   `yaml:"selfAddr"`
	Members  []string `yaml:"members"`
}

// Manager handles cluster membership and leader election.
type Manager struct {
	cfg       Config
	store     *storage.ClusterNodeStore
	logger    *zap.Logger
	mu        sync.RWMutex
	nodes     []model.ClusterNode
	stopCh    chan struct{}
}

func NewManager(cfg Config, store *storage.ClusterNodeStore, log *zap.Logger) *Manager {
	return &Manager{
		cfg:    cfg,
		store:  store,
		logger: log,
		stopCh: make(chan struct{}),
	}
}

// Start registers self and begins heartbeat.
func (m *Manager) Start() {
	// Register self node.
	selfNode := &model.ClusterNode{
		Address:  m.cfg.SelfAddr,
		State:    model.NodeStateUp,
		LastBeat: time.Now(),
	}
	if err := m.store.Upsert(selfNode); err != nil {
		m.logger.Error("failed to register self node", zap.Error(err))
	}

	// Register configured members.
	for _, addr := range m.cfg.Members {
		if addr == m.cfg.SelfAddr {
			continue
		}
		node := &model.ClusterNode{
			Address:  addr,
			State:    model.NodeStateUp,
			LastBeat: time.Now(),
		}
		_ = m.store.Upsert(node)
	}

	// Start heartbeat loop.
	go m.heartbeatLoop()
	m.logger.Info("cluster manager started", zap.String("self", m.cfg.SelfAddr))
}

// Stop stops the cluster manager.
func (m *Manager) Stop() {
	close(m.stopCh)
}

func (m *Manager) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.refreshNodes()
			m.updateSelfHeartbeat()
		case <-m.stopCh:
			return
		}
	}
}

func (m *Manager) updateSelfHeartbeat() {
	node := &model.ClusterNode{
		Address:  m.cfg.SelfAddr,
		State:    model.NodeStateUp,
		LastBeat: time.Now(),
	}
	_ = m.store.Upsert(node)
}

func (m *Manager) refreshNodes() {
	nodes, err := m.store.List()
	if err != nil {
		m.logger.Error("failed to refresh cluster nodes", zap.Error(err))
		return
	}

	// Mark nodes that haven't sent heartbeat for > 15s as DOWN.
	now := time.Now()
	for _, node := range nodes {
		if node.Address == m.cfg.SelfAddr {
			continue
		}
		if now.Sub(node.LastBeat) > 15*time.Second && node.State == model.NodeStateUp {
			_ = m.store.MarkDown(node.Address)
			node.State = model.NodeStateDown
		}
	}

	m.mu.Lock()
	m.nodes = nodes
	m.mu.Unlock()
}

// GetNodes returns current cluster nodes.
func (m *Manager) GetNodes() []model.ClusterNode {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]model.ClusterNode, len(m.nodes))
	copy(result, m.nodes)
	return result
}

// IsLeader returns true if this node is the leader (lowest address).
func (m *Manager) IsLeader() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.nodes) == 0 {
		return true // single node mode
	}
	addrs := make([]string, 0, len(m.nodes))
	for _, n := range m.nodes {
		if n.State == model.NodeStateUp {
			addrs = append(addrs, n.Address)
		}
	}
	if len(addrs) == 0 {
		return true
	}
	sort.Strings(addrs)
	return addrs[0] == m.cfg.SelfAddr
}

// SelfAddr returns this node's address.
func (m *Manager) SelfAddr() string {
	return m.cfg.SelfAddr
}

// ShouldCheckInstance returns true if this node is responsible for checking
// the given instance (simple hash-based sharding).
func (m *Manager) ShouldCheckInstance(instanceKey string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	upNodes := make([]string, 0)
	for _, n := range m.nodes {
		if n.State == model.NodeStateUp {
			upNodes = append(upNodes, n.Address)
		}
	}
	if len(upNodes) == 0 {
		return true
	}
	sort.Strings(upNodes)

	// Simple hash to assign.
	hash := 0
	for _, c := range instanceKey {
		hash = hash*31 + int(c)
	}
	if hash < 0 {
		hash = -hash
	}
	idx := hash % len(upNodes)
	return upNodes[idx] == m.cfg.SelfAddr
}
