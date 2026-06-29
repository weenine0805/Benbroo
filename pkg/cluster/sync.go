package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// SyncEvent represents a data change notification sent between cluster nodes.
type SyncEvent struct {
	Type      string `json:"type"`      // "service_change" or "config_change"
	Namespace string `json:"namespace"`
	Group     string `json:"group"`
	Service   string `json:"service,omitempty"` // for service events
	DataID    string `json:"dataId,omitempty"`  // for config events
	Source    string `json:"source"`            // originating node address
	Timestamp int64  `json:"timestamp"`
}

// Syncer handles data replication notifications between cluster nodes.
type Syncer struct {
	manager    *Manager
	logger     *zap.Logger
	httpClient *http.Client
	stopCh     chan struct{}
}

// NewSyncer creates a new cluster data syncer.
func NewSyncer(manager *Manager, log *zap.Logger) *Syncer {
	return &Syncer{
		manager:    manager,
		logger:     log,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		stopCh:     make(chan struct{}),
	}
}

// Start begins the periodic sync loop.
func (s *Syncer) Start() {
	go s.syncLoop()
	s.logger.Info("cluster syncer started")
}

// Stop stops the syncer.
func (s *Syncer) Stop() {
	close(s.stopCh)
}

// NotifyServiceChange sends a service change event to all peer nodes.
func (s *Syncer) NotifyServiceChange(namespace, group, service string) {
	event := SyncEvent{
		Type:      "service_change",
		Namespace: namespace,
		Group:     group,
		Service:   service,
		Source:    s.manager.SelfAddr(),
		Timestamp: time.Now().UnixMilli(),
	}
	s.broadcastEvent(event)
}

// NotifyConfigChange sends a config change event to all peer nodes.
func (s *Syncer) NotifyConfigChange(namespace, group, dataID string) {
	event := SyncEvent{
		Type:      "config_change",
		Namespace: namespace,
		Group:     group,
		DataID:    dataID,
		Source:    s.manager.SelfAddr(),
		Timestamp: time.Now().UnixMilli(),
	}
	s.broadcastEvent(event)
}

// broadcastEvent sends an event to all peer nodes (except self).
func (s *Syncer) broadcastEvent(event SyncEvent) {
	nodes := s.manager.GetNodes()
	for _, node := range nodes {
		if node.Address == s.manager.SelfAddr() {
			continue
		}
		if node.State != "UP" {
			continue
		}
		go s.sendEvent(node.Address, event)
	}
}

// sendEvent sends a sync event to a single peer node.
func (s *Syncer) sendEvent(addr string, event SyncEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	url := fmt.Sprintf("http://%s/v1/cluster/sync", addr)
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Benbroo-Sync", "1")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Debug("cluster sync: failed to notify peer",
			zap.String("peer", addr), zap.Error(err))
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		s.logger.Debug("cluster sync: peer returned non-200",
			zap.String("peer", addr), zap.Int("status", resp.StatusCode))
	}
}

// syncLoop periodically broadcasts a "ping" event to ensure connectivity.
func (s *Syncer) syncLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.logger.Debug("cluster sync: periodic heartbeat",
				zap.String("self", s.manager.SelfAddr()))
		case <-s.stopCh:
			return
		}
	}
}
