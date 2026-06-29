package tcpserver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	cfgservice "github.com/benbroo/benbroo/pkg/config"
	"github.com/benbroo/benbroo/pkg/health"
	"github.com/benbroo/benbroo/pkg/model"
	"github.com/benbroo/benbroo/pkg/naming"
	"github.com/benbroo/benbroo/pkg/storage"
	"go.uber.org/zap"
)

// Server is a raw TCP socket server for high-performance Benbroo communication.
// Protocol: text-based, newline-delimited.
//
//	Request:  COMMAND json_payload\n
//	Response: OK json_payload\n  or  ERR message\n
//
// Supported commands:
//
//	REGISTER    - register a service instance
//	DEREGISTER  - deregister a service instance
//	HEARTBEAT   - send a heartbeat for an instance
//	DISCOVER    - query instances for a service
//	CONFIG_GET  - get a config item
//	CONFIG_PUB  - publish (create/update) a config
//	CONFIG_DEL  - delete a config
//	HEALTH_OK   - report a successful call to an instance
//	HEALTH_FAIL - report a failed call to an instance
//	PING        - connectivity check
type Server struct {
	addr      string
	namingSvc *naming.Service
	configSvc *cfgservice.Service
	healthChk *health.Checker
	instStore *storage.InstanceStore
	logger    *zap.Logger

	listener net.Listener
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	stopCh   chan struct{}
}

// NewServer creates a new TCP server.
func NewServer(
	addr string,
	namingSvc *naming.Service,
	configSvc *cfgservice.Service,
	healthChk *health.Checker,
	instStore *storage.InstanceStore,
	log *zap.Logger,
) *Server {
	return &Server{
		addr:      addr,
		namingSvc: namingSvc,
		configSvc: configSvc,
		healthChk: healthChk,
		instStore: instStore,
		logger:    log,
		conns:     make(map[net.Conn]struct{}),
		stopCh:    make(chan struct{}),
	}
}

// Start begins listening for TCP connections.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("tcp server listen: %w", err)
	}
	s.listener = ln
	s.logger.Info("TCP server listening", zap.String("addr", s.addr))

	go s.acceptLoop()
	return nil
}

// Stop shuts down the TCP server.
func (s *Server) Stop() {
	close(s.stopCh)
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Lock()
	for conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()
}

// Addr returns the listening address.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				s.logger.Debug("tcp accept error", zap.Error(err))
				continue
			}
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 1MB max line

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		resp := s.processLine(line)
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		fmt.Fprintln(conn, resp)
	}
}

// processLine parses and dispatches a single command line.
func (s *Server) processLine(line string) string {
	// Split into COMMAND and JSON payload.
	idx := strings.IndexByte(line, ' ')
	var cmd, payload string
	if idx < 0 {
		cmd = strings.ToUpper(strings.TrimSpace(line))
		payload = "{}"
	} else {
		cmd = strings.ToUpper(line[:idx])
		payload = line[idx+1:]
	}

	switch cmd {
	case "PING":
		return `OK {"msg":"pong","ts":` + fmt.Sprintf("%d", time.Now().UnixMilli()) + "}"

	case "REGISTER":
		return s.cmdRegister(payload)

	case "DEREGISTER":
		return s.cmdDeregister(payload)

	case "HEARTBEAT":
		return s.cmdHeartbeat(payload)

	case "DISCOVER":
		return s.cmdDiscover(payload)

	case "CONFIG_GET":
		return s.cmdConfigGet(payload)

	case "CONFIG_PUB":
		return s.cmdConfigPub(payload)

	case "CONFIG_DEL":
		return s.cmdConfigDel(payload)

	case "HEALTH_OK":
		return s.cmdHealthReport(payload, true)

	case "HEALTH_FAIL":
		return s.cmdHealthReport(payload, false)

	default:
		return "ERR unknown command: " + cmd
	}
}

// ==================== Command Handlers ====================

type registerReq struct {
	NamespaceID string  `json:"namespaceId"`
	GroupName   string  `json:"groupName"`
	ServiceName string  `json:"serviceName"`
	ClusterName string  `json:"clusterName"`
	IP          string  `json:"ip"`
	Port        int     `json:"port"`
	Weight      float64 `json:"weight"`
	Ephemeral   bool    `json:"ephemeral"`
	Metadata    string  `json:"metadata"`
}

func (s *Server) cmdRegister(payload string) string {
	var req registerReq
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		return "ERR invalid json: " + err.Error()
	}
	inst := &model.ServiceInstance{
		NamespaceID: withDef(req.NamespaceID, "public"),
		GroupName:   withDef(req.GroupName, "DEFAULT_GROUP"),
		ServiceName: req.ServiceName,
		ClusterName: withDef(req.ClusterName, "DEFAULT"),
		IP:          req.IP,
		Port:        req.Port,
		Weight:      req.Weight,
		Ephemeral:   req.Ephemeral,
		Metadata:    withDef(req.Metadata, "{}"),
	}
	if err := s.namingSvc.RegisterInstance(inst); err != nil {
		return "ERR " + err.Error()
	}
	return `OK {"registered":true}`
}

type deregisterReq struct {
	NamespaceID string `json:"namespaceId"`
	GroupName   string `json:"groupName"`
	ServiceName string `json:"serviceName"`
	ClusterName string `json:"clusterName"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
}

func (s *Server) cmdDeregister(payload string) string {
	var req deregisterReq
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		return "ERR invalid json: " + err.Error()
	}
	err := s.namingSvc.DeregisterInstance(
		withDef(req.NamespaceID, "public"),
		withDef(req.GroupName, "DEFAULT_GROUP"),
		req.ServiceName,
		withDef(req.ClusterName, "DEFAULT"),
		req.IP, req.Port,
	)
	if err != nil {
		return "ERR " + err.Error()
	}
	return `OK {"deregistered":true}`
}

type heartbeatReq struct {
	NamespaceID string `json:"namespaceId"`
	GroupName   string `json:"groupName"`
	ServiceName string `json:"serviceName"`
	ClusterName string `json:"clusterName"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
}

func (s *Server) cmdHeartbeat(payload string) string {
	var req heartbeatReq
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		return "ERR invalid json: " + err.Error()
	}
	err := s.namingSvc.Heartbeat(
		withDef(req.NamespaceID, "public"),
		withDef(req.GroupName, "DEFAULT_GROUP"),
		req.ServiceName,
		withDef(req.ClusterName, "DEFAULT"),
		req.IP, req.Port,
	)
	if err != nil {
		return "ERR " + err.Error()
	}
	return `OK {"beat":true}`
}

type discoverReq struct {
	NamespaceID string `json:"namespaceId"`
	GroupName   string `json:"groupName"`
	ServiceName string `json:"serviceName"`
	HealthyOnly bool   `json:"healthyOnly"`
}

func (s *Server) cmdDiscover(payload string) string {
	var req discoverReq
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		return "ERR invalid json: " + err.Error()
	}
	instances, err := s.namingSvc.GetInstances(
		withDef(req.NamespaceID, "public"),
		withDef(req.GroupName, "DEFAULT_GROUP"),
		req.ServiceName,
		nil, req.HealthyOnly,
	)
	if err != nil {
		return "ERR " + err.Error()
	}
	data, _ := json.Marshal(instances)
	return "OK " + string(data)
}

type configGetReq struct {
	NamespaceID string `json:"namespaceId"`
	GroupName   string `json:"groupName"`
	DataID      string `json:"dataId"`
}

func (s *Server) cmdConfigGet(payload string) string {
	var req configGetReq
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		return "ERR invalid json: " + err.Error()
	}
	item, err := s.configSvc.GetConfig(
		withDef(req.NamespaceID, "public"),
		withDef(req.GroupName, "DEFAULT_GROUP"),
		req.DataID,
	)
	if err != nil {
		return "ERR " + err.Error()
	}
	data, _ := json.Marshal(item)
	return "OK " + string(data)
}

type configPubReq struct {
	NamespaceID string `json:"namespaceId"`
	GroupName   string `json:"groupName"`
	DataID      string `json:"dataId"`
	Content     string `json:"content"`
	Type        string `json:"type"`
}

func (s *Server) cmdConfigPub(payload string) string {
	var req configPubReq
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		return "ERR invalid json: " + err.Error()
	}
	err := s.configSvc.PublishConfig(
		withDef(req.NamespaceID, "public"),
		withDef(req.GroupName, "DEFAULT_GROUP"),
		req.DataID,
		req.Content,
		withDef(req.Type, "text"),
	)
	if err != nil {
		return "ERR " + err.Error()
	}
	return `OK {"published":true}`
}

type configDelReq struct {
	NamespaceID string `json:"namespaceId"`
	GroupName   string `json:"groupName"`
	DataID      string `json:"dataId"`
}

func (s *Server) cmdConfigDel(payload string) string {
	var req configDelReq
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		return "ERR invalid json: " + err.Error()
	}
	err := s.configSvc.DeleteConfig(
		withDef(req.NamespaceID, "public"),
		withDef(req.GroupName, "DEFAULT_GROUP"),
		req.DataID,
	)
	if err != nil {
		return "ERR " + err.Error()
	}
	return `OK {"deleted":true}`
}

type healthReportReq struct {
	NamespaceID string `json:"namespaceId"`
	GroupName   string `json:"groupName"`
	ServiceName string `json:"serviceName"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
}

func (s *Server) cmdHealthReport(payload string, success bool) string {
	var req healthReportReq
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		return "ERR invalid json: " + err.Error()
	}
	nsID := withDef(req.NamespaceID, "public")
	grpName := withDef(req.GroupName, "DEFAULT_GROUP")
	clusterName := "DEFAULT"

	// Look up instance ID.
	inst, err := s.instStore.Get(nsID, grpName, req.ServiceName, clusterName, req.IP, req.Port)
	if err != nil {
		return "ERR instance not found: " + err.Error()
	}
	if success {
		s.healthChk.ReportSuccess(inst.ID, nsID, grpName, req.ServiceName)
	} else {
		s.healthChk.ReportFailure(inst.ID, nsID, grpName, req.ServiceName)
	}
	return `OK {"reported":true}`
}

// ==================== Helpers ====================

func withDef(val, def string) string {
	if val == "" {
		return def
	}
	return val
}
