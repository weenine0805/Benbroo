package client

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/benbroo/benbroo/pkg/grpcserver"
	"google.golang.org/grpc"
)

// LBStrategy defines a load-balancing strategy.
type LBStrategy int

const (
	LBRoundRobin LBStrategy = iota
	LBRandom
	LBWeightedRoundRobin
	LBConsistentHash
)

// Client is the Benbroo SDK client.
type Client struct {
	serverAddr string
	token      string
	httpClient *http.Client

	// gRPC connection (optional)
	grpcConn     *grpc.ClientConn
	namingClient pb.NamingServiceClient
	configClient pb.ConfigServiceClient
	healthClient pb.HealthServiceClient

	// TCP socket connection (optional)
	tcpConn   net.Conn
	tcpReader *bufio.Reader

	// Auto-heartbeat management
	mu           sync.Mutex
	beatCancel   map[string]chan struct{} // key: instanceKey -> cancel channel
	beatInterval time.Duration

	// Round-robin counter per service
	rrMu    sync.Mutex
	rrIndex map[string]int
}

var (
	errNoGRPC            = errors.New("gRPC connection not established, call ConnectGRPC() first")
	errNoHealthyInstance = errors.New("no healthy instance available")
)

// syncOnce is an alias for sync.Once used in grpc.go
type syncOnce = sync.Once

// randInt returns a random int in [0, n)
func randInt(n int) int { return rand.Intn(n) }

// New creates a new Benbroo client.
func New(serverAddr string) *Client {
	return &Client{
		serverAddr:   strings.TrimRight(serverAddr, "/"),
		httpClient:   &http.Client{Timeout: 35 * time.Second},
		beatCancel:   make(map[string]chan struct{}),
		beatInterval: 5 * time.Second,
		rrIndex:      make(map[string]int),
	}
}

// ==================== Auth ====================

// Login authenticates with the server and stores the access token.
func (c *Client) Login(username, password string) error {
	resp, err := c.postForm("/v1/auth/login", url.Values{
		"username": {username},
		"password": {password},
	})
	if err != nil {
		return fmt.Errorf("login failed: %w", err)
	}
	if resp["code"] != float64(0) {
		return fmt.Errorf("login failed: %v", resp["message"])
	}
	data, ok := resp["data"].(map[string]interface{})
	if !ok {
		return errors.New("login: unexpected response format")
	}
	token, ok := data["accessToken"].(string)
	if !ok {
		return errors.New("login: no accessToken in response")
	}
	c.token = token
	return nil
}

// ==================== Service Registration ====================

// RegisterInstance registers a service instance with the server.
type RegisterOptions struct {
	NamespaceID string
	GroupName   string
	ServiceName string
	ClusterName string
	IP          string
	Port        int
	Weight      float64
	Ephemeral   bool
	Metadata    string
}

func (c *Client) RegisterInstance(opts RegisterOptions) error {
	params := url.Values{
		"namespaceId": {withDefault(opts.NamespaceID, "public")},
		"groupName":   {withDefault(opts.GroupName, "DEFAULT_GROUP")},
		"serviceName": {opts.ServiceName},
		"clusterName": {withDefault(opts.ClusterName, "DEFAULT")},
		"ip":          {opts.IP},
		"port":        {fmt.Sprintf("%d", opts.Port)},
		"weight":      {fmt.Sprintf("%f", opts.Weight)},
		"ephemeral":   {fmt.Sprintf("%t", opts.Ephemeral)},
		"metadata":    {withDefault(opts.Metadata, "{}")},
	}
	resp, err := c.postForm("/v1/ns/instance", params)
	if err != nil {
		return err
	}
	return checkResp(resp)
}

// DeregisterInstance removes a service instance.
func (c *Client) DeregisterInstance(namespaceID, groupName, serviceName, clusterName, ip string, port int) error {
	params := url.Values{
		"namespaceId": {withDefault(namespaceID, "public")},
		"groupName":   {withDefault(groupName, "DEFAULT_GROUP")},
		"serviceName": {serviceName},
		"clusterName": {withDefault(clusterName, "DEFAULT")},
		"ip":          {ip},
		"port":        {fmt.Sprintf("%d", port)},
	}
	resp, err := c.deleteReq("/v1/ns/instance", params)
	if err != nil {
		return err
	}
	return checkResp(resp)
}

// ==================== Heartbeat ====================

// SendHeartbeat sends a single heartbeat for an instance.
func (c *Client) SendHeartbeat(namespaceID, groupName, serviceName, clusterName, ip string, port int) error {
	params := url.Values{
		"namespaceId": {withDefault(namespaceID, "public")},
		"groupName":   {withDefault(groupName, "DEFAULT_GROUP")},
		"serviceName": {serviceName},
		"clusterName": {withDefault(clusterName, "DEFAULT")},
		"ip":          {ip},
		"port":        {fmt.Sprintf("%d", port)},
	}
	resp, err := c.putForm("/v1/ns/instance/beat", params)
	if err != nil {
		return err
	}
	return checkResp(resp)
}

// StartAutoHeartbeat starts a background goroutine that sends heartbeats periodically.
func (c *Client) StartAutoHeartbeat(namespaceID, groupName, serviceName, clusterName, ip string, port int) {
	key := fmt.Sprintf("%s#%s#%s#%s#%s:%d", namespaceID, groupName, serviceName, clusterName, ip, port)
	c.mu.Lock()
	if _, exists := c.beatCancel[key]; exists {
		c.mu.Unlock()
		return // already running
	}
	cancel := make(chan struct{})
	c.beatCancel[key] = cancel
	c.mu.Unlock()

	go func() {
		ticker := time.NewTicker(c.beatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = c.SendHeartbeat(namespaceID, groupName, serviceName, clusterName, ip, port)
			case <-cancel:
				return
			}
		}
	}()
}

// StopAutoHeartbeat stops the background heartbeat for an instance.
func (c *Client) StopAutoHeartbeat(namespaceID, groupName, serviceName, clusterName, ip string, port int) {
	key := fmt.Sprintf("%s#%s#%s#%s#%s:%d", namespaceID, groupName, serviceName, clusterName, ip, port)
	c.mu.Lock()
	if ch, ok := c.beatCancel[key]; ok {
		close(ch)
		delete(c.beatCancel, key)
	}
	c.mu.Unlock()
}

// StopAllHeartbeats stops all background heartbeats.
func (c *Client) StopAllHeartbeats() {
	c.mu.Lock()
	for key, ch := range c.beatCancel {
		close(ch)
		delete(c.beatCancel, key)
	}
	c.mu.Unlock()
}

// ==================== Service Discovery ====================

// Instance represents a service instance returned by the server.
type Instance struct {
	ID          uint64  `json:"id"`
	NamespaceID string  `json:"namespaceId"`
	GroupName   string  `json:"groupName"`
	ServiceName string  `json:"serviceName"`
	ClusterName string  `json:"clusterName"`
	IP          string  `json:"ip"`
	Port        int     `json:"port"`
	Weight      float64 `json:"weight"`
	Healthy     bool    `json:"healthy"`
	Enabled     bool    `json:"enabled"`
	Ephemeral   bool    `json:"ephemeral"`
	Metadata    string  `json:"metadata"`
}

// GetInstances returns instances for a service.
func (c *Client) GetInstances(namespaceID, groupName, serviceName string, healthyOnly bool) ([]Instance, error) {
	params := url.Values{
		"namespaceId": {withDefault(namespaceID, "public")},
		"groupName":   {withDefault(groupName, "DEFAULT_GROUP")},
		"serviceName": {serviceName},
		"healthyOnly": {fmt.Sprintf("%t", healthyOnly)},
	}
	resp, err := c.getReq("/v1/ns/instance/list", params)
	if err != nil {
		return nil, err
	}
	if err := checkResp(resp); err != nil {
		return nil, err
	}
	hosts, ok := resp["hosts"].([]interface{})
	if !ok {
		return nil, nil
	}
	instances := make([]Instance, 0, len(hosts))
	for _, h := range hosts {
		data, _ := json.Marshal(h)
		var inst Instance
		if err := json.Unmarshal(data, &inst); err == nil {
			instances = append(instances, inst)
		}
	}
	return instances, nil
}

// SelectInstance picks one instance using the given load-balancing strategy.
func (c *Client) SelectInstance(namespaceID, groupName, serviceName string, strategy LBStrategy) (*Instance, error) {
	return c.SelectInstanceWithKey(namespaceID, groupName, serviceName, strategy, "")
}

// SelectInstanceWithKey picks one instance using the given strategy and a hash key.
// The hashKey is only used for LBConsistentHash strategy to ensure the same key
// always maps to the same instance.
func (c *Client) SelectInstanceWithKey(namespaceID, groupName, serviceName string, strategy LBStrategy, hashKey string) (*Instance, error) {
	instances, err := c.GetInstances(namespaceID, groupName, serviceName, true)
	if err != nil {
		return nil, err
	}
	if len(instances) == 0 {
		return nil, errors.New("no healthy instance available")
	}

	switch strategy {
	case LBRoundRobin:
		return c.roundRobin(namespaceID+"#"+groupName+"#"+serviceName, instances), nil
	case LBRandom:
		return &instances[rand.Intn(len(instances))], nil
	case LBWeightedRoundRobin:
		return c.weightedSelect(instances), nil
	case LBConsistentHash:
		return c.consistentHash(instances, hashKey), nil
	default:
		return &instances[0], nil
	}
}

func (c *Client) roundRobin(key string, instances []Instance) *Instance {
	c.rrMu.Lock()
	idx := c.rrIndex[key] % len(instances)
	c.rrIndex[key] = idx + 1
	c.rrMu.Unlock()
	return &instances[idx]
}

func (c *Client) weightedSelect(instances []Instance) *Instance {
	totalWeight := 0.0
	for _, inst := range instances {
		totalWeight += inst.Weight
	}
	if totalWeight <= 0 {
		return &instances[rand.Intn(len(instances))]
	}
	r := rand.Float64() * totalWeight
	cumulative := 0.0
	for i, inst := range instances {
		cumulative += inst.Weight
		if r <= cumulative {
			return &instances[i]
		}
	}
	return &instances[len(instances)-1]
}

func (c *Client) consistentHash(instances []Instance, key string) *Instance {
	if key == "" {
		key = fmt.Sprintf("%d", rand.Int63())
	}
	// Build a sorted hash ring from all instances.
	type node struct {
		hash uint32
		idx  int
	}
	ring := make([]node, 0, len(instances)*160) // 160 virtual nodes per instance
	for i, inst := range instances {
		addr := fmt.Sprintf("%s:%d", inst.IP, inst.Port)
		for v := 0; v < 160; v++ {
			vkey := fmt.Sprintf("%s#%d", addr, v)
			h := crc32.ChecksumIEEE([]byte(vkey))
			ring = append(ring, node{hash: h, idx: i})
		}
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].hash < ring[j].hash })

	// Find the first node with hash >= keyHash.
	keyHash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(ring), func(i int) bool { return ring[i].hash >= keyHash })
	if idx >= len(ring) {
		idx = 0
	}
	return &instances[ring[idx].idx]
}

// ==================== Config Management ====================

// ConfigItem represents a configuration item.
type ConfigItem struct {
	NamespaceID string `json:"namespaceId"`
	GroupName   string `json:"groupName"`
	DataID      string `json:"dataId"`
	Content     string `json:"content"`
	MD5         string `json:"md5"`
	Type        string `json:"type"`
}

// GetConfig retrieves a configuration value.
func (c *Client) GetConfig(namespaceID, groupName, dataID string) (*ConfigItem, error) {
	params := url.Values{
		"tenant": {withDefault(namespaceID, "public")},
		"group":  {withDefault(groupName, "DEFAULT_GROUP")},
		"dataId": {dataID},
	}
	resp, err := c.getReq("/v1/cs/configs", params)
	if err != nil {
		return nil, err
	}
	if err := checkResp(resp); err != nil {
		return nil, err
	}
	data, _ := json.Marshal(resp["data"])
	var item ConfigItem
	if err := json.Unmarshal(data, &item); err != nil {
		return nil, err
	}
	return &item, nil
}

// PublishConfig creates or updates a configuration value.
func (c *Client) PublishConfig(namespaceID, groupName, dataID, content, cfgType string) error {
	params := url.Values{
		"tenant":  {withDefault(namespaceID, "public")},
		"group":   {withDefault(groupName, "DEFAULT_GROUP")},
		"dataId":  {dataID},
		"content": {content},
		"type":    {withDefault(cfgType, "text")},
	}
	resp, err := c.postForm("/v1/cs/configs", params)
	if err != nil {
		return err
	}
	return checkResp(resp)
}

// DeleteConfig removes a configuration value.
func (c *Client) DeleteConfig(namespaceID, groupName, dataID string) error {
	params := url.Values{
		"tenant": {withDefault(namespaceID, "public")},
		"group":  {withDefault(groupName, "DEFAULT_GROUP")},
		"dataId": {dataID},
	}
	resp, err := c.deleteReq("/v1/cs/configs", params)
	if err != nil {
		return err
	}
	return checkResp(resp)
}

// WatchConfig watches for config changes using long-polling.
// It calls the onChange callback whenever the config changes.
// Call the returned cancel function to stop watching.
func (c *Client) WatchConfig(namespaceID, groupName, dataID string, onChange func(newContent string)) (cancel func()) {
	stopCh := make(chan struct{})
	closed := false
	var once sync.Once
	cancelFn := func() {
		once.Do(func() {
			closed = true
			close(stopCh)
		})
	}
	_ = closed
	go func() {
		// Initialize lastMD5 from current server value.
		lastMD5 := ""
		lastContent := ""
		if item, err := c.GetConfig(namespaceID, groupName, dataID); err == nil {
			lastMD5 = item.MD5
			lastContent = item.Content
		}
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			// Build listening configs string.
			listenStr := fmt.Sprintf("%s\x02%s\x02%s\x02%s\x01",
				dataID, withDefault(groupName, "DEFAULT_GROUP"),
				lastMD5, withDefault(namespaceID, "public"))

			params := url.Values{
				"Listening-Configs": {listenStr},
				"tenant":            {withDefault(namespaceID, "public")},
			}
			resp, err := c.postForm("/v1/cs/configs/listener", params)
			if err != nil {
				select {
				case <-stopCh:
					return
				case <-time.After(2 * time.Second):
				}
				continue
			}
			changed, _ := resp["data"].(string)
			if changed != "" {
				// Server reported a change, fetch the new value.
				item, err := c.GetConfig(namespaceID, groupName, dataID)
				if err == nil && item.MD5 != lastMD5 {
					lastMD5 = item.MD5
					if item.Content != lastContent {
						lastContent = item.Content
						onChange(item.Content)
					}
				}
			}
		}
	}()
	return cancelFn
}

// ==================== Health Reporting ====================

// ReportFailure reports a call failure to the server (passive health check).
func (c *Client) ReportFailure(namespaceID, groupName, serviceName, ip string, port int) error {
	params := url.Values{
		"namespaceId": {withDefault(namespaceID, "public")},
		"groupName":   {withDefault(groupName, "DEFAULT_GROUP")},
		"serviceName": {serviceName},
		"ip":          {ip},
		"port":        {fmt.Sprintf("%d", port)},
	}
	resp, err := c.postForm("/v1/ns/health/instance/fail", params)
	if err != nil {
		return err
	}
	return checkResp(resp)
}

// ReportSuccess reports a call success to the server (passive health check).
func (c *Client) ReportSuccess(namespaceID, groupName, serviceName, ip string, port int) error {
	params := url.Values{
		"namespaceId": {withDefault(namespaceID, "public")},
		"groupName":   {withDefault(groupName, "DEFAULT_GROUP")},
		"serviceName": {serviceName},
		"ip":          {ip},
		"port":        {fmt.Sprintf("%d", port)},
	}
	resp, err := c.postForm("/v1/ns/health/instance/succeed", params)
	if err != nil {
		return err
	}
	return checkResp(resp)
}

// ==================== HTTP Helpers ====================

func (c *Client) postForm(path string, params url.Values) (map[string]interface{}, error) {
	reqURL := c.serverAddr + path
	req, err := http.NewRequest("POST", reqURL, strings.NewReader(params.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return c.doRequest(req)
}

func (c *Client) putForm(path string, params url.Values) (map[string]interface{}, error) {
	reqURL := c.serverAddr + path
	req, err := http.NewRequest("PUT", reqURL, strings.NewReader(params.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return c.doRequest(req)
}

func (c *Client) getReq(path string, params url.Values) (map[string]interface{}, error) {
	reqURL := c.serverAddr + path + "?" + params.Encode()
	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return c.doRequest(req)
}

func (c *Client) deleteReq(path string, params url.Values) (map[string]interface{}, error) {
	reqURL := c.serverAddr + path + "?" + params.Encode()
	req, err := http.NewRequest("DELETE", reqURL, nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return c.doRequest(req)
}

func (c *Client) doRequest(req *http.Request) (map[string]interface{}, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request %s %s failed: %w", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body failed: %w", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("invalid JSON response (status %d): %s", resp.StatusCode, string(body))
	}
	return result, nil
}

func checkResp(resp map[string]interface{}) error {
	code, ok := resp["code"].(float64)
	if !ok || code != 0 {
		msg, _ := resp["message"].(string)
		return fmt.Errorf("server error (code=%v): %s", resp["code"], msg)
	}
	return nil
}

func withDefault(val, def string) string {
	if val == "" {
		return def
	}
	return val
}
