package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
)

// ConnectTCP establishes a persistent TCP socket connection to the Benbroo TCP server.
// The addr should be in the form "host:port" (e.g., "localhost:6848").
func (c *Client) ConnectTCP(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tcpConn != nil {
		c.tcpConn.Close()
	}

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("tcp connect %s: %w", addr, err)
	}
	c.tcpConn = conn
	c.tcpReader = bufio.NewReader(conn)
	return nil
}

// CloseTCP closes the TCP socket connection.
func (c *Client) CloseTCP() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tcpConn != nil {
		c.tcpConn.Close()
		c.tcpConn = nil
		c.tcpReader = nil
	}
}

// tcpSend sends a command over the TCP socket and reads the response.
func (c *Client) tcpSend(cmd, payload string) (string, error) {
	c.mu.Lock()
	conn := c.tcpConn
	reader := c.tcpReader
	c.mu.Unlock()

	if conn == nil {
		return "", fmt.Errorf("TCP connection not established, call ConnectTCP() first")
	}

	conn.SetDeadline(time.Now().Add(10 * time.Second))

	line := cmd + " " + payload + "\n"
	if _, err := conn.Write([]byte(line)); err != nil {
		return "", fmt.Errorf("tcp write: %w", err)
	}

	resp, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("tcp read: %w", err)
	}
	resp = strings.TrimSpace(resp)

	if strings.HasPrefix(resp, "ERR ") {
		return "", fmt.Errorf("server error: %s", resp[4:])
	}
	if strings.HasPrefix(resp, "OK ") {
		return resp[3:], nil
	}
	return resp, nil
}

// ==================== TCP Service Operations ====================

// TCPRegisterInstance registers an instance via TCP socket.
func (c *Client) TCPRegisterInstance(opts RegisterOptions) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"namespaceId": withDefault(opts.NamespaceID, "public"),
		"groupName":   withDefault(opts.GroupName, "DEFAULT_GROUP"),
		"serviceName": opts.ServiceName,
		"clusterName": withDefault(opts.ClusterName, "DEFAULT"),
		"ip":          opts.IP,
		"port":        opts.Port,
		"weight":      opts.Weight,
		"ephemeral":   opts.Ephemeral,
		"metadata":    withDefault(opts.Metadata, "{}"),
	})
	_, err := c.tcpSend("REGISTER", string(payload))
	return err
}

// TCPDeregisterInstance removes an instance via TCP socket.
func (c *Client) TCPDeregisterInstance(namespaceID, groupName, serviceName, clusterName, ip string, port int) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"namespaceId": withDefault(namespaceID, "public"),
		"groupName":   withDefault(groupName, "DEFAULT_GROUP"),
		"serviceName": serviceName,
		"clusterName": withDefault(clusterName, "DEFAULT"),
		"ip":          ip,
		"port":        port,
	})
	_, err := c.tcpSend("DEREGISTER", string(payload))
	return err
}

// TCPSendHeartbeat sends a heartbeat via TCP socket.
func (c *Client) TCPSendHeartbeat(namespaceID, groupName, serviceName, clusterName, ip string, port int) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"namespaceId": withDefault(namespaceID, "public"),
		"groupName":   withDefault(groupName, "DEFAULT_GROUP"),
		"serviceName": serviceName,
		"clusterName": withDefault(clusterName, "DEFAULT"),
		"ip":          ip,
		"port":        port,
	})
	_, err := c.tcpSend("HEARTBEAT", string(payload))
	return err
}

// TCPGetInstances queries instances via TCP socket.
func (c *Client) TCPGetInstances(namespaceID, groupName, serviceName string, healthyOnly bool) ([]Instance, error) {
	payload, _ := json.Marshal(map[string]interface{}{
		"namespaceId": withDefault(namespaceID, "public"),
		"groupName":   withDefault(groupName, "DEFAULT_GROUP"),
		"serviceName": serviceName,
		"healthyOnly": healthyOnly,
	})
	data, err := c.tcpSend("DISCOVER", string(payload))
	if err != nil {
		return nil, err
	}
	var instances []Instance
	if err := json.Unmarshal([]byte(data), &instances); err != nil {
		return nil, fmt.Errorf("parse instances: %w", err)
	}
	return instances, nil
}

// TCPSelectInstance selects one instance using the given LB strategy via TCP.
func (c *Client) TCPSelectInstance(namespaceID, groupName, serviceName string, strategy LBStrategy) (*Instance, error) {
	instances, err := c.TCPGetInstances(namespaceID, groupName, serviceName, true)
	if err != nil {
		return nil, err
	}
	if len(instances) == 0 {
		return nil, fmt.Errorf("no healthy instance available")
	}

	key := namespaceID + "#" + groupName + "#" + serviceName
	switch strategy {
	case LBRoundRobin:
		return c.roundRobin(key, instances), nil
	case LBRandom:
		return &instances[randInt(len(instances))], nil
	case LBWeightedRoundRobin:
		return c.weightedSelect(instances), nil
	case LBConsistentHash:
		return c.consistentHash(instances, ""), nil
	default:
		return &instances[0], nil
	}
}

// ==================== TCP Config Operations ====================

// TCPGetConfig retrieves a config via TCP socket.
func (c *Client) TCPGetConfig(namespaceID, groupName, dataID string) (*ConfigItem, error) {
	payload, _ := json.Marshal(map[string]interface{}{
		"namespaceId": withDefault(namespaceID, "public"),
		"groupName":   withDefault(groupName, "DEFAULT_GROUP"),
		"dataId":      dataID,
	})
	data, err := c.tcpSend("CONFIG_GET", string(payload))
	if err != nil {
		return nil, err
	}
	var item ConfigItem
	if err := json.Unmarshal([]byte(data), &item); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &item, nil
}

// TCPPublishConfig publishes a config via TCP socket.
func (c *Client) TCPPublishConfig(namespaceID, groupName, dataID, content, cfgType string) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"namespaceId": withDefault(namespaceID, "public"),
		"groupName":   withDefault(groupName, "DEFAULT_GROUP"),
		"dataId":      dataID,
		"content":     content,
		"type":        withDefault(cfgType, "text"),
	})
	_, err := c.tcpSend("CONFIG_PUB", string(payload))
	return err
}

// TCPDeleteConfig deletes a config via TCP socket.
func (c *Client) TCPDeleteConfig(namespaceID, groupName, dataID string) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"namespaceId": withDefault(namespaceID, "public"),
		"groupName":   withDefault(groupName, "DEFAULT_GROUP"),
		"dataId":      dataID,
	})
	_, err := c.tcpSend("CONFIG_DEL", string(payload))
	return err
}

// ==================== TCP Health Operations ====================

// TCPReportSuccess reports a successful call via TCP socket.
func (c *Client) TCPReportSuccess(namespaceID, groupName, serviceName, ip string, port int) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"namespaceId": withDefault(namespaceID, "public"),
		"groupName":   withDefault(groupName, "DEFAULT_GROUP"),
		"serviceName": serviceName,
		"ip":          ip,
		"port":        port,
	})
	_, err := c.tcpSend("HEALTH_OK", string(payload))
	return err
}

// TCPReportFailure reports a failed call via TCP socket.
func (c *Client) TCPReportFailure(namespaceID, groupName, serviceName, ip string, port int) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"namespaceId": withDefault(namespaceID, "public"),
		"groupName":   withDefault(groupName, "DEFAULT_GROUP"),
		"serviceName": serviceName,
		"ip":          ip,
		"port":        port,
	})
	_, err := c.tcpSend("HEALTH_FAIL", string(payload))
	return err
}

// TCPPing sends a PING command to check connectivity.
func (c *Client) TCPPing() (string, error) {
	return c.tcpSend("PING", "{}")
}
