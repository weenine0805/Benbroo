package client

import (
	"context"
	"fmt"
	"time"

	pb "github.com/benbroo/benbroo/pkg/grpcserver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ConnectGRPC establishes a gRPC connection to the Benbroo server.
// The addr should be in the form "host:port" (e.g., "localhost:9848").
// After calling this, gRPC methods become available.
func (c *Client) ConnectGRPC(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.grpcConn != nil {
		c.grpcConn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	c.grpcConn = conn
	c.namingClient = pb.NewNamingServiceClient(conn)
	c.configClient = pb.NewConfigServiceClient(conn)
	c.healthClient = pb.NewHealthServiceClient(conn)
	return nil
}

// CloseGRPC closes the gRPC connection.
func (c *Client) CloseGRPC() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.grpcConn != nil {
		c.grpcConn.Close()
		c.grpcConn = nil
		c.namingClient = nil
		c.configClient = nil
		c.healthClient = nil
	}
}

// HasGRPC returns true if a gRPC connection is established.
func (c *Client) HasGRPC() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.grpcConn != nil
}

// ==================== gRPC Naming ====================

// GRPCRegisterInstance registers an instance via gRPC.
func (c *Client) GRPCRegisterInstance(opts RegisterOptions) error {
	if !c.HasGRPC() {
		return errNoGRPC
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.namingClient.RegisterInstance(ctx, &pb.RegisterInstanceRequest{
		NamespaceId: withDefault(opts.NamespaceID, "public"),
		GroupName:   withDefault(opts.GroupName, "DEFAULT_GROUP"),
		ServiceName: opts.ServiceName,
		ClusterName: withDefault(opts.ClusterName, "DEFAULT"),
		Ip:          opts.IP,
		Port:        int32(opts.Port),
		Weight:      opts.Weight,
		Ephemeral:   opts.Ephemeral,
		Metadata:    withDefault(opts.Metadata, "{}"),
	})
	return err
}

// GRPCDeregisterInstance deregisters an instance via gRPC.
func (c *Client) GRPCDeregisterInstance(namespaceID, groupName, serviceName, clusterName, ip string, port int) error {
	if !c.HasGRPC() {
		return errNoGRPC
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.namingClient.DeregisterInstance(ctx, &pb.DeregisterInstanceRequest{
		NamespaceId: withDefault(namespaceID, "public"),
		GroupName:   withDefault(groupName, "DEFAULT_GROUP"),
		ServiceName: serviceName,
		ClusterName: withDefault(clusterName, "DEFAULT"),
		Ip:          ip,
		Port:        int32(port),
	})
	return err
}

// GRPCSendHeartbeat sends a heartbeat via gRPC.
func (c *Client) GRPCSendHeartbeat(namespaceID, groupName, serviceName, clusterName, ip string, port int) error {
	if !c.HasGRPC() {
		return errNoGRPC
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.namingClient.Heartbeat(ctx, &pb.HeartbeatRequest{
		NamespaceId: withDefault(namespaceID, "public"),
		GroupName:   withDefault(groupName, "DEFAULT_GROUP"),
		ServiceName: serviceName,
		ClusterName: withDefault(clusterName, "DEFAULT"),
		Ip:          ip,
		Port:        int32(port),
	})
	return err
}

// GRPCGetInstances queries instances via gRPC.
func (c *Client) GRPCGetInstances(namespaceID, groupName, serviceName string, healthyOnly bool) ([]Instance, error) {
	if !c.HasGRPC() {
		return nil, errNoGRPC
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.namingClient.GetInstances(ctx, &pb.InstanceQuery{
		NamespaceId: withDefault(namespaceID, "public"),
		GroupName:   withDefault(groupName, "DEFAULT_GROUP"),
		ServiceName: serviceName,
		HealthyOnly: healthyOnly,
	})
	if err != nil {
		return nil, err
	}
	instances := make([]Instance, 0, len(resp.Instances))
	for _, pi := range resp.Instances {
		instances = append(instances, Instance{
			ID:          pi.Id,
			NamespaceID: pi.NamespaceId,
			GroupName:   pi.GroupName,
			ServiceName: pi.ServiceName,
			ClusterName: pi.ClusterName,
			IP:          pi.Ip,
			Port:        int(pi.Port),
			Weight:      pi.Weight,
			Healthy:     pi.Healthy,
			Enabled:     pi.Enabled,
			Ephemeral:   pi.Ephemeral,
			Metadata:    pi.Metadata,
		})
	}
	return instances, nil
}

// GRPCSelectInstance picks one instance via gRPC using the given LB strategy.
func (c *Client) GRPCSelectInstance(namespaceID, groupName, serviceName string, strategy LBStrategy) (*Instance, error) {
	return c.GRPCSelectInstanceWithKey(namespaceID, groupName, serviceName, strategy, "")
}

// GRPCSelectInstanceWithKey picks one instance via gRPC using the given strategy and hash key.
func (c *Client) GRPCSelectInstanceWithKey(namespaceID, groupName, serviceName string, strategy LBStrategy, hashKey string) (*Instance, error) {
	instances, err := c.GRPCGetInstances(namespaceID, groupName, serviceName, true)
	if err != nil {
		return nil, err
	}
	if len(instances) == 0 {
		return nil, errNoHealthyInstance
	}
	switch strategy {
	case LBRoundRobin:
		return c.roundRobin(namespaceID+"#"+groupName+"#"+serviceName, instances), nil
	case LBRandom:
		idx := randInt(len(instances))
		return &instances[idx], nil
	case LBWeightedRoundRobin:
		return c.weightedSelect(instances), nil
	case LBConsistentHash:
		return c.consistentHash(instances, hashKey), nil
	default:
		return &instances[0], nil
	}
}

// ==================== gRPC Config ====================

// GRPCGetConfig retrieves a config via gRPC.
func (c *Client) GRPCGetConfig(namespaceID, groupName, dataID string) (*ConfigItem, error) {
	if !c.HasGRPC() {
		return nil, errNoGRPC
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.configClient.GetConfig(ctx, &pb.ConfigQuery{
		NamespaceId: withDefault(namespaceID, "public"),
		GroupName:   withDefault(groupName, "DEFAULT_GROUP"),
		DataId:      dataID,
	})
	if err != nil {
		return nil, err
	}
	return &ConfigItem{
		NamespaceID: resp.NamespaceId,
		GroupName:   resp.GroupName,
		DataID:      resp.DataId,
		Content:     resp.Content,
		MD5:         resp.Md5,
		Type:        resp.Type,
	}, nil
}

// GRPCPublishConfig publishes a config via gRPC.
func (c *Client) GRPCPublishConfig(namespaceID, groupName, dataID, content, cfgType string) error {
	if !c.HasGRPC() {
		return errNoGRPC
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.configClient.PublishConfig(ctx, &pb.ConfigPublishRequest{
		NamespaceId: withDefault(namespaceID, "public"),
		GroupName:   withDefault(groupName, "DEFAULT_GROUP"),
		DataId:      dataID,
		Content:     content,
		Type:        withDefault(cfgType, "text"),
	})
	return err
}

// GRPCWatchConfig watches for config changes via gRPC.
func (c *Client) GRPCWatchConfig(namespaceID, groupName, dataID string, onChange func(newContent string)) (cancel func()) {
	stopCh := make(chan struct{})
	var once syncOnce
	cancelFn := func() {
		once.Do(func() {
			close(stopCh)
		})
	}
	go func() {
		lastMD5 := ""
		lastContent := ""
		if item, err := c.GRPCGetConfig(namespaceID, groupName, dataID); err == nil {
			lastMD5 = item.MD5
			lastContent = item.Content
		}
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			ctx, ctxCancel := context.WithTimeout(context.Background(), 35*time.Second)
			resp, err := c.configClient.ListenConfig(ctx, &pb.ConfigListenRequest{
				NamespaceId: withDefault(namespaceID, "public"),
				GroupName:   withDefault(groupName, "DEFAULT_GROUP"),
				DataId:      dataID,
				Md5:         lastMD5,
			})
			ctxCancel()
			if err != nil {
				select {
				case <-stopCh:
					return
				case <-time.After(2 * time.Second):
				}
				continue
			}
			if resp.Changed && resp.Md5 != lastMD5 {
				lastMD5 = resp.Md5
				if resp.Content != lastContent {
					lastContent = resp.Content
					onChange(resp.Content)
				}
			}
		}
	}()
	return cancelFn
}

// ==================== gRPC Health ====================

// GRPCReportFailure reports a failure via gRPC.
func (c *Client) GRPCReportFailure(namespaceID, groupName, serviceName, ip string, port int) error {
	if !c.HasGRPC() {
		return errNoGRPC
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.healthClient.ReportFailure(ctx, &pb.HealthReportRequest{
		NamespaceId: withDefault(namespaceID, "public"),
		GroupName:   withDefault(groupName, "DEFAULT_GROUP"),
		ServiceName: serviceName,
		Ip:          ip,
		Port:        int32(port),
	})
	return err
}

// GRPCReportSuccess reports a success via gRPC.
func (c *Client) GRPCReportSuccess(namespaceID, groupName, serviceName, ip string, port int) error {
	if !c.HasGRPC() {
		return errNoGRPC
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.healthClient.ReportSuccess(ctx, &pb.HealthReportRequest{
		NamespaceId: withDefault(namespaceID, "public"),
		GroupName:   withDefault(groupName, "DEFAULT_GROUP"),
		ServiceName: serviceName,
		Ip:          ip,
		Port:        int32(port),
	})
	return err
}
