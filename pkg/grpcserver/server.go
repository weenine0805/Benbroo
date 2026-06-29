package grpcserver

import (
	"context"
	"fmt"
	"time"

	cfgservice "github.com/benbroo/benbroo/pkg/config"
	"github.com/benbroo/benbroo/pkg/health"
	"github.com/benbroo/benbroo/pkg/model"
	"github.com/benbroo/benbroo/pkg/naming"
	"github.com/benbroo/benbroo/pkg/storage"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements all gRPC service interfaces.
type Server struct {
	UnsafeNamingServiceServer
	UnsafeConfigServiceServer
	UnsafeHealthServiceServer

	namingSvc *naming.Service
	configSvc *cfgservice.Service
	healthChk *health.Checker
	instStore *storage.InstanceStore
	logger    *zap.Logger
}

// NewServer creates a new gRPC server implementation.
func NewServer(
	namingSvc *naming.Service,
	configSvc *cfgservice.Service,
	healthChk *health.Checker,
	instStore *storage.InstanceStore,
	log *zap.Logger,
) *Server {
	return &Server{
		namingSvc: namingSvc,
		configSvc: configSvc,
		healthChk: healthChk,
		instStore: instStore,
		logger:    log,
	}
}

// Register registers all gRPC services on the given grpc.Server.
func (s *Server) Register(srv *grpc.Server) {
	RegisterNamingServiceServer(srv, s)
	RegisterConfigServiceServer(srv, s)
	RegisterHealthServiceServer(srv, s)
}

// ==================== NamingService ====================

func (s *Server) RegisterInstance(ctx context.Context, req *RegisterInstanceRequest) (*Response, error) {
	inst := &model.ServiceInstance{
		NamespaceID: defaultVal(req.NamespaceId, "public"),
		GroupName:   defaultVal(req.GroupName, "DEFAULT_GROUP"),
		ServiceName: req.ServiceName,
		ClusterName: defaultVal(req.ClusterName, "DEFAULT"),
		IP:          req.Ip,
		Port:        int(req.Port),
		Weight:      req.Weight,
		Ephemeral:   req.Ephemeral,
		Metadata:    defaultVal(req.Metadata, "{}"),
	}
	if err := s.namingSvc.RegisterInstance(inst); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &Response{Code: 0, Message: "ok"}, nil
}

func (s *Server) DeregisterInstance(ctx context.Context, req *DeregisterInstanceRequest) (*Response, error) {
	err := s.namingSvc.DeregisterInstance(
		defaultVal(req.NamespaceId, "public"),
		defaultVal(req.GroupName, "DEFAULT_GROUP"),
		req.ServiceName,
		defaultVal(req.ClusterName, "DEFAULT"),
		req.Ip,
		int(req.Port),
	)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &Response{Code: 0, Message: "ok"}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*Response, error) {
	err := s.namingSvc.Heartbeat(
		defaultVal(req.NamespaceId, "public"),
		defaultVal(req.GroupName, "DEFAULT_GROUP"),
		req.ServiceName,
		defaultVal(req.ClusterName, "DEFAULT"),
		req.Ip,
		int(req.Port),
	)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &Response{Code: 0, Message: "ok"}, nil
}

func (s *Server) GetInstances(ctx context.Context, req *InstanceQuery) (*InstanceListResponse, error) {
	clusters := naming.ParseClusters(req.Clusters)
	list, err := s.namingSvc.GetInstances(
		defaultVal(req.NamespaceId, "public"),
		defaultVal(req.GroupName, "DEFAULT_GROUP"),
		req.ServiceName,
		clusters,
		req.HealthyOnly,
	)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	instances := make([]*InstanceInfo, 0, len(list))
	for _, inst := range list {
		instances = append(instances, toProtoInstance(&inst))
	}
	return &InstanceListResponse{
		Instances: instances,
		Count:     int32(len(instances)),
	}, nil
}

// ==================== ConfigService ====================

func (s *Server) GetConfig(ctx context.Context, req *ConfigQuery) (*ConfigInfo, error) {
	item, err := s.configSvc.GetConfig(
		defaultVal(req.NamespaceId, "public"),
		defaultVal(req.GroupName, "DEFAULT_GROUP"),
		req.DataId,
	)
	if err != nil {
		return nil, status.Error(codes.NotFound, "config not found")
	}
	return &ConfigInfo{
		NamespaceId: item.NamespaceID,
		GroupName:   item.GroupName,
		DataId:      item.DataID,
		Content:     item.Content,
		Md5:         item.MD5,
		Type:        item.Type,
	}, nil
}

func (s *Server) PublishConfig(ctx context.Context, req *ConfigPublishRequest) (*Response, error) {
	err := s.configSvc.PublishConfig(
		defaultVal(req.NamespaceId, "public"),
		defaultVal(req.GroupName, "DEFAULT_GROUP"),
		req.DataId,
		req.Content,
		defaultVal(req.Type, "text"),
	)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &Response{Code: 0, Message: "ok"}, nil
}

func (s *Server) ListenConfig(ctx context.Context, req *ConfigListenRequest) (*ConfigChangedResponse, error) {
	ns := defaultVal(req.NamespaceId, "public")
	clientMD5 := map[string]string{
		fmt.Sprintf("%s#%s#%s", ns, defaultVal(req.GroupName, "DEFAULT_GROUP"), req.DataId): req.Md5,
	}
	changed, err := s.configSvc.LongPoll(ns, clientMD5, 30*time.Second)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if changed == nil {
		return &ConfigChangedResponse{Changed: false}, nil
	}
	// Fetch updated content.
	item, err := s.configSvc.GetConfig(ns, defaultVal(req.GroupName, "DEFAULT_GROUP"), req.DataId)
	if err != nil {
		return &ConfigChangedResponse{Changed: false}, nil
	}
	return &ConfigChangedResponse{
		Changed: true,
		Content: item.Content,
		Md5:     item.MD5,
	}, nil
}

// ==================== HealthService ====================

func (s *Server) ReportFailure(ctx context.Context, req *HealthReportRequest) (*Response, error) {
	inst, err := s.instStore.Get(
		defaultVal(req.NamespaceId, "public"),
		defaultVal(req.GroupName, "DEFAULT_GROUP"),
		req.ServiceName,
		"DEFAULT",
		req.Ip,
		int(req.Port),
	)
	if err != nil {
		return nil, status.Error(codes.NotFound, "instance not found")
	}
	s.healthChk.ReportFailure(inst.ID, defaultVal(req.NamespaceId, "public"), defaultVal(req.GroupName, "DEFAULT_GROUP"), req.ServiceName)
	return &Response{Code: 0, Message: "ok"}, nil
}

func (s *Server) ReportSuccess(ctx context.Context, req *HealthReportRequest) (*Response, error) {
	inst, err := s.instStore.Get(
		defaultVal(req.NamespaceId, "public"),
		defaultVal(req.GroupName, "DEFAULT_GROUP"),
		req.ServiceName,
		"DEFAULT",
		req.Ip,
		int(req.Port),
	)
	if err != nil {
		return nil, status.Error(codes.NotFound, "instance not found")
	}
	s.healthChk.ReportSuccess(inst.ID, defaultVal(req.NamespaceId, "public"), defaultVal(req.GroupName, "DEFAULT_GROUP"), req.ServiceName)
	return &Response{Code: 0, Message: "ok"}, nil
}

// ==================== Helpers ====================

func toProtoInstance(inst *model.ServiceInstance) *InstanceInfo {
	return &InstanceInfo{
		Id:          inst.ID,
		NamespaceId: inst.NamespaceID,
		GroupName:   inst.GroupName,
		ServiceName: inst.ServiceName,
		ClusterName: inst.ClusterName,
		Ip:          inst.IP,
		Port:        int32(inst.Port),
		Weight:      inst.Weight,
		Healthy:     inst.Healthy,
		Enabled:     inst.Enabled,
		Ephemeral:   inst.Ephemeral,
		Metadata:    inst.Metadata,
	}
}

func defaultVal(val, def string) string {
	if val == "" {
		return def
	}
	return val
}
