package naming

import (
	"errors"
	"strings"
	"time"

	"github.com/benbroo/benbroo/pkg/model"
	"github.com/benbroo/benbroo/pkg/storage"
	"github.com/benbroo/benbroo/pkg/subscribe"
	"go.uber.org/zap"
)

// Service implements service discovery and instance registration.
type Service struct {
	instanceStore *storage.InstanceStore
	serviceStore  *storage.ServiceStore
	events        *subscribe.EventBus
	logger        *zap.Logger
}

func NewService(instStore *storage.InstanceStore, svcStore *storage.ServiceStore, events *subscribe.EventBus, log *zap.Logger) *Service {
	return &Service{
		instanceStore: instStore,
		serviceStore:  svcStore,
		events:        events,
		logger:        log,
	}
}

// RegisterInstance registers a new instance, creating the parent service if needed.
func (s *Service) RegisterInstance(inst *model.ServiceInstance) error {
	if inst.ServiceName == "" || inst.IP == "" || inst.Port <= 0 {
		return errors.New("serviceName, ip and port are required")
	}
	if inst.NamespaceID == "" {
		inst.NamespaceID = "public"
	}
	if inst.GroupName == "" {
		inst.GroupName = "DEFAULT_GROUP"
	}
	if inst.ClusterName == "" {
		inst.ClusterName = "DEFAULT"
	}
	if inst.Weight <= 0 {
		inst.Weight = 1.0
	}
	inst.Healthy = true
	inst.Enabled = true
	inst.LastBeat = time.Now()

	// Ensure the service exists.
	svc := &model.Service{
		NamespaceID: inst.NamespaceID,
		GroupName:   inst.GroupName,
		ServiceName: inst.ServiceName,
	}
	if err := s.serviceStore.Create(svc); err != nil {
		return err
	}

	if err := s.instanceStore.Create(inst); err != nil {
		return err
	}

	s.events.PublishServiceChange(inst.NamespaceID, inst.GroupName, inst.ServiceName)
	s.logger.Info("instance registered",
		zap.String("service", inst.ServiceName),
		zap.String("ip", inst.IP),
		zap.Int("port", inst.Port),
	)
	return nil
}

// DeregisterInstance removes an instance.
func (s *Service) DeregisterInstance(namespaceID, groupName, serviceName, clusterName, ip string, port int) error {
	if err := s.instanceStore.Delete(namespaceID, groupName, serviceName, clusterName, ip, port); err != nil {
		return err
	}
	s.events.PublishServiceChange(namespaceID, groupName, serviceName)
	s.logger.Info("instance deregistered",
		zap.String("service", serviceName),
		zap.String("ip", ip),
		zap.Int("port", port),
	)
	return nil
}

// UpdateInstance updates instance metadata/weight/enabled.
func (s *Service) UpdateInstance(inst *model.ServiceInstance) error {
	if err := s.instanceStore.Update(inst); err != nil {
		return err
	}
	s.events.PublishServiceChange(inst.NamespaceID, inst.GroupName, inst.ServiceName)
	return nil
}

// GetInstance returns a single instance.
func (s *Service) GetInstance(namespaceID, groupName, serviceName, clusterName, ip string, port int) (*model.ServiceInstance, error) {
	return s.instanceStore.Get(namespaceID, groupName, serviceName, clusterName, ip, port)
}

// GetInstances returns instances for a service, optionally filtered by clusters and health.
func (s *Service) GetInstances(namespaceID, groupName, serviceName string, clusters []string, healthyOnly bool) ([]model.ServiceInstance, error) {
	list, err := s.instanceStore.ListByClusters(namespaceID, groupName, serviceName, clusters)
	if err != nil {
		return nil, err
	}
	if healthyOnly {
		filtered := make([]model.ServiceInstance, 0, len(list))
		for _, inst := range list {
			if inst.Healthy && inst.Enabled {
				filtered = append(filtered, inst)
			}
		}
		return filtered, nil
	}
	return list, nil
}

// Heartbeat updates the last_beat timestamp for an instance.
func (s *Service) Heartbeat(namespaceID, groupName, serviceName, clusterName, ip string, port int) error {
	return s.instanceStore.UpdateHeartbeat(namespaceID, groupName, serviceName, clusterName, ip, port)
}

// GetService returns service info with instance count.
func (s *Service) GetService(namespaceID, groupName, serviceName string) (*model.Service, []model.ServiceInstance, error) {
	svc, err := s.serviceStore.Get(namespaceID, groupName, serviceName)
	if err != nil {
		return nil, nil, err
	}
	instances, err := s.instanceStore.List(namespaceID, groupName, serviceName)
	if err != nil {
		return nil, nil, err
	}
	return svc, instances, nil
}

// ListServices returns paginated services.
func (s *Service) ListServices(namespaceID, groupName string, pageNo, pageSize int) ([]model.Service, int64, error) {
	if pageNo < 1 {
		pageNo = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	offset := (pageNo - 1) * pageSize
	return s.serviceStore.List(namespaceID, groupName, offset, pageSize)
}

// CreateService creates a service entry.
func (s *Service) CreateService(svc *model.Service) error {
	if svc.NamespaceID == "" {
		svc.NamespaceID = "public"
	}
	if svc.GroupName == "" {
		svc.GroupName = "DEFAULT_GROUP"
	}
	return s.serviceStore.Create(svc)
}

// ParseClusters splits a comma-separated cluster string.
func ParseClusters(clusterStr string) []string {
	if clusterStr == "" {
		return nil
	}
	parts := strings.Split(clusterStr, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
