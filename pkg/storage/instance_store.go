package storage

import (
	"github.com/benbroo/benbroo/pkg/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// InstanceStore provides CRUD for service instances.
type InstanceStore struct {
	db *gorm.DB
}

func NewInstanceStore(db *gorm.DB) *InstanceStore {
	return &InstanceStore{db: db}
}

// Create inserts a new instance (or updates on conflict).
func (s *InstanceStore) Create(inst *model.ServiceInstance) error {
	return s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "namespace_id"}, {Name: "group_name"}, {Name: "service_name"}, {Name: "cluster_name"}, {Name: "ip"}, {Name: "port"}},
		DoUpdates: clause.AssignmentColumns([]string{"weight", "healthy", "enabled", "ephemeral", "metadata", "last_beat", "updated_at"}),
	}).Create(inst).Error
}

// Delete removes an instance by its unique key.
func (s *InstanceStore) Delete(namespaceID, groupName, serviceName, clusterName, ip string, port int) error {
	return s.db.Where(
		"namespace_id = ? AND group_name = ? AND service_name = ? AND cluster_name = ? AND ip = ? AND port = ?",
		namespaceID, groupName, serviceName, clusterName, ip, port,
	).Delete(&model.ServiceInstance{}).Error
}

// Update updates instance fields.
func (s *InstanceStore) Update(inst *model.ServiceInstance) error {
	return s.db.Model(inst).Updates(map[string]interface{}{
		"weight":    inst.Weight,
		"healthy":   inst.Healthy,
		"enabled":   inst.Enabled,
		"metadata":  inst.Metadata,
		"last_beat": inst.LastBeat,
	}).Error
}

// Get retrieves a single instance by its unique key.
func (s *InstanceStore) Get(namespaceID, groupName, serviceName, clusterName, ip string, port int) (*model.ServiceInstance, error) {
	var inst model.ServiceInstance
	err := s.db.Where(
		"namespace_id = ? AND group_name = ? AND service_name = ? AND cluster_name = ? AND ip = ? AND port = ?",
		namespaceID, groupName, serviceName, clusterName, ip, port,
	).First(&inst).Error
	if err != nil {
		return nil, err
	}
	return &inst, nil
}

// List returns all instances for a service.
func (s *InstanceStore) List(namespaceID, groupName, serviceName string) ([]model.ServiceInstance, error) {
	var list []model.ServiceInstance
	err := s.db.Where(
		"namespace_id = ? AND group_name = ? AND service_name = ?",
		namespaceID, groupName, serviceName,
	).Find(&list).Error
	return list, err
}

// ListByClusters returns instances filtered by cluster names.
func (s *InstanceStore) ListByClusters(namespaceID, groupName, serviceName string, clusters []string) ([]model.ServiceInstance, error) {
	var list []model.ServiceInstance
	q := s.db.Where("namespace_id = ? AND group_name = ? AND service_name = ?", namespaceID, groupName, serviceName)
	if len(clusters) > 0 {
		q = q.Where("cluster_name IN ?", clusters)
	}
	err := q.Find(&list).Error
	return list, err
}

// UpdateHeartbeat updates the last_beat timestamp.
func (s *InstanceStore) UpdateHeartbeat(namespaceID, groupName, serviceName, clusterName, ip string, port int) error {
	return s.db.Model(&model.ServiceInstance{}).Where(
		"namespace_id = ? AND group_name = ? AND service_name = ? AND cluster_name = ? AND ip = ? AND port = ?",
		namespaceID, groupName, serviceName, clusterName, ip, port,
	).Update("last_beat", gorm.Expr("NOW(3)")).Error
}

// UpdateHealthy sets the healthy flag.
func (s *InstanceStore) UpdateHealthy(id uint64, healthy bool) error {
	return s.db.Model(&model.ServiceInstance{}).Where("id = ?", id).Update("healthy", healthy).Error
}

// ListUnhealthyBefore returns instances that have been unhealthy (last_beat older than cutoff).
func (s *InstanceStore) ListAll() ([]model.ServiceInstance, error) {
	var list []model.ServiceInstance
	err := s.db.Find(&list).Error
	return list, err
}

// ServiceInstanceCount holds total and healthy instance counts for a service.
type ServiceInstanceCount struct {
	NamespaceID string `gorm:"column:namespace_id"`
	GroupName   string `gorm:"column:group_name"`
	ServiceName string `gorm:"column:service_name"`
	Total       int    `gorm:"column:total"`
	Healthy     int    `gorm:"column:healthy"`
}

// CountGrouped returns instance counts grouped by service (single query).
func (s *InstanceStore) CountGrouped() (map[string]ServiceInstanceCount, error) {
	var rows []ServiceInstanceCount
	err := s.db.Model(&model.ServiceInstance{}).
		Select("namespace_id, group_name, service_name, COUNT(*) as total, SUM(CASE WHEN healthy = 1 THEN 1 ELSE 0 END) as healthy").
		Group("namespace_id, group_name, service_name").
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	m := make(map[string]ServiceInstanceCount, len(rows))
	for _, r := range rows {
		key := r.NamespaceID + "#" + r.GroupName + "#" + r.ServiceName
		m[key] = r
	}
	return m, nil
}

// Count returns the total instance count.
func (s *InstanceStore) Count() (int64, error) {
	var count int64
	err := s.db.Model(&model.ServiceInstance{}).Count(&count).Error
	return count, err
}
