package storage

import (
	"github.com/benbroo/benbroo/pkg/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ServiceStore provides CRUD for services.
type ServiceStore struct {
	db *gorm.DB
}

func NewServiceStore(db *gorm.DB) *ServiceStore {
	return &ServiceStore{db: db}
}

// Create inserts a new service (or updates on conflict).
func (s *ServiceStore) Create(svc *model.Service) error {
	return s.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "namespace_id"}, {Name: "group_name"}, {Name: "service_name"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"protect_threshold", "metadata",
			"health_check_type", "health_check_proto", "health_check_path", "health_check_port",
			"active_interval", "passive_window", "passive_threshold",
			"updated_at",
		}),
	}).Create(svc).Error
}

// Get retrieves a service by its unique key.
func (s *ServiceStore) Get(namespaceID, groupName, serviceName string) (*model.Service, error) {
	var svc model.Service
	err := s.db.Where(
		"namespace_id = ? AND group_name = ? AND service_name = ?",
		namespaceID, groupName, serviceName,
	).First(&svc).Error
	if err != nil {
		return nil, err
	}
	return &svc, nil
}

// Delete removes a service.
func (s *ServiceStore) Delete(namespaceID, groupName, serviceName string) error {
	return s.db.Where(
		"namespace_id = ? AND group_name = ? AND service_name = ?",
		namespaceID, groupName, serviceName,
	).Delete(&model.Service{}).Error
}

// List returns paginated services.
func (s *ServiceStore) List(namespaceID, groupName string, offset, limit int) ([]model.Service, int64, error) {
	var (
		list  []model.Service
		total int64
	)
	q := s.db.Where("namespace_id = ?", namespaceID)
	if groupName != "" {
		q = q.Where("group_name = ?", groupName)
	}
	if err := q.Model(&model.Service{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if err := q.Offset(offset).Limit(limit).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

// ListAll returns all services (used by health checker to load configs).
func (s *ServiceStore) ListAll() ([]model.Service, error) {
	var list []model.Service
	err := s.db.Find(&list).Error
	return list, err
}

// UpdateHealthConfig updates only the health check configuration for a service.
func (s *ServiceStore) UpdateHealthConfig(namespaceID, groupName, serviceName string, cfg map[string]interface{}) error {
	return s.db.Model(&model.Service{}).
		Where("namespace_id = ? AND group_name = ? AND service_name = ?", namespaceID, groupName, serviceName).
		Updates(cfg).Error
}

// Count returns total service count.
func (s *ServiceStore) Count() (int64, error) {
	var count int64
	err := s.db.Model(&model.Service{}).Count(&count).Error
	return count, err
}
