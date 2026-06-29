package storage

import (
	"github.com/benbroo/benbroo/pkg/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ConfigStore provides CRUD for config items.
type ConfigStore struct {
	db *gorm.DB
}

func NewConfigStore(db *gorm.DB) *ConfigStore {
	return &ConfigStore{db: db}
}

// Publish creates or updates a config item and saves a history record.
func (s *ConfigStore) Publish(item *model.ConfigItem, history *model.ConfigHistory) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "namespace_id"}, {Name: "group_name"}, {Name: "data_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"content", "md5", "type", "updated_at"}),
		}).Create(item).Error; err != nil {
			return err
		}
		return tx.Create(history).Error
	})
}

// Get retrieves a config item by its unique key.
func (s *ConfigStore) Get(namespaceID, groupName, dataID string) (*model.ConfigItem, error) {
	var item model.ConfigItem
	err := s.db.Where(
		"namespace_id = ? AND group_name = ? AND data_id = ?",
		namespaceID, groupName, dataID,
	).First(&item).Error
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// Delete removes a config item and saves a delete history record.
func (s *ConfigStore) Delete(namespaceID, groupName, dataID string, history *model.ConfigHistory) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where(
			"namespace_id = ? AND group_name = ? AND data_id = ?",
			namespaceID, groupName, dataID,
		).Delete(&model.ConfigItem{}).Error; err != nil {
			return err
		}
		return tx.Create(history).Error
	})
}

// List returns paginated config items.
func (s *ConfigStore) List(namespaceID, groupName, dataID string, offset, limit int) ([]model.ConfigItem, int64, error) {
	var (
		list  []model.ConfigItem
		total int64
	)
	q := s.db.Where("namespace_id = ?", namespaceID)
	if groupName != "" {
		q = q.Where("group_name = ?", groupName)
	}
	if dataID != "" {
		q = q.Where("data_id LIKE ?", "%"+dataID+"%")
	}
	if err := q.Model(&model.ConfigItem{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if err := q.Offset(offset).Limit(limit).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

// ListAllMD5 returns all config MD5 signatures for change detection.
func (s *ConfigStore) ListAllMD5(namespaceID string) ([]model.ConfigItem, error) {
	var list []model.ConfigItem
	err := s.db.Select("namespace_id, group_name, data_id, md5").
		Where("namespace_id = ?", namespaceID).
		Find(&list).Error
	return list, err
}

// History returns config history for a specific config item.
func (s *ConfigStore) History(namespaceID, groupName, dataID string, offset, limit int) ([]model.ConfigHistory, int64, error) {
	var (
		list  []model.ConfigHistory
		total int64
	)
	q := s.db.Where("namespace_id = ? AND group_name = ? AND data_id = ?", namespaceID, groupName, dataID)
	if err := q.Model(&model.ConfigHistory{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if err := q.Order("id DESC").Offset(offset).Limit(limit).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

// Count returns total config count.
func (s *ConfigStore) Count() (int64, error) {
	var count int64
	err := s.db.Model(&model.ConfigItem{}).Count(&count).Error
	return count, err
}
