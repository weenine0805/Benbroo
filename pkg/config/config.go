package config

import (
	"crypto/md5"
	"errors"
	"fmt"
	"time"

	"github.com/benbroo/benbroo/pkg/model"
	"github.com/benbroo/benbroo/pkg/storage"
	"github.com/benbroo/benbroo/pkg/subscribe"
	"go.uber.org/zap"
)

// Service implements configuration management.
type Service struct {
	configStore *storage.ConfigStore
	events      *subscribe.EventBus
	logger      *zap.Logger
}

func NewService(cfgStore *storage.ConfigStore, events *subscribe.EventBus, log *zap.Logger) *Service {
	return &Service{
		configStore: cfgStore,
		events:      events,
		logger:      log,
	}
}

// PublishConfig creates or updates a config entry.
func (s *Service) PublishConfig(namespaceID, groupName, dataID, content, cfgType string) error {
	if dataID == "" {
		return errors.New("dataId is required")
	}
	if namespaceID == "" {
		namespaceID = "public"
	}
	if groupName == "" {
		groupName = "DEFAULT_GROUP"
	}
	if cfgType == "" {
		cfgType = "text"
	}

	md5sum := CalcMD5(content)

	item := &model.ConfigItem{
		NamespaceID: namespaceID,
		GroupName:   groupName,
		DataID:      dataID,
		Content:     content,
		MD5:         md5sum,
		Type:        cfgType,
	}
	history := &model.ConfigHistory{
		NamespaceID: namespaceID,
		GroupName:   groupName,
		DataID:      dataID,
		Content:     content,
		MD5:         md5sum,
		OpType:      "U",
	}

	if err := s.configStore.Publish(item, history); err != nil {
		return err
	}

	s.events.PublishConfigChange(namespaceID, groupName, dataID)
	s.logger.Info("config published",
		zap.String("dataId", dataID),
		zap.String("group", groupName),
		zap.String("namespace", namespaceID),
	)
	return nil
}

// GetConfig retrieves a config entry.
func (s *Service) GetConfig(namespaceID, groupName, dataID string) (*model.ConfigItem, error) {
	if namespaceID == "" {
		namespaceID = "public"
	}
	if groupName == "" {
		groupName = "DEFAULT_GROUP"
	}
	return s.configStore.Get(namespaceID, groupName, dataID)
}

// DeleteConfig removes a config entry.
func (s *Service) DeleteConfig(namespaceID, groupName, dataID string) error {
	if namespaceID == "" {
		namespaceID = "public"
	}
	if groupName == "" {
		groupName = "DEFAULT_GROUP"
	}

	// Get current content for history.
	existing, _ := s.configStore.Get(namespaceID, groupName, dataID)
	content := ""
	md5sum := CalcMD5("")
	if existing != nil {
		content = existing.Content
		md5sum = existing.MD5
	}

	history := &model.ConfigHistory{
		NamespaceID: namespaceID,
		GroupName:   groupName,
		DataID:      dataID,
		Content:     content,
		MD5:         md5sum,
		OpType:      "D",
	}

	if err := s.configStore.Delete(namespaceID, groupName, dataID, history); err != nil {
		return err
	}

	s.events.PublishConfigChange(namespaceID, groupName, dataID)
	s.logger.Info("config deleted",
		zap.String("dataId", dataID),
		zap.String("group", groupName),
	)
	return nil
}

// ListConfigs returns paginated configs.
func (s *Service) ListConfigs(namespaceID, groupName, dataID string, pageNo, pageSize int) ([]model.ConfigItem, int64, error) {
	if namespaceID == "" {
		namespaceID = "public"
	}
	if pageNo < 1 {
		pageNo = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	offset := (pageNo - 1) * pageSize
	return s.configStore.List(namespaceID, groupName, dataID, offset, pageSize)
}

// ConfigHistory returns config version history.
func (s *Service) ConfigHistory(namespaceID, groupName, dataID string, pageNo, pageSize int) ([]model.ConfigHistory, int64, error) {
	if pageNo < 1 {
		pageNo = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	offset := (pageNo - 1) * pageSize
	return s.configStore.History(namespaceID, groupName, dataID, offset, pageSize)
}

// GetAllMD5 returns all config MD5 for a namespace (used by long-polling).
func (s *Service) GetAllMD5(namespaceID string) (map[string]string, error) {
	items, err := s.configStore.ListAllMD5(namespaceID)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, len(items))
	for _, item := range items {
		key := fmt.Sprintf("%s#%s#%s", item.NamespaceID, item.GroupName, item.DataID)
		result[key] = item.MD5
	}
	return result, nil
}

// CalcMD5 computes the MD5 hex digest of content.
func CalcMD5(content string) string {
	h := md5.New()
	h.Write([]byte(content))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// LongPoll checks for config changes. Returns changed keys or times out.
func (s *Service) LongPoll(namespaceID string, clientMD5 map[string]string, timeout time.Duration) (map[string]string, error) {
	deadline := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return nil, nil // timeout, no changes
		case <-ticker.C:
			serverMD5, err := s.GetAllMD5(namespaceID)
			if err != nil {
				return nil, err
			}
			changed := make(map[string]string)
			for key, serverHash := range serverMD5 {
				if clientHash, ok := clientMD5[key]; !ok || clientHash != serverHash {
					changed[key] = serverHash
				}
			}
			// Check for deletions
			for key := range clientMD5 {
				if _, exists := serverMD5[key]; !exists {
					changed[key] = ""
				}
			}
			if len(changed) > 0 {
				return changed, nil
			}
		}
	}
}
