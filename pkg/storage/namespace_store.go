package storage

import (
	"github.com/benbroo/benbroo/pkg/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// NamespaceStore provides CRUD for namespaces.
type NamespaceStore struct {
	db *gorm.DB
}

func NewNamespaceStore(db *gorm.DB) *NamespaceStore {
	return &NamespaceStore{db: db}
}

// Create inserts a new namespace.
func (s *NamespaceStore) Create(ns *model.Namespace) error {
	return s.db.Clauses(clause.OnConflict{DoNothing: true}).Create(ns).Error
}

// Get retrieves a namespace by ID.
func (s *NamespaceStore) Get(id string) (*model.Namespace, error) {
	var ns model.Namespace
	err := s.db.Where("id = ?", id).First(&ns).Error
	if err != nil {
		return nil, err
	}
	return &ns, nil
}

// Delete removes a namespace by ID.
func (s *NamespaceStore) Delete(id string) error {
	return s.db.Where("id = ?", id).Delete(&model.Namespace{}).Error
}

// Update updates a namespace.
func (s *NamespaceStore) Update(ns *model.Namespace) error {
	return s.db.Model(ns).Updates(map[string]interface{}{
		"name":        ns.Name,
		"description": ns.Description,
	}).Error
}

// List returns all namespaces.
func (s *NamespaceStore) List() ([]model.Namespace, error) {
	var list []model.Namespace
	err := s.db.Find(&list).Error
	return list, err
}

// ClusterNodeStore provides CRUD for cluster nodes.
type ClusterNodeStore struct {
	db *gorm.DB
}

func NewClusterNodeStore(db *gorm.DB) *ClusterNodeStore {
	return &ClusterNodeStore{db: db}
}

// Upsert creates or updates a cluster node.
func (s *ClusterNodeStore) Upsert(node *model.ClusterNode) error {
	return s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "address"}},
		DoUpdates: clause.AssignmentColumns([]string{"state", "last_beat", "updated_at"}),
	}).Create(node).Error
}

// List returns all cluster nodes.
func (s *ClusterNodeStore) List() ([]model.ClusterNode, error) {
	var list []model.ClusterNode
	err := s.db.Find(&list).Error
	return list, err
}

// MarkDown marks a node as DOWN.
func (s *ClusterNodeStore) MarkDown(address string) error {
	return s.db.Model(&model.ClusterNode{}).Where("address = ?", address).Update("state", model.NodeStateDown).Error
}
