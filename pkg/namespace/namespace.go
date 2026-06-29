package namespace

import (
	"errors"

	"github.com/benbroo/benbroo/pkg/model"
	"github.com/benbroo/benbroo/pkg/storage"
	"go.uber.org/zap"
)

// Service manages namespaces.
type Service struct {
	store  *storage.NamespaceStore
	logger *zap.Logger
}

func NewService(store *storage.NamespaceStore, log *zap.Logger) *Service {
	return &Service{store: store, logger: log}
}

// InitDefault ensures the "public" namespace exists.
func (s *Service) InitDefault() error {
	return s.store.Create(&model.Namespace{
		ID:          "public",
		Name:        "Public",
		Description: "Default public namespace",
	})
}

// Create creates a new namespace.
func (s *Service) Create(id, name, description string) error {
	if id == "" || name == "" {
		return errors.New("id and name are required")
	}
	ns := &model.Namespace{ID: id, Name: name, Description: description}
	if err := s.store.Create(ns); err != nil {
		return err
	}
	s.logger.Info("namespace created", zap.String("id", id), zap.String("name", name))
	return nil
}

// Delete removes a namespace.
func (s *Service) Delete(id string) error {
	if id == "public" {
		return errors.New("cannot delete the default public namespace")
	}
	return s.store.Delete(id)
}

// Update updates a namespace.
func (s *Service) Update(id, name, description string) error {
	ns, err := s.store.Get(id)
	if err != nil {
		return err
	}
	if name != "" {
		ns.Name = name
	}
	ns.Description = description
	return s.store.Update(ns)
}

// List returns all namespaces.
func (s *Service) List() ([]model.Namespace, error) {
	return s.store.List()
}

// Get returns a namespace by ID.
func (s *Service) Get(id string) (*model.Namespace, error) {
	return s.store.Get(id)
}
