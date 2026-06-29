package model

import "time"

// Namespace represents an isolated namespace.
type Namespace struct {
	ID          string    `json:"id"          gorm:"column:id;size:128;primaryKey"`
	Name        string    `json:"name"        gorm:"column:name;size:256;not null"`
	Description string    `json:"description" gorm:"column:description;size:1024;default:''"`
	CreatedAt   time.Time `json:"createdAt"   gorm:"column:created_at;autoCreateTime"`
	UpdatedAt   time.Time `json:"updatedAt"   gorm:"column:updated_at;autoUpdateTime"`
}

func (Namespace) TableName() string {
	return "namespace"
}
