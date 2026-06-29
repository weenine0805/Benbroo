package model

import "time"

// ClusterNode represents a node in the Benbroo cluster.
type ClusterNode struct {
	ID        uint64    `json:"id"        gorm:"primaryKey;autoIncrement"`
	Address   string    `json:"address"   gorm:"column:address;size:256;not null;uniqueIndex:uk_address"`
	State     string    `json:"state"     gorm:"column:state;size:32;not null;default:UP"`
	LastBeat  time.Time `json:"lastBeat"  gorm:"column:last_beat;not null"`
	CreatedAt time.Time `json:"createdAt" gorm:"column:created_at;autoCreateTime"`
	UpdatedAt time.Time `json:"updatedAt" gorm:"column:updated_at;autoUpdateTime"`
}

func (ClusterNode) TableName() string {
	return "cluster_node"
}

const (
	NodeStateUp   = "UP"
	NodeStateDown = "DOWN"
)
