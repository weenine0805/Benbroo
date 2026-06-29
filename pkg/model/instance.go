package model

import (
	"strconv"
	"time"
)

// ServiceInstance represents a registered service instance.
type ServiceInstance struct {
	ID          uint64    `json:"id"           gorm:"primaryKey;autoIncrement"`
	NamespaceID string    `json:"namespaceId"  gorm:"column:namespace_id;size:128;not null;default:public;uniqueIndex:uk_ns_svc_ip_port"`
	GroupName   string    `json:"groupName"    gorm:"column:group_name;size:128;not null;default:DEFAULT_GROUP;uniqueIndex:uk_ns_svc_ip_port"`
	ServiceName string    `json:"serviceName"  gorm:"column:service_name;size:191;not null;uniqueIndex:uk_ns_svc_ip_port;index:idx_ns_service"`
	ClusterName string    `json:"clusterName"  gorm:"column:cluster_name;size:128;not null;default:DEFAULT;uniqueIndex:uk_ns_svc_ip_port"`
	IP          string    `json:"ip"           gorm:"column:ip;size:64;not null;uniqueIndex:uk_ns_svc_ip_port"`
	Port        int       `json:"port"         gorm:"column:port;not null;uniqueIndex:uk_ns_svc_ip_port"`
	Weight      float64   `json:"weight"       gorm:"column:weight;not null;default:1.0"`
	Healthy     bool      `json:"healthy"      gorm:"column:healthy;not null;default:true"`
	Enabled     bool      `json:"enabled"      gorm:"column:enabled;not null;default:true"`
	Ephemeral   bool      `json:"ephemeral"    gorm:"column:ephemeral;not null;default:true"`
	Metadata    string    `json:"metadata"     gorm:"column:metadata;type:text"`
	LastBeat    time.Time `json:"lastBeat"     gorm:"column:last_beat;not null"`
	CreatedAt   time.Time `json:"createdAt"    gorm:"column:created_at;autoCreateTime"`
	UpdatedAt   time.Time `json:"updatedAt"    gorm:"column:updated_at;autoUpdateTime"`
}

func (ServiceInstance) TableName() string {
	return "instance"
}

// InstanceKey returns a unique string key for this instance.
func (s *ServiceInstance) InstanceKey() string {
	return s.NamespaceID + "#" + s.GroupName + "#" + s.ServiceName + "#" + s.ClusterName + "#" + s.IP + ":" + strconv.Itoa(s.Port)
}
