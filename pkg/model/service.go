package model

import "time"

// Health check modes.
const (
	HealthCheckNone   = "NONE"   // Disabled
	HealthCheckActive = "ACTIVE" // Server-side active probing
	HealthCheckPassive = "PASSIVE" // Consumer-side failure reporting
	HealthCheckBoth   = "BOTH"   // Active + Passive combined
)

// Health check protocols for active checks.
const (
	HealthProtoTCP  = "TCP"
	HealthProtoHTTP = "HTTP"
)

// Service represents a logical service.
type Service struct {
	ID               uint64    `json:"id"               gorm:"primaryKey;autoIncrement"`
	NamespaceID      string    `json:"namespaceId"      gorm:"column:namespace_id;size:128;not null;default:public;uniqueIndex:uk_ns_group_service"`
	GroupName        string    `json:"groupName"        gorm:"column:group_name;size:128;not null;default:DEFAULT_GROUP;uniqueIndex:uk_ns_group_service"`
	ServiceName      string    `json:"serviceName"      gorm:"column:service_name;size:191;not null;uniqueIndex:uk_ns_group_service"`
	ProtectThreshold float64   `json:"protectThreshold" gorm:"column:protect_threshold;not null;default:0.0"`
	Metadata         string    `json:"metadata"         gorm:"column:metadata;type:text"`

	// --- Health check configuration (provider side) ---
	HealthCheckType  string `json:"healthCheckType"  gorm:"column:health_check_type;size:16;not null;default:ACTIVE"`
	HealthCheckProto string `json:"healthCheckProto" gorm:"column:health_check_proto;size:16;not null;default:TCP"`
	HealthCheckPath  string `json:"healthCheckPath"  gorm:"column:health_check_path;size:256;default:''"`
	HealthCheckPort  int    `json:"healthCheckPort"  gorm:"column:health_check_port;default:0"`
	ActiveInterval   int    `json:"activeInterval"   gorm:"column:active_interval;not null;default:5"`
	PassiveWindow    int    `json:"passiveWindow"    gorm:"column:passive_window;not null;default:60"`
	PassiveThreshold int    `json:"passiveThreshold" gorm:"column:passive_threshold;not null;default:5"`

	CreatedAt time.Time `json:"createdAt" gorm:"column:created_at;autoCreateTime"`
	UpdatedAt time.Time `json:"updatedAt" gorm:"column:updated_at;autoUpdateTime"`
}

func (Service) TableName() string {
	return "service"
}
