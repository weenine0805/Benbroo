package model

import "time"

// ConfigItem represents a configuration entry.
type ConfigItem struct {
	ID          uint64    `json:"id"          gorm:"primaryKey;autoIncrement"`
	NamespaceID string    `json:"namespaceId" gorm:"column:namespace_id;size:128;not null;default:public;uniqueIndex:uk_ns_group_dataid"`
	GroupName   string    `json:"groupName"   gorm:"column:group_name;size:128;not null;default:DEFAULT_GROUP;uniqueIndex:uk_ns_group_dataid"`
	DataID      string    `json:"dataId"      gorm:"column:data_id;size:191;not null;uniqueIndex:uk_ns_group_dataid"`
	Content     string    `json:"content"     gorm:"column:content;type:longtext;not null"`
	MD5         string    `json:"md5"         gorm:"column:md5;size:64;not null"`
	Type        string    `json:"type"        gorm:"column:type;size:64;default:text"`
	CreatedAt   time.Time `json:"createdAt"   gorm:"column:created_at;autoCreateTime"`
	UpdatedAt   time.Time `json:"updatedAt"   gorm:"column:updated_at;autoUpdateTime"`
}

func (ConfigItem) TableName() string {
	return "config_info"
}

// ConfigHistory represents a historical version of a config item.
type ConfigHistory struct {
	ID          uint64    `json:"id"          gorm:"primaryKey;autoIncrement"`
	NamespaceID string    `json:"namespaceId" gorm:"column:namespace_id;size:128;not null;default:public;index:idx_ns_group_dataid"`
	GroupName   string    `json:"groupName"   gorm:"column:group_name;size:128;not null;default:DEFAULT_GROUP;index:idx_ns_group_dataid"`
	DataID      string    `json:"dataId"      gorm:"column:data_id;size:191;not null;index:idx_ns_group_dataid"`
	Content     string    `json:"content"     gorm:"column:content;type:longtext;not null"`
	MD5         string    `json:"md5"         gorm:"column:md5;size:64;not null"`
	OpType      string    `json:"opType"      gorm:"column:op_type;size:32;not null;default:U"`
	CreatedAt   time.Time `json:"createdAt"   gorm:"column:created_at;autoCreateTime"`
}

func (ConfigHistory) TableName() string {
	return "config_history"
}
