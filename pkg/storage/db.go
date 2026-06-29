package storage

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/benbroo/benbroo/pkg/model"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Config holds MySQL connection settings.
// Each field is individually configurable in the YAML config file.
type Config struct {
	Host         string `yaml:"host"`         // MySQL server host (default: 127.0.0.1)
	Port         int    `yaml:"port"`         // MySQL server port (default: 3306)
	Username     string `yaml:"username"`     // MySQL username
	Password     string `yaml:"password"`     // MySQL password
	Database     string `yaml:"database"`     // Database name (auto-created if not exists)
	Charset      string `yaml:"charset"`      // Character set (default: utf8mb4)
	MaxIdleConns int    `yaml:"maxIdleConns"` // Max idle connections (default: 10)
	MaxOpenConns int    `yaml:"maxOpenConns"` // Max open connections (default: 100)

	// DSN is kept for backward compatibility. If set, it overrides the individual fields.
	DSN string `yaml:"dsn"`
}

// buildDSN constructs a MySQL DSN string from individual config fields.
// If DSN is explicitly set, it takes priority.
func (c *Config) buildDSN() string {
	if c.DSN != "" {
		return c.DSN
	}
	c.applyDefaults()
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=Local",
		c.Username, c.Password, c.Host, c.Port, c.Database, c.Charset)
}

// applyDefaults fills in default values for empty fields.
func (c *Config) applyDefaults() {
	if c.Host == "" {
		c.Host = "127.0.0.1"
	}
	if c.Port == 0 {
		c.Port = 3306
	}
	if c.Database == "" {
		c.Database = "benbroo"
	}
	if c.Charset == "" {
		c.Charset = "utf8mb4"
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 10
	}
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = 100
	}
}

// Storage wraps the GORM database handle.
type Storage struct {
	DB     *gorm.DB
	logger *zap.Logger
}

// New creates a new Storage and connects to MySQL.
// It auto-creates the database if it does not exist.
func New(cfg Config, log *zap.Logger) (*Storage, error) {
	cfg.applyDefaults()
	dsn := cfg.buildDSN()

	// Ensure the database exists.
	if err := ensureDatabase(dsn); err != nil {
		return nil, fmt.Errorf("ensure database: %w", err)
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mysql: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)

	// Auto-migrate all models.
	if err := db.AutoMigrate(
		&model.Namespace{},
		&model.Service{},
		&model.ServiceInstance{},
		&model.ConfigItem{},
		&model.ConfigHistory{},
		&model.ClusterNode{},
	); err != nil {
		return nil, fmt.Errorf("auto-migrate failed: %w", err)
	}

	log.Info("MySQL connected and migrated",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database),
		zap.String("username", cfg.Username),
	)
	return &Storage{DB: db, logger: log}, nil
}

// ensureDatabase connects to MySQL (without a specific DB) and creates
// the target database if it does not already exist.
func ensureDatabase(dsn string) error {
	// Parse: user:pass@tcp(host:port)/dbname?params
	idx := strings.Index(dsn, "/")
	if idx < 0 {
		return nil
	}
	prefix := dsn[:idx] // user:pass@tcp(host:port)
	rest := dsn[idx+1:] // dbname?params
	dbName := strings.SplitN(rest, "?", 2)[0]
	if dbName == "" {
		return nil
	}

	// Connect without database.
	rootDSN := prefix + "/?charset=utf8mb4&parseTime=True&loc=Local"
	conn, err := sql.Open("mysql", rootDSN)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", dbName))
	return err
}

// Tx executes fn inside a transaction. If fn returns an error, the transaction
// is rolled back; otherwise it is committed.
func (s *Storage) Tx(fn func(tx *gorm.DB) error) error {
	return s.DB.Transaction(fn)
}
