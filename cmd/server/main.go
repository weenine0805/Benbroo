package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/benbroo/benbroo/pkg/api"
	"github.com/benbroo/benbroo/pkg/auth"
	"github.com/benbroo/benbroo/pkg/cluster"
	cfgservice "github.com/benbroo/benbroo/pkg/config"
	dnsservice "github.com/benbroo/benbroo/pkg/dns"
	"github.com/benbroo/benbroo/pkg/grpcserver"
	"github.com/benbroo/benbroo/pkg/health"
	"github.com/benbroo/benbroo/pkg/namespace"
	"github.com/benbroo/benbroo/pkg/naming"
	"github.com/benbroo/benbroo/pkg/storage"
	"github.com/benbroo/benbroo/pkg/subscribe"
	"github.com/benbroo/benbroo/pkg/tcpserver"
	"github.com/benbroo/benbroo/web"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

// AppConfig holds the full application configuration.
type AppConfig struct {
	Server struct {
		Port     int    `yaml:"port"`
		GrpcPort int    `yaml:"grpcPort"`
		DnsPort  int    `yaml:"dnsPort"`
		TcpPort  int    `yaml:"tcpPort"`
		Host     string `yaml:"host"`
	} `yaml:"server"`
	MySQL   storage.Config `yaml:"mysql"`
	Cluster cluster.Config `yaml:"cluster"`
	Health  health.Config  `yaml:"health"`
	Auth    auth.Config    `yaml:"auth"`
	Log     struct {
		Level  string `yaml:"level"`
		Output string `yaml:"output"`
	} `yaml:"log"`
}

func main() {
	// Load config.
	cfg := loadConfig("configs/server.yaml")

	// Initialize logger.
	logger := initLogger(cfg.Log.Level)
	defer logger.Sync()

	logger.Info("starting Benbroo server",
		zap.Int("port", cfg.Server.Port),
		zap.String("host", cfg.Server.Host),
	)

	// Connect to MySQL.
	store, err := storage.New(cfg.MySQL, logger)
	if err != nil {
		logger.Fatal("failed to connect to MySQL", zap.Error(err))
	}

	// Create stores.
	instStore := storage.NewInstanceStore(store.DB)
	svcStore := storage.NewServiceStore(store.DB)
	cfgStore := storage.NewConfigStore(store.DB)
	nsStore := storage.NewNamespaceStore(store.DB)
	nodeStore := storage.NewClusterNodeStore(store.DB)

	// Event bus.
	events := subscribe.NewEventBus()

	// Services.
	namingSvc := naming.NewService(instStore, svcStore, events, logger)
	configSvc := cfgservice.NewService(cfgStore, events, logger)
	nsSvc := namespace.NewService(nsStore, logger)
	clusterMgr := cluster.NewManager(cfg.Cluster, nodeStore, logger)

	// Initialize default namespace.
	if err := nsSvc.InitDefault(); err != nil {
		logger.Warn("init default namespace", zap.Error(err))
	}

	// Start health checker.
	healthChecker := health.NewChecker(instStore, svcStore, events, cfg.Health, logger)
	healthChecker.Start()

	// Start cluster manager.
	clusterMgr.Start()

	// Initialize auth manager.
	authMgr := auth.NewManager(cfg.Auth)

	// Setup Gin.
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), gin.Logger())

	// Register API routes (with auth middleware).
	server := api.NewServer(
		namingSvc, configSvc, nsSvc, clusterMgr, healthChecker,
		events, instStore, svcStore, cfgStore, authMgr, logger,
	)
	server.RegisterRoutes(r)

	// Register web console.
	web.Register(r)

	// Start cluster syncer.
	syncer := cluster.NewSyncer(clusterMgr, logger)
	syncer.Start()
	server.SetSyncer(syncer)

	// Start DNS server.
	dnsPort := cfg.Server.DnsPort
	if dnsPort == 0 {
		dnsPort = 8553
	}
	dnsSrv := dnsservice.NewServer(dnsservice.Config{Port: dnsPort}, namingSvc, logger)
	if err := dnsSrv.Start(); err != nil {
		logger.Warn("DNS server start failed", zap.Error(err))
	}

	// Start TCP socket server.
	tcpPort := cfg.Server.TcpPort
	if tcpPort == 0 {
		tcpPort = cfg.Server.Port - 2000 // default: HTTP port - 2000 = 6848
	}
	tcpAddr := fmt.Sprintf("%s:%d", cfg.Server.Host, tcpPort)
	tcpSrv := tcpserver.NewServer(tcpAddr, namingSvc, configSvc, healthChecker, instStore, logger)
	if err := tcpSrv.Start(); err != nil {
		logger.Warn("TCP server start failed", zap.Error(err))
	}

	// Start gRPC server.
	grpcPort := cfg.Server.GrpcPort
	if grpcPort == 0 {
		grpcPort = cfg.Server.Port + 1000 // default: HTTP port + 1000
	}
	grpcAddr := fmt.Sprintf("%s:%d", cfg.Server.Host, grpcPort)
	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Fatal("failed to listen gRPC", zap.Error(err))
	}
	grpcSrv := grpc.NewServer()
	grpcImpl := grpcserver.NewServer(namingSvc, configSvc, healthChecker, instStore, logger)
	grpcImpl.Register(grpcSrv)
	go func() {
		logger.Info("gRPC server listening", zap.String("addr", grpcAddr))
		if err := grpcSrv.Serve(grpcLis); err != nil {
			logger.Fatal("gRPC server error", zap.Error(err))
		}
	}()

	// Start HTTP server.
	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	go func() {
		logger.Info("HTTP server listening", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server error", zap.Error(err))
		}
	}()

	// Graceful shutdown.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down server...")
	healthChecker.Stop()
	clusterMgr.Stop()
	syncer.Stop()
	dnsSrv.Stop()
	tcpSrv.Stop()
	grpcSrv.GracefulStop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("server forced to shutdown", zap.Error(err))
	}
	logger.Info("server exited")
}

func loadConfig(path string) *AppConfig {
	data, err := os.ReadFile(path)
	if err != nil {
		// Use defaults if config file not found.
		cfg := &AppConfig{}
		cfg.Server.Port = 8848
		cfg.Server.GrpcPort = 9848
		cfg.Server.DnsPort = 8553
		cfg.Server.TcpPort = 6848
		cfg.Server.Host = "0.0.0.0"
		cfg.MySQL.Host = "127.0.0.1"
		cfg.MySQL.Port = 3306
		cfg.MySQL.Username = "root"
		cfg.MySQL.Password = "root"
		cfg.MySQL.Database = "benbroo"
		cfg.MySQL.Charset = "utf8mb4"
		cfg.MySQL.MaxIdleConns = 10
		cfg.MySQL.MaxOpenConns = 100
		cfg.Cluster.SelfAddr = "127.0.0.1:8848"
		cfg.Cluster.Members = []string{"127.0.0.1:8848"}
		cfg.Health.CheckInterval = 5
		cfg.Health.FailThreshold = 3
		cfg.Health.RemoveTimeout = 30
		cfg.Health.PassiveWindow = 60
		cfg.Health.PassiveThreshold = 5
		cfg.Health.RecoveryThreshold = 3
		cfg.Health.ActiveTimeout = 3
		cfg.Auth = auth.DefaultConfig()
		cfg.Log.Level = "info"
		return cfg
	}
	var cfg AppConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		panic("invalid config: " + err.Error())
	}
	return &cfg
}

func initLogger(level string) *zap.Logger {
	var cfg zap.Config
	if level == "debug" {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}
	logger, err := cfg.Build()
	if err != nil {
		panic("failed to init logger: " + err.Error())
	}
	return logger
}
