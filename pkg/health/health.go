package health

import (
	"sync"
	"time"

	"github.com/benbroo/benbroo/pkg/model"
	"github.com/benbroo/benbroo/pkg/storage"
	"github.com/benbroo/benbroo/pkg/subscribe"
	"go.uber.org/zap"
)

// Checker coordinates active probing and passive failure tracking.
type Checker struct {
	instanceStore *storage.InstanceStore
	serviceStore  *storage.ServiceStore
	events        *subscribe.EventBus
	cfg           Config
	logger        *zap.Logger

	// Active check state: consecutive fail/success counts per instance.
	mu            sync.Mutex
	activeFails   map[uint64]int
	activeSuccess map[uint64]int

	// Passive check tracker.
	passive *passiveTracker

	// Active prober.
	prober *activeProber

	stopCh chan struct{}
}

func NewChecker(instStore *storage.InstanceStore, svcStore *storage.ServiceStore, events *subscribe.EventBus, cfg Config, log *zap.Logger) *Checker {
	cfg.applyDefaults()
	return &Checker{
		instanceStore: instStore,
		serviceStore:  svcStore,
		events:        events,
		cfg:           cfg,
		logger:        log,
		activeFails:   make(map[uint64]int),
		activeSuccess: make(map[uint64]int),
		passive:       newPassiveTracker(),
		prober:        newActiveProber(cfg.ActiveTimeout, log),
		stopCh:        make(chan struct{}),
	}
}

// Start begins the periodic active health check loop.
func (c *Checker) Start() {
	go c.activeLoop()
	c.logger.Info("health checker started",
		zap.Int("activeInterval", c.cfg.CheckInterval),
		zap.Int("passiveWindow", c.cfg.PassiveWindow),
		zap.Int("passiveThreshold", c.cfg.PassiveThreshold),
	)
}

// Stop stops the health checker.
func (c *Checker) Stop() {
	close(c.stopCh)
}

// ==================== Active Check Loop ====================

func (c *Checker) activeLoop() {
	ticker := time.NewTicker(time.Duration(c.cfg.CheckInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.runActiveCheck()
		case <-c.stopCh:
			return
		}
	}
}

func (c *Checker) runActiveCheck() {
	// Load all services to get per-service health config.
	services, err := c.serviceStore.ListAll()
	if err != nil {
		c.logger.Error("failed to list services for health check", zap.Error(err))
		return
	}

	// Build a lookup: ns#group#service -> service config.
	svcMap := make(map[string]*model.Service, len(services))
	for i := range services {
		svc := &services[i]
		key := svc.NamespaceID + "#" + svc.GroupName + "#" + svc.ServiceName
		svcMap[key] = svc
	}

	instances, err := c.instanceStore.ListAll()
	if err != nil {
		c.logger.Error("failed to list instances for health check", zap.Error(err))
		return
	}

	for _, inst := range instances {
		if !inst.Ephemeral {
			continue
		}

		// Look up service health config.
		svcKey := inst.NamespaceID + "#" + inst.GroupName + "#" + inst.ServiceName
		svc := svcMap[svcKey]

		checkType := model.HealthCheckActive
		protocol := model.HealthProtoTCP
		path := ""
		checkPort := inst.Port
		if svc != nil {
			checkType = svc.HealthCheckType
			protocol = svc.HealthCheckProto
			path = svc.HealthCheckPath
			if svc.HealthCheckPort > 0 {
				checkPort = svc.HealthCheckPort
			}
		}

		// Skip if health check is disabled or passive-only.
		if checkType == model.HealthCheckNone || checkType == model.HealthCheckPassive {
			continue
		}

		// Check heartbeat timeout first.
		elapsed := time.Since(inst.LastBeat)
		if elapsed > time.Duration(c.cfg.RemoveTimeout)*time.Second {
			if inst.Healthy {
				c.markUnhealthy(inst, "heartbeat timeout")
			}
			continue
		}

		// Perform active probe.
		healthy := c.prober.Probe(protocol, inst.IP, checkPort, path)
		c.processActiveResult(inst, healthy)
	}
}

func (c *Checker) processActiveResult(inst model.ServiceInstance, healthy bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if healthy {
		c.activeFails[inst.ID] = 0
		c.activeSuccess[inst.ID]++

		// Recover if enough consecutive successes.
		if !inst.Healthy && c.activeSuccess[inst.ID] >= c.cfg.RecoveryThreshold {
			c.mu.Unlock()
			c.markHealthy(inst, "active check recovered")
			c.mu.Lock()
		}
	} else {
		c.activeSuccess[inst.ID] = 0
		c.activeFails[inst.ID]++

		if inst.Healthy && c.activeFails[inst.ID] >= c.cfg.FailThreshold {
			c.mu.Unlock()
			c.markUnhealthy(inst, "active check failed (threshold reached)")
			c.mu.Lock()
		}
	}
}

// ==================== Passive Check (Consumer Reports) ====================

// ReportFailure is called by consumers when an instance call fails.
// It records the failure and checks if the passive threshold is exceeded.
func (c *Checker) ReportFailure(instanceID uint64, namespaceID, groupName, serviceName string) {
	c.passive.ReportFailure(instanceID)

	// Look up the service's passive config.
	window := time.Duration(c.cfg.PassiveWindow) * time.Second
	threshold := c.cfg.PassiveThreshold

	if svc, err := c.serviceStore.Get(namespaceID, groupName, serviceName); err == nil {
		if svc.PassiveWindow > 0 {
			window = time.Duration(svc.PassiveWindow) * time.Second
		}
		if svc.PassiveThreshold > 0 {
			threshold = svc.PassiveThreshold
		}
	}

	count := c.passive.FailureCount(instanceID, window)
	if count >= threshold {
		c.logger.Warn("passive check: failure threshold exceeded",
			zap.Uint64("instanceId", instanceID),
			zap.String("service", serviceName),
			zap.Int("failures", count),
			zap.Int("threshold", threshold),
		)
		_ = c.instanceStore.UpdateHealthy(instanceID, false)
		c.events.PublishServiceChange(namespaceID, groupName, serviceName)
	}
}

// ReportSuccess is called by consumers when an instance call succeeds.
// It clears passive failure counters.
func (c *Checker) ReportSuccess(instanceID uint64, namespaceID, groupName, serviceName string) {
	c.passive.ReportSuccess(instanceID)
}

// ==================== Health Status Query ====================

// GetHealthStatus returns the current health check status for a service.
func (c *Checker) GetHealthStatus(namespaceID, groupName, serviceName string) map[string]interface{} {
	instances, _ := c.instanceStore.List(namespaceID, groupName, serviceName)

	total := len(instances)
	healthyCount := 0
	type instStatus struct {
		IP      string `json:"ip"`
		Port    int    `json:"port"`
		Healthy bool   `json:"healthy"`
	}
	instList := make([]instStatus, 0, total)
	for _, inst := range instances {
		if inst.Healthy {
			healthyCount++
		}
		instList = append(instList, instStatus{
			IP: inst.IP, Port: inst.Port, Healthy: inst.Healthy,
		})
	}

	// Get service config.
	svc, _ := c.serviceStore.Get(namespaceID, groupName, serviceName)
	cfgInfo := map[string]interface{}{}
	if svc != nil {
		cfgInfo = map[string]interface{}{
			"checkType":        svc.HealthCheckType,
			"protocol":         svc.HealthCheckProto,
			"path":             svc.HealthCheckPath,
			"activeInterval":   svc.ActiveInterval,
			"passiveWindow":    svc.PassiveWindow,
			"passiveThreshold": svc.PassiveThreshold,
		}
	}

	return map[string]interface{}{
		"service":      serviceName,
		"total":        total,
		"healthy":      healthyCount,
		"unhealthy":    total - healthyCount,
		"healthConfig": cfgInfo,
		"instances":    instList,
	}
}

// ==================== Helpers ====================

func (c *Checker) markUnhealthy(inst model.ServiceInstance, reason string) {
	_ = c.instanceStore.UpdateHealthy(inst.ID, false)
	c.events.PublishServiceChange(inst.NamespaceID, inst.GroupName, inst.ServiceName)
	c.logger.Warn("instance marked unhealthy",
		zap.String("reason", reason),
		zap.String("service", inst.ServiceName),
		zap.String("ip", inst.IP),
		zap.Int("port", inst.Port),
	)
}

func (c *Checker) markHealthy(inst model.ServiceInstance, reason string) {
	_ = c.instanceStore.UpdateHealthy(inst.ID, true)
	c.events.PublishServiceChange(inst.NamespaceID, inst.GroupName, inst.ServiceName)
	c.logger.Info("instance recovered",
		zap.String("reason", reason),
		zap.String("service", inst.ServiceName),
		zap.String("ip", inst.IP),
		zap.Int("port", inst.Port),
	)
}
