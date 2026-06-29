package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/benbroo/benbroo/pkg/auth"
	"github.com/benbroo/benbroo/pkg/cluster"
	cfgservice "github.com/benbroo/benbroo/pkg/config"
	"github.com/benbroo/benbroo/pkg/health"
	"github.com/benbroo/benbroo/pkg/model"
	nsservice "github.com/benbroo/benbroo/pkg/namespace"
	"github.com/benbroo/benbroo/pkg/naming"
	"github.com/benbroo/benbroo/pkg/storage"
	"github.com/benbroo/benbroo/pkg/subscribe"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Server holds all API dependencies.
type Server struct {
	namingSvc    *naming.Service
	configSvc    *cfgservice.Service
	namespaceSvc *nsservice.Service
	clusterMgr   *cluster.Manager
	syncer       *cluster.Syncer
	healthChk    *health.Checker
	events       *subscribe.EventBus
	instStore    *storage.InstanceStore
	svcStore     *storage.ServiceStore
	cfgStore     *storage.ConfigStore
	authMgr      *auth.Manager
	logger       *zap.Logger
}

func NewServer(
	namingSvc *naming.Service,
	configSvc *cfgservice.Service,
	namespaceSvc *nsservice.Service,
	clusterMgr *cluster.Manager,
	healthChk *health.Checker,
	events *subscribe.EventBus,
	instStore *storage.InstanceStore,
	svcStore *storage.ServiceStore,
	cfgStore *storage.ConfigStore,
	authMgr *auth.Manager,
	log *zap.Logger,
) *Server {
	return &Server{
		namingSvc:    namingSvc,
		configSvc:    configSvc,
		namespaceSvc: namespaceSvc,
		clusterMgr:   clusterMgr,
		healthChk:    healthChk,
		events:       events,
		instStore:    instStore,
		svcStore:     svcStore,
		cfgStore:     cfgStore,
		authMgr:      authMgr,
		logger:       log,
	}
}

// SetSyncer sets the cluster syncer for cross-node replication notifications.
func (s *Server) SetSyncer(syncer *cluster.Syncer) {
	s.syncer = syncer
}

// RegisterRoutes sets up all API routes.
func (s *Server) RegisterRoutes(r *gin.Engine) {
	v1 := r.Group("/v1")
	{
		// Naming service APIs
		ns := v1.Group("/ns")
		{
			ns.POST("/instance", s.registerInstance)
			ns.DELETE("/instance", s.deregisterInstance)
			ns.PUT("/instance", s.updateInstance)
			ns.GET("/instance", s.getInstance)
			ns.GET("/instance/list", s.getInstanceList)
			ns.PUT("/instance/beat", s.heartbeat)
			ns.POST("/service", s.createService)
			ns.GET("/service", s.getService)
			ns.GET("/service/list", s.listServices)
			ns.GET("/serverlist", s.getServerList)

			// Health check APIs
			ns.PUT("/health/config", s.updateHealthConfig)       // Provider: configure health check
			ns.GET("/health/status", s.getHealthStatus)          // Query health status
			ns.POST("/health/instance/fail", s.reportFail)       // Consumer: report failure
			ns.POST("/health/instance/succeed", s.reportSucceed) // Consumer: report success
		}

		// Config service APIs
		cs := v1.Group("/cs")
		{
			cs.GET("/configs", s.getConfig)
			cs.POST("/configs", s.publishConfig)
			cs.DELETE("/configs", s.deleteConfig)
			cs.GET("/configs/list", s.listConfigs)
			cs.GET("/configs/history", s.configHistory)
			cs.POST("/configs/listener", s.configListener)
		}

		// Console APIs
		console := v1.Group("/console")
		{
			console.GET("/namespaces", s.listNamespaces)
			console.POST("/namespaces", s.createNamespace)
			console.PUT("/namespaces", s.updateNamespace)
			console.DELETE("/namespaces", s.deleteNamespace)
			console.GET("/dashboard", s.dashboard)
			console.GET("/subscribers", s.listSubscribers)
		}

		// Cluster sync API (internal, between nodes)
		v1.POST("/cluster/sync", s.clusterSync)
	}
}

// ==================== Naming Service Handlers ====================

func (s *Server) registerInstance(c *gin.Context) {
	port, _ := strconv.Atoi(c.DefaultPostForm("port", "0"))
	weight, _ := strconv.ParseFloat(c.DefaultPostForm("weight", "1.0"), 64)
	ephemeral, _ := strconv.ParseBool(c.DefaultPostForm("ephemeral", "true"))

	inst := &model.ServiceInstance{
		NamespaceID: c.DefaultPostForm("namespaceId", "public"),
		GroupName:   c.DefaultPostForm("groupName", "DEFAULT_GROUP"),
		ServiceName: c.PostForm("serviceName"),
		ClusterName: c.DefaultPostForm("clusterName", "DEFAULT"),
		IP:          c.PostForm("ip"),
		Port:        port,
		Weight:      weight,
		Ephemeral:   ephemeral,
		Metadata:    c.DefaultPostForm("metadata", "{}"),
	}
	if err := s.namingSvc.RegisterInstance(inst); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": err.Error()})
		return
	}
	if s.syncer != nil {
		s.syncer.NotifyServiceChange(inst.NamespaceID, inst.GroupName, inst.ServiceName)
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok"})
}

func (s *Server) deregisterInstance(c *gin.Context) {
	port, _ := strconv.Atoi(c.DefaultQuery("port", "0"))
	err := s.namingSvc.DeregisterInstance(
		c.DefaultQuery("namespaceId", "public"),
		c.DefaultQuery("groupName", "DEFAULT_GROUP"),
		c.Query("serviceName"),
		c.DefaultQuery("clusterName", "DEFAULT"),
		c.Query("ip"),
		port,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	if s.syncer != nil {
		s.syncer.NotifyServiceChange(
			c.DefaultQuery("namespaceId", "public"),
			c.DefaultQuery("groupName", "DEFAULT_GROUP"),
			c.Query("serviceName"),
		)
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok"})
}

func (s *Server) updateInstance(c *gin.Context) {
	port, _ := strconv.Atoi(c.DefaultPostForm("port", "0"))
	weight, _ := strconv.ParseFloat(c.DefaultPostForm("weight", "1.0"), 64)
	enabled, _ := strconv.ParseBool(c.DefaultPostForm("enabled", "true"))

	inst := &model.ServiceInstance{
		NamespaceID: c.DefaultPostForm("namespaceId", "public"),
		GroupName:   c.DefaultPostForm("groupName", "DEFAULT_GROUP"),
		ServiceName: c.PostForm("serviceName"),
		ClusterName: c.DefaultPostForm("clusterName", "DEFAULT"),
		IP:          c.PostForm("ip"),
		Port:        port,
		Weight:      weight,
		Enabled:     enabled,
		Metadata:    c.DefaultPostForm("metadata", "{}"),
	}
	if err := s.namingSvc.UpdateInstance(inst); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok"})
}

func (s *Server) getInstance(c *gin.Context) {
	port, _ := strconv.Atoi(c.DefaultQuery("port", "0"))
	inst, err := s.namingSvc.GetInstance(
		c.DefaultQuery("namespaceId", "public"),
		c.DefaultQuery("groupName", "DEFAULT_GROUP"),
		c.Query("serviceName"),
		c.DefaultQuery("clusterName", "DEFAULT"),
		c.Query("ip"),
		port,
	)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"code": 404, "message": "instance not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": inst})
}

func (s *Server) getInstanceList(c *gin.Context) {
	healthyOnly, _ := strconv.ParseBool(c.DefaultQuery("healthyOnly", "false"))
	clusters := naming.ParseClusters(c.DefaultQuery("clusters", ""))
	instances, err := s.namingSvc.GetInstances(
		c.DefaultQuery("namespaceId", "public"),
		c.DefaultQuery("groupName", "DEFAULT_GROUP"),
		c.Query("serviceName"),
		clusters,
		healthyOnly,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "hosts": instances, "count": len(instances)})
}

func (s *Server) heartbeat(c *gin.Context) {
	port, _ := strconv.Atoi(c.DefaultPostForm("port", "0"))
	err := s.namingSvc.Heartbeat(
		c.DefaultPostForm("namespaceId", "public"),
		c.DefaultPostForm("groupName", "DEFAULT_GROUP"),
		c.PostForm("serviceName"),
		c.DefaultPostForm("clusterName", "DEFAULT"),
		c.PostForm("ip"),
		port,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok"})
}

func (s *Server) createService(c *gin.Context) {
	protectThreshold, _ := strconv.ParseFloat(c.DefaultPostForm("protectThreshold", "0"), 64)
	activeInterval, _ := strconv.Atoi(c.DefaultPostForm("activeInterval", "5"))
	passiveWindow, _ := strconv.Atoi(c.DefaultPostForm("passiveWindow", "60"))
	passiveThreshold, _ := strconv.Atoi(c.DefaultPostForm("passiveThreshold", "5"))
	healthCheckPort, _ := strconv.Atoi(c.DefaultPostForm("healthCheckPort", "0"))

	svc := &model.Service{
		NamespaceID:      c.DefaultPostForm("namespaceId", "public"),
		GroupName:        c.DefaultPostForm("groupName", "DEFAULT_GROUP"),
		ServiceName:      c.PostForm("serviceName"),
		ProtectThreshold: protectThreshold,
		Metadata:         c.DefaultPostForm("metadata", "{}"),
		HealthCheckType:  c.DefaultPostForm("healthCheckType", "ACTIVE"),
		HealthCheckProto: c.DefaultPostForm("healthCheckProto", "TCP"),
		HealthCheckPath:  c.DefaultPostForm("healthCheckPath", ""),
		HealthCheckPort:  healthCheckPort,
		ActiveInterval:   activeInterval,
		PassiveWindow:    passiveWindow,
		PassiveThreshold: passiveThreshold,
	}
	if err := s.namingSvc.CreateService(svc); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok"})
}

func (s *Server) getService(c *gin.Context) {
	svc, instances, err := s.namingSvc.GetService(
		c.DefaultQuery("namespaceId", "public"),
		c.DefaultQuery("groupName", "DEFAULT_GROUP"),
		c.Query("serviceName"),
	)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"code": 404, "message": "service not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": gin.H{
			"service":   svc,
			"hosts":     instances,
			"hostCount": len(instances),
		},
	})
}

func (s *Server) listServices(c *gin.Context) {
	pageNo, _ := strconv.Atoi(c.DefaultQuery("pageNo", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("pageSize", "20"))
	list, total, err := s.namingSvc.ListServices(
		c.DefaultQuery("namespaceId", "public"),
		c.DefaultQuery("groupName", ""),
		pageNo, pageSize,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}

	// Attach instance counts to each service.
	counts, _ := s.instStore.CountGrouped()
	type svcWithCount struct {
		model.Service
		InstanceCount int `json:"instanceCount"`
		HealthyCount  int `json:"healthyCount"`
	}
	enriched := make([]svcWithCount, 0, len(list))
	for _, svc := range list {
		key := svc.NamespaceID + "#" + svc.GroupName + "#" + svc.ServiceName
		ic := counts[key] // zero-value if not found
		enriched = append(enriched, svcWithCount{
			Service:       svc,
			InstanceCount: ic.Total,
			HealthyCount:  ic.Healthy,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"code":  0,
		"count": total,
		"doms":  enriched,
	})
}

func (s *Server) getServerList(c *gin.Context) {
	nodes := s.clusterMgr.GetNodes()
	c.JSON(http.StatusOK, gin.H{"code": 0, "servers": nodes})
}

// ==================== Config Service Handlers ====================

func (s *Server) getConfig(c *gin.Context) {
	item, err := s.configSvc.GetConfig(
		c.DefaultQuery("tenant", "public"),
		c.DefaultQuery("group", "DEFAULT_GROUP"),
		c.Query("dataId"),
	)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"code": 404, "message": "config not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": item})
}

func (s *Server) publishConfig(c *gin.Context) {
	tenant := c.DefaultPostForm("tenant", "public")
	group := c.DefaultPostForm("group", "DEFAULT_GROUP")
	dataID := c.PostForm("dataId")
	err := s.configSvc.PublishConfig(
		tenant, group, dataID,
		c.PostForm("content"),
		c.DefaultPostForm("type", "text"),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	if s.syncer != nil {
		s.syncer.NotifyConfigChange(tenant, group, dataID)
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok", "data": true})
}

func (s *Server) deleteConfig(c *gin.Context) {
	tenant := c.DefaultQuery("tenant", "public")
	group := c.DefaultQuery("group", "DEFAULT_GROUP")
	dataID := c.Query("dataId")
	err := s.configSvc.DeleteConfig(tenant, group, dataID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	if s.syncer != nil {
		s.syncer.NotifyConfigChange(tenant, group, dataID)
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok", "data": true})
}

func (s *Server) listConfigs(c *gin.Context) {
	pageNo, _ := strconv.Atoi(c.DefaultQuery("pageNo", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("pageSize", "20"))
	list, total, err := s.configSvc.ListConfigs(
		c.DefaultQuery("tenant", "public"),
		c.DefaultQuery("group", ""),
		c.DefaultQuery("dataId", ""),
		pageNo, pageSize,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"code":       0,
		"totalCount": total,
		"pageItems":  list,
	})
}

func (s *Server) configHistory(c *gin.Context) {
	pageNo, _ := strconv.Atoi(c.DefaultQuery("pageNo", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("pageSize", "20"))
	list, total, err := s.configSvc.ConfigHistory(
		c.DefaultQuery("tenant", "public"),
		c.DefaultQuery("group", "DEFAULT_GROUP"),
		c.Query("dataId"),
		pageNo, pageSize,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"code":       0,
		"totalCount": total,
		"pageItems":  list,
	})
}

// configListener implements long-polling for config changes.
// Client sends a body with "Listening-Configs" in the format:
// dataId\x02group\x02md5\x02tenant\x01  (repeated)
func (s *Server) configListener(c *gin.Context) {
	listeningConfigs := c.PostForm("Listening-Configs")
	if listeningConfigs == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "Listening-Configs is required"})
		return
	}

	// Parse listening configs.
	clientMD5 := make(map[string]string)
	namespaceID := c.DefaultPostForm("tenant", "public")

	// Simple parsing: each entry is "dataId\x02group\x02md5\x02tenant\x01"
	entries := splitListeningConfigs(listeningConfigs)
	for _, entry := range entries {
		if len(entry) >= 3 {
			key := entry[3] + "#" + entry[1] + "#" + entry[0] // tenant#group#dataId
			clientMD5[key] = entry[2]
			if entry[3] != "" {
				namespaceID = entry[3]
			}
		}
	}

	// Long poll with 30 second timeout.
	changed, err := s.configSvc.LongPoll(namespaceID, clientMD5, 30*time.Second)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	if changed == nil {
		c.JSON(http.StatusOK, gin.H{"code": 0, "data": ""})
		return
	}

	// Format response.
	result := ""
	for key, md5 := range changed {
		result += key + "\x02" + md5 + "\x02\n"
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": result})
}

func splitListeningConfigs(input string) [][]string {
	var result [][]string
	entries := split(input, "\x01")
	for _, entry := range entries {
		entry = trimSpace(entry)
		if entry == "" {
			continue
		}
		parts := split(entry, "\x02")
		if len(parts) >= 3 {
			result = append(result, parts)
		}
	}
	return result
}

func split(s, sep string) []string {
	var result []string
	for {
		i := indexOf(s, sep)
		if i < 0 {
			if s != "" {
				result = append(result, s)
			}
			break
		}
		result = append(result, s[:i])
		s = s[i+len(sep):]
	}
	return result
}

func indexOf(s, sub string) int {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func trimSpace(s string) string {
	start := 0
	for start < len(s) && (s[start] == ' ' || s[start] == '\n' || s[start] == '\r' || s[start] == '\t') {
		start++
	}
	end := len(s)
	for end > start && (s[end-1] == ' ' || s[end-1] == '\n' || s[end-1] == '\r' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}

// ==================== Namespace Handlers ====================

func (s *Server) listNamespaces(c *gin.Context) {
	list, err := s.namespaceSvc.List()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": list})
}

func (s *Server) createNamespace(c *gin.Context) {
	err := s.namespaceSvc.Create(
		c.PostForm("customNamespaceId"),
		c.PostForm("namespaceName"),
		c.DefaultPostForm("namespaceDesc", ""),
	)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok", "data": true})
}

func (s *Server) updateNamespace(c *gin.Context) {
	err := s.namespaceSvc.Update(
		c.PostForm("namespace"),
		c.PostForm("namespaceShowName"),
		c.DefaultPostForm("namespaceDesc", ""),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok", "data": true})
}

func (s *Server) deleteNamespace(c *gin.Context) {
	err := s.namespaceSvc.Delete(c.Query("namespaceId"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok", "data": true})
}

// ==================== Dashboard Handler ====================

func (s *Server) dashboard(c *gin.Context) {
	svcCount, _ := s.svcStore.Count()
	instCount, _ := s.instStore.Count()
	cfgCount, _ := s.cfgStore.Count()
	nodes := s.clusterMgr.GetNodes()

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": gin.H{
			"serviceCount":  svcCount,
			"instanceCount": instCount,
			"configCount":   cfgCount,
			"clusterNodes":  nodes,
			"isLeader":      s.clusterMgr.IsLeader(),
			"selfAddr":      s.clusterMgr.SelfAddr(),
		},
	})
}

// ==================== Health Check Handlers ====================

// param returns a value from form body or query string (supports both PUT and POST).
func param(c *gin.Context, key string) string {
	if v := c.PostForm(key); v != "" {
		return v
	}
	return c.Query(key)
}

func paramDefault(c *gin.Context, key, def string) string {
	if v := c.DefaultPostForm(key, ""); v != "" {
		return v
	}
	return c.DefaultQuery(key, def)
}

// updateHealthConfig updates health check configuration for a service (provider side).
func (s *Server) updateHealthConfig(c *gin.Context) {
	namespaceID := paramDefault(c, "namespaceId", "public")
	groupName := paramDefault(c, "groupName", "DEFAULT_GROUP")
	serviceName := param(c, "serviceName")
	if serviceName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "serviceName is required"})
		return
	}

	cfg := make(map[string]interface{})
	if v := param(c, "healthCheckType"); v != "" {
		cfg["health_check_type"] = v
	}
	if v := param(c, "healthCheckProto"); v != "" {
		cfg["health_check_proto"] = v
	}
	if v := param(c, "healthCheckPath"); v != "" {
		cfg["health_check_path"] = v
	}
	if v := param(c, "healthCheckPort"); v != "" {
		port, _ := strconv.Atoi(v)
		cfg["health_check_port"] = port
	}
	if v := param(c, "activeInterval"); v != "" {
		val, _ := strconv.Atoi(v)
		cfg["active_interval"] = val
	}
	if v := param(c, "passiveWindow"); v != "" {
		val, _ := strconv.Atoi(v)
		cfg["passive_window"] = val
	}
	if v := param(c, "passiveThreshold"); v != "" {
		val, _ := strconv.Atoi(v)
		cfg["passive_threshold"] = val
	}

	if len(cfg) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "no health config fields provided"})
		return
	}

	if err := s.svcStore.UpdateHealthConfig(namespaceID, groupName, serviceName, cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok"})
}

// getHealthStatus returns the health status for a service.
func (s *Server) getHealthStatus(c *gin.Context) {
	status := s.healthChk.GetHealthStatus(
		c.DefaultQuery("namespaceId", "public"),
		c.DefaultQuery("groupName", "DEFAULT_GROUP"),
		c.Query("serviceName"),
	)
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": status})
}

// reportFail is called by consumers when a call to an instance fails (passive check).
func (s *Server) reportFail(c *gin.Context) {
	instanceID, _ := strconv.ParseUint(param(c, "instanceId"), 10, 64)
	serviceName := param(c, "serviceName")
	nsID := paramDefault(c, "namespaceId", "public")
	grpName := paramDefault(c, "groupName", "DEFAULT_GROUP")

	if instanceID == 0 {
		ip := param(c, "ip")
		port, _ := strconv.Atoi(param(c, "port"))
		inst, err := s.instStore.Get(nsID, grpName, serviceName, paramDefault(c, "clusterName", "DEFAULT"), ip, port)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"code": 404, "message": "instance not found"})
			return
		}
		instanceID = inst.ID
	}

	s.healthChk.ReportFailure(instanceID, nsID, grpName, serviceName)
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok"})
}

// reportSucceed is called by consumers when a call to an instance succeeds (passive check).
func (s *Server) reportSucceed(c *gin.Context) {
	instanceID, _ := strconv.ParseUint(param(c, "instanceId"), 10, 64)
	serviceName := param(c, "serviceName")
	nsID := paramDefault(c, "namespaceId", "public")
	grpName := paramDefault(c, "groupName", "DEFAULT_GROUP")

	if instanceID == 0 {
		ip := param(c, "ip")
		port, _ := strconv.Atoi(param(c, "port"))
		inst, err := s.instStore.Get(nsID, grpName, serviceName, paramDefault(c, "clusterName", "DEFAULT"), ip, port)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"code": 404, "message": "instance not found"})
			return
		}
		instanceID = inst.ID
	}

	s.healthChk.ReportSuccess(instanceID, nsID, grpName, serviceName)
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok"})
}

// ==================== Subscriber Handler ====================

func (s *Server) listSubscribers(c *gin.Context) {
	list := s.events.GetServiceSubscribers()
	if list == nil {
		list = []subscribe.SubscriberInfo{}
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": list})
}

// ==================== Cluster Sync Handler ====================

// clusterSync receives a sync event from a peer node.
// The receiving node logs the event and can trigger a local refresh.
// Since all nodes share the same MySQL database, the data is already
// consistent — this notification is used for cache invalidation / event
// propagation to local subscribers.
func (s *Server) clusterSync(c *gin.Context) {
	var event cluster.SyncEvent
	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": err.Error()})
		return
	}
	s.logger.Debug("cluster sync: received event",
		zap.String("type", event.Type),
		zap.String("source", event.Source),
		zap.String("service", event.Service),
		zap.String("dataId", event.DataID),
	)
	// Propagate the event to the local event bus so subscribers are notified.
	switch event.Type {
	case "service_change":
		s.events.PublishServiceChange(event.Namespace, event.Group, event.Service)
	case "config_change":
		s.events.PublishConfigChange(event.Namespace, event.Group, event.DataID)
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "ok"})
}
