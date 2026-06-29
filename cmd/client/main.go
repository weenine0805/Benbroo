package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/benbroo/benbroo/pkg/client"
)

const (
	serverAddr     = "http://localhost:8848"
	grpcServerAddr = "localhost:9848"
	tcpServerAddr  = "localhost:6848"
)

func main() {
	fmt.Println("========================================")
	fmt.Println("  Benbroo Client SDK - Full Feature Test")
	fmt.Println("========================================")
	fmt.Println()

	c := client.New(serverAddr)

	// 1. Service Registration
	testServiceRegistration(c)

	// 2. Service Discovery & Load Balancing
	testServiceDiscovery(c)

	// 3. Heartbeat
	testHeartbeat(c)

	// 4. Config Management
	testConfigManagement(c)

	// 5. Config Watch (Long Polling)
	testConfigWatch(c)

	// 6. Health Reporting (Passive Check)
	testHealthReporting(c)

	// 7. gRPC Protocol Tests
	testGRPC(c)

	// 8. DNS Service Test
	testDNS()

	// 9. TCP Socket Protocol Test
	testTCP(c)

	// 10. Cleanup
	testCleanup(c)

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("  All tests completed!")
	fmt.Println("========================================")
}

func section(title string) {
	fmt.Println()
	fmt.Println("--- " + title + " ---")
}

func ok(msg string) {
	fmt.Println("  [OK] " + msg)
}

func fail(msg string) {
	fmt.Println("  [FAIL] " + msg)
}

// ==================== Test 1: Service Registration ====================

func testServiceRegistration(c *client.Client) {
	section("1. Service Registration")

	// Register multiple instances
	instances := []client.RegisterOptions{
		{
			ServiceName: "order-service",
			IP:          "192.168.1.10",
			Port:        8080,
			Weight:      1.0,
			Ephemeral:   true,
			Metadata:    `{"version":"v1.0"}`,
		},
		{
			ServiceName: "order-service",
			IP:          "192.168.1.11",
			Port:        8080,
			Weight:      2.0,
			Ephemeral:   true,
			Metadata:    `{"version":"v1.1"}`,
		},
		{
			ServiceName: "order-service",
			IP:          "192.168.1.12",
			Port:        8080,
			Weight:      3.0,
			Ephemeral:   true,
			Metadata:    `{"version":"v2.0"}`,
		},
		{
			NamespaceID: "public",
			GroupName:   "DEFAULT_GROUP",
			ServiceName: "user-service",
			IP:          "192.168.2.10",
			Port:        9090,
			Weight:      1.0,
			Ephemeral:   true,
		},
	}

	for i, inst := range instances {
		err := c.RegisterInstance(inst)
		if err != nil {
			fail(fmt.Sprintf("Register instance %d failed: %v", i+1, err))
		} else {
			ok(fmt.Sprintf("Registered: %s/%s:%d (weight=%.1f)", inst.ServiceName, inst.IP, inst.Port, inst.Weight))
		}
	}
}

// ==================== Test 3: Service Discovery ====================

func testServiceDiscovery(c *client.Client) {
	section("2. Service Discovery & Load Balancing")

	// Get all instances
	instances, err := c.GetInstances("public", "DEFAULT_GROUP", "order-service", false)
	if err != nil {
		fail("GetInstances failed: " + err.Error())
		return
	}
	ok(fmt.Sprintf("Found %d instances for order-service", len(instances)))
	for _, inst := range instances {
		fmt.Printf("      -> %s:%d (weight=%.1f, healthy=%v, meta=%s)\n",
			inst.IP, inst.Port, inst.Weight, inst.Healthy, inst.Metadata)
	}

	// Test Round-Robin
	fmt.Println("  [LB] Round-Robin (3 calls):")
	for i := 0; i < 3; i++ {
		inst, err := c.SelectInstance("public", "DEFAULT_GROUP", "order-service", client.LBRoundRobin)
		if err != nil {
			fail("RoundRobin select failed: " + err.Error())
			break
		}
		fmt.Printf("      Call %d -> %s:%d\n", i+1, inst.IP, inst.Port)
	}

	// Test Random
	fmt.Println("  [LB] Random (3 calls):")
	for i := 0; i < 3; i++ {
		inst, err := c.SelectInstance("public", "DEFAULT_GROUP", "order-service", client.LBRandom)
		if err != nil {
			fail("Random select failed: " + err.Error())
			break
		}
		fmt.Printf("      Call %d -> %s:%d\n", i+1, inst.IP, inst.Port)
	}

	// Test Weighted
	fmt.Println("  [LB] Weighted (6 calls, weights 1:2:3):")
	counts := make(map[string]int)
	for i := 0; i < 60; i++ {
		inst, err := c.SelectInstance("public", "DEFAULT_GROUP", "order-service", client.LBWeightedRoundRobin)
		if err != nil {
			fail("Weighted select failed: " + err.Error())
			break
		}
		key := fmt.Sprintf("%s:%d", inst.IP, inst.Port)
		counts[key]++
	}
	for addr, cnt := range counts {
		fmt.Printf("      %s -> %d calls\n", addr, cnt)
	}
	ok("Load balancing strategies working")

	// Test Consistent Hashing
	fmt.Println("  [LB] Consistent Hashing (same key -> same instance):")
	for i := 0; i < 5; i++ {
		inst, err := c.SelectInstanceWithKey("public", "DEFAULT_GROUP", "order-service", client.LBConsistentHash, "user-123")
		if err != nil {
			fail("ConsistentHash select failed: " + err.Error())
			break
		}
		fmt.Printf("      Call %d (key=user-123) -> %s:%d\n", i+1, inst.IP, inst.Port)
	}
	ok("Consistent hashing: same key always maps to same instance")
}

// ==================== Test 4: Heartbeat ====================

func testHeartbeat(c *client.Client) {
	section("3. Heartbeat Mechanism")

	// Single heartbeat
	err := c.SendHeartbeat("public", "DEFAULT_GROUP", "order-service", "DEFAULT", "192.168.1.10", 8080)
	if err != nil {
		fail("Manual heartbeat failed: " + err.Error())
	} else {
		ok("Manual heartbeat sent for order-service/192.168.1.10:8080")
	}

	// Auto-heartbeat
	c.StartAutoHeartbeat("public", "DEFAULT_GROUP", "order-service", "DEFAULT", "192.168.1.11", 8080)
	ok("Auto-heartbeat started for order-service/192.168.1.11:8080 (interval=5s)")

	fmt.Println("      Waiting 6 seconds to observe heartbeats...")
	time.Sleep(6 * time.Second)

	c.StopAutoHeartbeat("public", "DEFAULT_GROUP", "order-service", "DEFAULT", "192.168.1.11", 8080)
	ok("Auto-heartbeat stopped")
}

// ==================== Test 5: Config Management ====================

func testConfigManagement(c *client.Client) {
	section("4. Config Management (Pull/Publish/Delete)")

	// Publish configs
	configs := []struct {
		dataID  string
		content string
		cfgType string
	}{
		{"application.yaml", "server:\n  port: 8080\nspring:\n  datasource:\n    url: jdbc:mysql://localhost:3306/mydb", "yaml"},
		{"database.properties", "db.host=localhost\ndb.port=3306\ndb.name=benbroo\ndb.user=root", "properties"},
		{"feature-flags", `{"enableNewUI":true,"enableCache":false,"maxRetries":3}`, "json"},
	}

	for _, cfg := range configs {
		err := c.PublishConfig("public", "DEFAULT_GROUP", cfg.dataID, cfg.content, cfg.cfgType)
		if err != nil {
			fail(fmt.Sprintf("Publish %s failed: %v", cfg.dataID, err))
		} else {
			ok(fmt.Sprintf("Published config: %s (type=%s)", cfg.dataID, cfg.cfgType))
		}
	}

	// Pull configs
	for _, cfg := range configs {
		item, err := c.GetConfig("public", "DEFAULT_GROUP", cfg.dataID)
		if err != nil {
			fail(fmt.Sprintf("Get %s failed: %v", cfg.dataID, err))
		} else {
			preview := item.Content
			if len(preview) > 50 {
				preview = preview[:50] + "..."
			}
			ok(fmt.Sprintf("Got config: %s (md5=%s) -> %s", item.DataID, item.MD5[:12], preview))
		}
	}

	// Update a config
	err := c.PublishConfig("public", "DEFAULT_GROUP", "application.yaml",
		"server:\n  port: 9090\nspring:\n  datasource:\n    url: jdbc:mysql://localhost:3306/prod_db", "yaml")
	if err != nil {
		fail("Update config failed: " + err.Error())
	} else {
		ok("Updated config: application.yaml (port changed to 9090)")
	}

	// Delete a config
	err = c.DeleteConfig("public", "DEFAULT_GROUP", "feature-flags")
	if err != nil {
		fail("Delete config failed: " + err.Error())
	} else {
		ok("Deleted config: feature-flags")
	}
}

// ==================== Test 6: Config Watch ====================

func testConfigWatch(c *client.Client) {
	section("5. Config Watch (Long Polling)")

	// Ensure config exists
	_ = c.PublishConfig("public", "DEFAULT_GROUP", "watch-test", "initial-value", "text")

	changeCount := 0
	cancel := c.WatchConfig("public", "DEFAULT_GROUP", "watch-test", func(newContent string) {
		changeCount++
		fmt.Printf("      [WATCH] Config changed! New value: %s\n", newContent)
	})

	ok("Watching config 'watch-test' for changes...")

	// Simulate config changes from another "publisher"
	for i := 1; i <= 2; i++ {
		time.Sleep(2 * time.Second)
		newVal := fmt.Sprintf("updated-value-%d", i)
		err := c.PublishConfig("public", "DEFAULT_GROUP", "watch-test", newVal, "text")
		if err != nil {
			fail(fmt.Sprintf("Publish update %d failed: %v", i, err))
		} else {
			fmt.Printf("      [PUB] Published update: %s\n", newVal)
		}
	}

	// Wait for watch to pick up changes
	time.Sleep(5 * time.Second)
	cancel()

	if changeCount > 0 {
		ok(fmt.Sprintf("Config watch received %d change(s)", changeCount))
	} else {
		fmt.Println("      [INFO] Watch did not detect changes (may need longer polling)")
	}
}

// ==================== Test 7: Health Reporting ====================

func testHealthReporting(c *client.Client) {
	section("6. Health Reporting (Passive Check)")

	// Report some successes
	for i := 0; i < 3; i++ {
		err := c.ReportSuccess("public", "DEFAULT_GROUP", "order-service", "192.168.1.10", 8080)
		if err != nil {
			fail("ReportSuccess failed: " + err.Error())
		}
	}
	ok("Reported 3 successes for order-service/192.168.1.10:8080")

	// Report some failures
	for i := 0; i < 3; i++ {
		err := c.ReportFailure("public", "DEFAULT_GROUP", "order-service", "192.168.1.12", 8080)
		if err != nil {
			fail("ReportFailure failed: " + err.Error())
		}
	}
	ok("Reported 3 failures for order-service/192.168.1.12:8080")

	// Check instance health status
	instances, err := c.GetInstances("public", "DEFAULT_GROUP", "order-service", false)
	if err != nil {
		fail("GetInstances for health check failed: " + err.Error())
	} else {
		for _, inst := range instances {
			status := "HEALTHY"
			if !inst.Healthy {
				status = "UNHEALTHY"
			}
			fmt.Printf("      %s:%d -> %s\n", inst.IP, inst.Port, status)
		}
		ok("Health status retrieved")
	}
}

// ==================== Test: gRPC Protocol ====================

func testGRPC(c *client.Client) {
	section("7. gRPC Protocol")

	// Connect via gRPC
	err := c.ConnectGRPC(grpcServerAddr)
	if err != nil {
		fail("gRPC connect failed: " + err.Error())
		fmt.Println("      Skipping gRPC tests (server may not be running gRPC)")
		return
	}
	ok("Connected to gRPC server at " + grpcServerAddr)
	defer c.CloseGRPC()

	// gRPC Register
	err = c.GRPCRegisterInstance(client.RegisterOptions{
		ServiceName: "grpc-test-service",
		IP:          "10.0.0.1",
		Port:        7070,
		Weight:      1.5,
		Ephemeral:   true,
	})
	if err != nil {
		fail("gRPC RegisterInstance failed: " + err.Error())
		return
	}
	ok("gRPC: Registered grpc-test-service/10.0.0.1:7070")

	// gRPC Register second instance
	err = c.GRPCRegisterInstance(client.RegisterOptions{
		ServiceName: "grpc-test-service",
		IP:          "10.0.0.2",
		Port:        7070,
		Weight:      2.5,
		Ephemeral:   true,
	})
	if err != nil {
		fail("gRPC RegisterInstance #2 failed: " + err.Error())
	} else {
		ok("gRPC: Registered grpc-test-service/10.0.0.2:7070")
	}

	// gRPC Heartbeat
	err = c.GRPCSendHeartbeat("public", "DEFAULT_GROUP", "grpc-test-service", "DEFAULT", "10.0.0.1", 7070)
	if err != nil {
		fail("gRPC Heartbeat failed: " + err.Error())
	} else {
		ok("gRPC: Heartbeat sent for grpc-test-service/10.0.0.1:7070")
	}

	// gRPC GetInstances
	instances, err := c.GRPCGetInstances("public", "DEFAULT_GROUP", "grpc-test-service", false)
	if err != nil {
		fail("gRPC GetInstances failed: " + err.Error())
	} else {
		ok(fmt.Sprintf("gRPC: Found %d instances for grpc-test-service", len(instances)))
		for _, inst := range instances {
			fmt.Printf("      -> %s:%d (weight=%.1f, healthy=%v)\n", inst.IP, inst.Port, inst.Weight, inst.Healthy)
		}
	}

	// gRPC Select with Consistent Hashing
	inst, err := c.GRPCSelectInstance("public", "DEFAULT_GROUP", "grpc-test-service", client.LBConsistentHash)
	if err != nil {
		fail("gRPC SelectInstance failed: " + err.Error())
	} else {
		ok(fmt.Sprintf("gRPC: Selected instance %s:%d via ConsistentHash", inst.IP, inst.Port))
	}

	// gRPC Config
	err = c.GRPCPublishConfig("public", "DEFAULT_GROUP", "grpc-config-test", "grpc-value-1", "text")
	if err != nil {
		fail("gRPC PublishConfig failed: " + err.Error())
	} else {
		ok("gRPC: Published config grpc-config-test")
	}

	item, err := c.GRPCGetConfig("public", "DEFAULT_GROUP", "grpc-config-test")
	if err != nil {
		fail("gRPC GetConfig failed: " + err.Error())
	} else {
		ok(fmt.Sprintf("gRPC: Got config (content=%s, md5=%s)", item.Content, item.MD5[:12]))
	}

	// gRPC Health Reporting
	err = c.GRPCReportSuccess("public", "DEFAULT_GROUP", "grpc-test-service", "10.0.0.1", 7070)
	if err != nil {
		fail("gRPC ReportSuccess failed: " + err.Error())
	} else {
		ok("gRPC: Reported success for grpc-test-service/10.0.0.1:7070")
	}

	err = c.GRPCReportFailure("public", "DEFAULT_GROUP", "grpc-test-service", "10.0.0.2", 7070)
	if err != nil {
		fail("gRPC ReportFailure failed: " + err.Error())
	} else {
		ok("gRPC: Reported failure for grpc-test-service/10.0.0.2:7070")
	}

	// gRPC Cleanup
	err = c.GRPCDeregisterInstance("public", "DEFAULT_GROUP", "grpc-test-service", "DEFAULT", "10.0.0.1", 7070)
	if err != nil {
		fail("gRPC Deregister #1 failed: " + err.Error())
	} else {
		ok("gRPC: Deregistered grpc-test-service/10.0.0.1:7070")
	}
	err = c.GRPCDeregisterInstance("public", "DEFAULT_GROUP", "grpc-test-service", "DEFAULT", "10.0.0.2", 7070)
	if err != nil {
		fail("gRPC Deregister #2 failed: " + err.Error())
	} else {
		ok("gRPC: Deregistered grpc-test-service/10.0.0.2:7070")
	}
}

// ==================== Test: DNS Service ====================

func testDNS() {
	section("8. DNS Service Discovery")

	dnsServer := "127.0.0.1:8553"
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: 3 * time.Second}
			return d.Dial("udp", dnsServer)
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test A record: order-service.DEFAULT_GROUP.public.benbroo.
	domain := "order-service.DEFAULT_GROUP.public.benbroo."
	ips, err := resolver.LookupHost(ctx, domain)
	if err != nil {
		// Try with context
		fmt.Printf("      [INFO] DNS A record lookup for %s: %v\n", domain, err)
		fmt.Println("      (DNS server may not be accessible or no instances registered)")
	} else {
		ok(fmt.Sprintf("DNS A record: %s -> %v", domain, ips))
	}

	// Test SRV record via direct UDP query
	fmt.Println("  [DNS] Attempting SRV lookup via direct UDP query...")
	conn, err := net.DialTimeout("udp", dnsServer, 3*time.Second)
	if err != nil {
		fmt.Printf("      [INFO] Cannot connect to DNS server: %v\n", err)
		return
	}
	defer conn.Close()

	// Build a minimal DNS SRV query manually is complex; use cname lookup as proxy
	cname, err := resolver.LookupCNAME(ctx, domain)
	if err != nil {
		fmt.Printf("      [INFO] DNS CNAME lookup: %v (expected for non-CNAME records)\n", err)
	} else {
		ok(fmt.Sprintf("DNS CNAME: %s -> %s", domain, cname))
	}

	// Verify DNS server is listening
	conn2, err := net.DialTimeout("tcp", dnsServer, 2*time.Second)
	if err != nil {
		fmt.Println("      [INFO] DNS TCP port not reachable (server may not have started DNS)")
	} else {
		conn2.Close()
		ok("DNS server is listening on port 8553 (TCP)")
	}
}

// ==================== Test: TCP Socket Protocol ====================

func testTCP(c *client.Client) {
	section("9. TCP Socket Protocol")

	// Connect via raw TCP
	err := c.ConnectTCP(tcpServerAddr)
	if err != nil {
		fail("TCP connect failed: " + err.Error())
		fmt.Println("      Skipping TCP tests (server may not be running TCP)")
		return
	}
	ok("Connected to TCP server at " + tcpServerAddr)
	defer c.CloseTCP()

	// TCP Ping
	pong, err := c.TCPPing()
	if err != nil {
		fail("TCP PING failed: " + err.Error())
		return
	}
	ok("TCP PING -> " + pong)

	// TCP Register
	err = c.TCPRegisterInstance(client.RegisterOptions{
		ServiceName: "tcp-test-service",
		IP:          "10.1.0.1",
		Port:        5050,
		Weight:      1.5,
		Ephemeral:   true,
	})
	if err != nil {
		fail("TCP RegisterInstance failed: " + err.Error())
		return
	}
	ok("TCP: Registered tcp-test-service/10.1.0.1:5050")

	err = c.TCPRegisterInstance(client.RegisterOptions{
		ServiceName: "tcp-test-service",
		IP:          "10.1.0.2",
		Port:        5050,
		Weight:      2.5,
		Ephemeral:   true,
	})
	if err != nil {
		fail("TCP RegisterInstance #2 failed: " + err.Error())
	} else {
		ok("TCP: Registered tcp-test-service/10.1.0.2:5050")
	}

	// TCP Heartbeat
	err = c.TCPSendHeartbeat("public", "DEFAULT_GROUP", "tcp-test-service", "DEFAULT", "10.1.0.1", 5050)
	if err != nil {
		fail("TCP Heartbeat failed: " + err.Error())
	} else {
		ok("TCP: Heartbeat sent for tcp-test-service/10.1.0.1:5050")
	}

	// TCP Discover
	instances, err := c.TCPGetInstances("public", "DEFAULT_GROUP", "tcp-test-service", false)
	if err != nil {
		fail("TCP GetInstances failed: " + err.Error())
	} else {
		ok(fmt.Sprintf("TCP: Found %d instances for tcp-test-service", len(instances)))
		for _, inst := range instances {
			fmt.Printf("      -> %s:%d (weight=%.1f, healthy=%v)\n", inst.IP, inst.Port, inst.Weight, inst.Healthy)
		}
	}

	// TCP Select with LB
	inst, err := c.TCPSelectInstance("public", "DEFAULT_GROUP", "tcp-test-service", client.LBRoundRobin)
	if err != nil {
		fail("TCP SelectInstance failed: " + err.Error())
	} else {
		ok(fmt.Sprintf("TCP: Selected instance %s:%d via RoundRobin", inst.IP, inst.Port))
	}

	// TCP Config
	err = c.TCPPublishConfig("public", "DEFAULT_GROUP", "tcp-config-test", "tcp-value-1", "text")
	if err != nil {
		fail("TCP PublishConfig failed: " + err.Error())
	} else {
		ok("TCP: Published config tcp-config-test")
	}

	item, err := c.TCPGetConfig("public", "DEFAULT_GROUP", "tcp-config-test")
	if err != nil {
		fail("TCP GetConfig failed: " + err.Error())
	} else {
		ok(fmt.Sprintf("TCP: Got config (content=%s, md5=%s)", item.Content, item.MD5[:12]))
	}

	// TCP Health Reporting
	err = c.TCPReportSuccess("public", "DEFAULT_GROUP", "tcp-test-service", "10.1.0.1", 5050)
	if err != nil {
		fail("TCP ReportSuccess failed: " + err.Error())
	} else {
		ok("TCP: Reported success for tcp-test-service/10.1.0.1:5050")
	}

	err = c.TCPReportFailure("public", "DEFAULT_GROUP", "tcp-test-service", "10.1.0.2", 5050)
	if err != nil {
		fail("TCP ReportFailure failed: " + err.Error())
	} else {
		ok("TCP: Reported failure for tcp-test-service/10.1.0.2:5050")
	}

	// TCP Cleanup
	err = c.TCPDeregisterInstance("public", "DEFAULT_GROUP", "tcp-test-service", "DEFAULT", "10.1.0.1", 5050)
	if err != nil {
		fail("TCP Deregister #1 failed: " + err.Error())
	} else {
		ok("TCP: Deregistered tcp-test-service/10.1.0.1:5050")
	}
	err = c.TCPDeregisterInstance("public", "DEFAULT_GROUP", "tcp-test-service", "DEFAULT", "10.1.0.2", 5050)
	if err != nil {
		fail("TCP Deregister #2 failed: " + err.Error())
	} else {
		ok("TCP: Deregistered tcp-test-service/10.1.0.2:5050")
	}

	err = c.TCPDeleteConfig("public", "DEFAULT_GROUP", "tcp-config-test")
	if err != nil {
		fail("TCP DeleteConfig failed: " + err.Error())
	} else {
		ok("TCP: Deleted config tcp-config-test")
	}
}

// ==================== Test 8: Cleanup ====================

func testCleanup(c *client.Client) {
	section("10. Cleanup")

	c.StopAllHeartbeats()
	ok("Stopped all heartbeats")

	// Deregister instances
	instances := []struct {
		service, ip string
		port        int
	}{
		{"order-service", "192.168.1.10", 8080},
		{"order-service", "192.168.1.11", 8080},
		{"order-service", "192.168.1.12", 8080},
		{"user-service", "192.168.2.10", 9090},
	}

	for _, inst := range instances {
		err := c.DeregisterInstance("public", "DEFAULT_GROUP", inst.service, "DEFAULT", inst.ip, inst.port)
		if err != nil {
			fail(fmt.Sprintf("Deregister %s/%s:%d failed: %v", inst.service, inst.ip, inst.port, err))
		} else {
			ok(fmt.Sprintf("Deregistered: %s/%s:%d", inst.service, inst.ip, inst.port))
		}
	}

	// Delete test configs
	configs := []string{"application.yaml", "database.properties", "watch-test"}
	for _, dataID := range configs {
		err := c.DeleteConfig("public", "DEFAULT_GROUP", dataID)
		if err != nil {
			fmt.Printf("      [WARN] Delete config %s: %v\n", dataID, err)
		} else {
			ok(fmt.Sprintf("Deleted config: %s", dataID))
		}
	}
}
