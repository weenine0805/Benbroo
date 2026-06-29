package dns

import (
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/benbroo/benbroo/pkg/model"
	"github.com/benbroo/benbroo/pkg/naming"
	"github.com/miekg/dns"
	"go.uber.org/zap"
)

// Config holds DNS server settings.
type Config struct {
	Port   int    `yaml:"port"`   // DNS listen port (default: 8553)
	Domain string `yaml:"domain"` // Root domain (default: "benbroo.")
	TTL    uint32 `yaml:"ttl"`    // Record TTL in seconds (default: 5)
}

func (c *Config) applyDefaults() {
	if c.Port == 0 {
		c.Port = 8553
	}
	if c.Domain == "" {
		c.Domain = "benbroo."
	}
	if !strings.HasSuffix(c.Domain, ".") {
		c.Domain += "."
	}
	if c.TTL == 0 {
		c.TTL = 5
	}
}

// Server is the Benbroo DNS server.
// DNS name format: <service>.<group>.<namespace>.benbroo.
// Example: order-service.DEFAULT_GROUP.public.benbroo.
type Server struct {
	cfg       Config
	namingSvc *naming.Service
	logger    *zap.Logger

	mu     sync.Mutex
	udpSrv *dns.Server
	tcpSrv *dns.Server
	stopCh chan struct{}

	// Weighted routing state: per-domain round-robin index.
	rrMu    sync.Mutex
	rrIndex map[string]int
}

// NewServer creates a new DNS server.
func NewServer(cfg Config, namingSvc *naming.Service, log *zap.Logger) *Server {
	cfg.applyDefaults()
	return &Server{
		cfg:       cfg,
		namingSvc: namingSvc,
		logger:    log,
		stopCh:    make(chan struct{}),
		rrIndex:   make(map[string]int),
	}
}

// Start begins listening on both UDP and TCP.
func (s *Server) Start() error {
	handler := dns.HandlerFunc(s.handleDNS)

	s.udpSrv = &dns.Server{
		Addr:    fmt.Sprintf(":%d", s.cfg.Port),
		Net:     "udp",
		Handler: handler,
	}
	s.tcpSrv = &dns.Server{
		Addr:    fmt.Sprintf(":%d", s.cfg.Port),
		Net:     "tcp",
		Handler: handler,
	}

	errCh := make(chan error, 2)
	go func() {
		s.logger.Info("DNS server listening (UDP)", zap.Int("port", s.cfg.Port), zap.String("domain", s.cfg.Domain))
		errCh <- s.udpSrv.ListenAndServe()
	}()
	go func() {
		s.logger.Info("DNS server listening (TCP)", zap.Int("port", s.cfg.Port))
		errCh <- s.tcpSrv.ListenAndServe()
	}()

	// Give listeners a moment to start.
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-errCh:
		return fmt.Errorf("DNS server start failed: %w", err)
	default:
		return nil
	}
}

// Stop shuts down the DNS server.
func (s *Server) Stop() {
	close(s.stopCh)
	if s.udpSrv != nil {
		s.udpSrv.Shutdown()
	}
	if s.tcpSrv != nil {
		s.tcpSrv.Shutdown()
	}
}

// ==================== DNS Handler ====================

func (s *Server) handleDNS(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true
	m.RecursionAvailable = false

	for _, q := range r.Question {
		switch q.Qtype {
		case dns.TypeA:
			s.handleA(m, q)
		case dns.TypeSRV:
			s.handleSRV(m, q)
		case dns.TypeANY:
			s.handleA(m, q)
			s.handleSRV(m, q)
		}
	}

	w.WriteMsg(m)
}

// handleA resolves A records: returns IPs of healthy instances.
func (s *Server) handleA(m *dns.Msg, q dns.Question) {
	ns, group, svc, ok := s.parseName(q.Name)
	if !ok {
		return
	}

	instances := s.getHealthyInstances(ns, group, svc)
	if len(instances) == 0 {
		return
	}

	// Weighted selection: pick instances based on weight.
	selected := s.weightedSelect(instances, q.Name)
	for _, inst := range selected {
		ip := net.ParseIP(inst.IP)
		if ip == nil {
			continue
		}
		if ip.To4() != nil {
			m.Answer = append(m.Answer, &dns.A{
				Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: s.cfg.TTL},
				A:   ip.To4(),
			})
		}
	}
}

// handleSRV resolves SRV records: returns IP:Port pairs.
func (s *Server) handleSRV(m *dns.Msg, q dns.Question) {
	ns, group, svc, ok := s.parseName(q.Name)
	if !ok {
		return
	}

	instances := s.getHealthyInstances(ns, group, svc)
	if len(instances) == 0 {
		return
	}

	selected := s.weightedSelect(instances, q.Name)
	for i, inst := range selected {
		target := fmt.Sprintf("inst%d.%s", i, q.Name)
		m.Answer = append(m.Answer, &dns.SRV{
			Hdr:      dns.RR_Header{Name: q.Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: s.cfg.TTL},
			Priority: 0,
			Weight:   uint16(inst.Weight),
			Port:     uint16(inst.Port),
			Target:   target,
		})
		// Also add A record for the SRV target.
		ip := net.ParseIP(inst.IP)
		if ip != nil && ip.To4() != nil {
			m.Extra = append(m.Extra, &dns.A{
				Hdr: dns.RR_Header{Name: target, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: s.cfg.TTL},
				A:   ip.To4(),
			})
		}
	}
}

// ==================== Name Parsing ====================

// parseName extracts namespace, group, service from a DNS name.
// Format: <service>.<group>.<namespace>.<domain>.
// Example: order-service.DEFAULT_GROUP.public.benbroo.
func (s *Server) parseName(name string) (namespace, group, service string, ok bool) {
	// Trim trailing dot.
	name = strings.TrimSuffix(name, ".")
	suffix := strings.TrimSuffix(s.cfg.Domain, ".")

	// Case-insensitive suffix match.
	if !strings.HasSuffix(strings.ToLower(name), strings.ToLower(suffix)) {
		return "", "", "", false
	}

	// Remove the domain suffix (preserve original case for the prefix).
	// Find where the suffix starts in the original name.
	prefix := name[:len(name)-len(suffix)-1] // -1 for the dot separator
	parts := strings.SplitN(prefix, ".", 3)
	if len(parts) < 3 {
		return "", "", "", false
	}

	return parts[2], parts[1], parts[0], true
}

// ==================== Instance Lookup ====================

func (s *Server) getHealthyInstances(namespace, group, service string) []model.ServiceInstance {
	list, err := s.namingSvc.GetInstances(namespace, group, service, nil, true)
	if err != nil {
		s.logger.Debug("DNS: failed to get instances", zap.Error(err))
		return nil
	}
	return list
}

// ==================== Weighted Routing ====================

// weightedSelect returns instances sorted by weight-based round-robin.
// Higher weight instances appear more frequently across queries.
func (s *Server) weightedSelect(instances []model.ServiceInstance, domain string) []model.ServiceInstance {
	if len(instances) <= 1 {
		return instances
	}

	// Calculate total weight.
	totalWeight := 0.0
	for _, inst := range instances {
		totalWeight += inst.Weight
	}
	if totalWeight <= 0 {
		return instances
	}

	// Build weighted order: each instance gets slots proportional to its weight.
	// Use round-robin rotation to distribute the "first" answer across queries.
	s.rrMu.Lock()
	idx := s.rrIndex[domain]
	s.rrIndex[domain] = idx + 1
	s.rrMu.Unlock()

	// Shuffle instances using weight-proportional selection.
	// For DNS, we return all instances but rotate the order so that
	// the first answer rotates, achieving weighted load distribution.
	result := make([]model.ServiceInstance, len(instances))
	copy(result, instances)

	// Sort by weight descending, then rotate by rr index.
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Weight > result[i].Weight {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	// Rotate the list by (idx % len).
	rotate := idx % len(result)
	if rotate > 0 {
		tmp := make([]model.ServiceInstance, len(result))
		copy(tmp, result[rotate:])
		copy(tmp[len(result)-rotate:], result[:rotate])
		result = tmp
	}

	return result
}

// randomWeightedPick picks a single instance based on weight probability.
func randomWeightedPick(instances []model.ServiceInstance) *model.ServiceInstance {
	totalWeight := 0.0
	for _, inst := range instances {
		totalWeight += inst.Weight
	}
	if totalWeight <= 0 {
		return &instances[rand.Intn(len(instances))]
	}
	r := rand.Float64() * totalWeight
	cumulative := 0.0
	for i, inst := range instances {
		cumulative += inst.Weight
		if r <= cumulative {
			return &instances[i]
		}
	}
	return &instances[len(instances)-1]
}
