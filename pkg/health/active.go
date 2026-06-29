package health

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// activeProber performs active health checks (TCP / HTTP).
type activeProber struct {
	timeout time.Duration
	logger  *zap.Logger
}

func newActiveProber(timeoutSec int, log *zap.Logger) *activeProber {
	return &activeProber{
		timeout: time.Duration(timeoutSec) * time.Second,
		logger:  log,
	}
}

// Probe performs a single active health check and returns true if healthy.
func (p *activeProber) Probe(protocol, ip string, port int, path string) bool {
	switch protocol {
	case "HTTP":
		return p.httpCheck(ip, port, path)
	default:
		return p.tcpCheck(ip, port)
	}
}

// tcpCheck tests TCP connectivity.
func (p *activeProber) tcpCheck(ip string, port int) bool {
	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	conn, err := net.DialTimeout("tcp", addr, p.timeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// httpCheck sends an HTTP GET and expects a 2xx/3xx response.
func (p *activeProber) httpCheck(ip string, port int, path string) bool {
	if path == "" {
		path = "/health"
	}
	url := fmt.Sprintf("http://%s%s", net.JoinHostPort(ip, fmt.Sprintf("%d", port)), path)
	client := &http.Client{Timeout: p.timeout}
	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}
