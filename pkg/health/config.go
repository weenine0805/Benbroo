package health

// Config holds global health check settings from server.yaml.
type Config struct {
	CheckInterval     int `yaml:"checkInterval"`     // default active check interval (seconds)
	FailThreshold     int `yaml:"failThreshold"`      // consecutive active failures before marking unhealthy
	RemoveTimeout     int `yaml:"removeTimeout"`      // heartbeat timeout seconds
	PassiveWindow     int `yaml:"passiveWindow"`      // passive failure sliding window (seconds)
	PassiveThreshold  int `yaml:"passiveThreshold"`   // passive failures in window to mark unhealthy
	RecoveryThreshold int `yaml:"recoveryThreshold"`  // consecutive successes to recover
	ActiveTimeout     int `yaml:"activeTimeout"`      // active check request timeout (seconds)
}

func (c *Config) applyDefaults() {
	if c.CheckInterval <= 0 {
		c.CheckInterval = 5
	}
	if c.FailThreshold <= 0 {
		c.FailThreshold = 3
	}
	if c.RemoveTimeout <= 0 {
		c.RemoveTimeout = 30
	}
	if c.PassiveWindow <= 0 {
		c.PassiveWindow = 60
	}
	if c.PassiveThreshold <= 0 {
		c.PassiveThreshold = 5
	}
	if c.RecoveryThreshold <= 0 {
		c.RecoveryThreshold = 3
	}
	if c.ActiveTimeout <= 0 {
		c.ActiveTimeout = 3
	}
}
