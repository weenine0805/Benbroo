package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// Config holds authentication settings.
type Config struct {
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	SecretKey string `yaml:"secretKey"`
	TokenTTL  int    `yaml:"tokenTTL"` // token expiry in seconds, default 86400 (24h)
}

// DefaultConfig returns the default auth config.
func DefaultConfig() Config {
	return Config{
		Username:  "benbroo",
		Password:  "benbroo",
		SecretKey: "benbroo-secret-key-2024",
		TokenTTL:  86400,
	}
}

// Manager handles login verification and token generation/validation.
type Manager struct {
	username  string
	password  string
	secretKey []byte
	tokenTTL  time.Duration
}

// NewManager creates an auth manager from config.
func NewManager(cfg Config) *Manager {
	if cfg.Username == "" {
		cfg.Username = "benbroo"
	}
	if cfg.Password == "" {
		cfg.Password = "benbroo"
	}
	if cfg.SecretKey == "" {
		cfg.SecretKey = "benbroo-secret-key-2024"
	}
	if cfg.TokenTTL <= 0 {
		cfg.TokenTTL = 86400
	}
	return &Manager{
		username:  cfg.Username,
		password:  cfg.Password,
		secretKey: []byte(cfg.SecretKey),
		tokenTTL:  time.Duration(cfg.TokenTTL) * time.Second,
	}
}

// Login verifies credentials and returns a signed token.
func (m *Manager) Login(username, password string) (string, error) {
	if username != m.username || password != m.password {
		return "", errors.New("invalid username or password")
	}
	return m.generateToken()
}

// ValidateToken checks if a token is valid and not expired.
func (m *Manager) ValidateToken(token string) error {
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return errors.New("invalid token format")
	}
	if len(data) < 12 { // 8 bytes timestamp + at least 4 bytes signature
		return errors.New("invalid token length")
	}

	// Extract timestamp and signature.
	tsBytes := data[:8]
	sig := data[8:]
	expiry := time.Unix(0, int64(binary.BigEndian.Uint64(tsBytes)))

	if time.Now().After(expiry) {
		return fmt.Errorf("token expired at %s", expiry.Format(time.RFC3339))
	}

	// Verify HMAC signature.
	mac := hmac.New(sha256.New, m.secretKey)
	mac.Write(tsBytes)
	expectedSig := mac.Sum(nil)

	if !hmac.Equal(sig, expectedSig) {
		return errors.New("invalid token signature")
	}

	return nil
}

func (m *Manager) generateToken() (string, error) {
	expiry := time.Now().Add(m.tokenTTL)
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(expiry.UnixNano()))

	mac := hmac.New(sha256.New, m.secretKey)
	mac.Write(tsBytes)
	sig := mac.Sum(nil)

	token := append(tsBytes, sig...)
	return base64.RawURLEncoding.EncodeToString(token), nil
}
