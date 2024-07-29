package httpkit

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

const (
	_defaultPort            = 8080
	_defaultIdleTimeout     = 1 * time.Minute
	_defaultReadTimeout     = 5 * time.Second
	_defaultWriteTimeout    = 10 * time.Second
	_defaultShutdownTimeout = 10 * time.Second
)

type Config struct {
	Host            string
	Port            int
	IdleTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	TLS             *tls.Config
	tlsErr          error
}

func DefaultConfig() Config {
	return Config{
		Port:            _defaultPort,
		IdleTimeout:     _defaultIdleTimeout,
		ReadTimeout:     _defaultReadTimeout,
		WriteTimeout:    _defaultWriteTimeout,
		ShutdownTimeout: _defaultShutdownTimeout,
	}
}

func (c Config) Addr() string { return net.JoinHostPort(c.Host, strconv.Itoa(c.Port)) }

func (c *Config) Override(other Config) {
	if other.Host != "" {
		c.Host = other.Host
	}

	if other.Port != 0 {
		c.Port = other.Port
	}

	if other.IdleTimeout != 0 {
		c.IdleTimeout = other.IdleTimeout
	}

	if other.ReadTimeout != 0 {
		c.ReadTimeout = other.ReadTimeout
	}

	if other.WriteTimeout != 0 {
		c.WriteTimeout = other.WriteTimeout
	}

	if other.ShutdownTimeout != 0 {
		c.ShutdownTimeout = other.ShutdownTimeout
	}
}

func (c *Config) Validate() error {
	c.setDefaultZeroValues()

	if c.Port <= 0 {
		return errors.New("port must be greater than 0")
	}

	if c.IdleTimeout <= 0 {
		return errors.New("idle timeout must be greater than 0")
	}

	if c.ReadTimeout <= 0 {
		return errors.New("read timeout must be greater than 0")
	}

	if c.WriteTimeout <= 0 {
		return errors.New("write timeout must be greater than 0")
	}

	if c.ShutdownTimeout <= 0 {
		return errors.New("shutdown timeout must be greater than 0")
	}

	if c.TLS != nil && c.tlsErr != nil {
		return fmt.Errorf("tls must be configured correctly if provided: %w", c.tlsErr)
	}

	return nil
}

func (c *Config) setDefaultZeroValues() {
	if c.Port <= 0 {
		c.Port = _defaultPort
	}

	if c.IdleTimeout <= 0 {
		c.IdleTimeout = _defaultIdleTimeout
	}

	if c.ReadTimeout <= 0 {
		c.ReadTimeout = _defaultReadTimeout
	}

	if c.WriteTimeout <= 0 {
		c.WriteTimeout = _defaultWriteTimeout
	}

	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = _defaultShutdownTimeout
	}
}

type ConfigOption interface{ applyToConfig(*Config) }

type (
	hostOption            struct{ value string }
	portOption            struct{ value int }
	idleTimeoutOption     struct{ value time.Duration }
	readTimeoutOption     struct{ value time.Duration }
	writeTimeoutOption    struct{ value time.Duration }
	shutdownTimeoutOption struct{ value time.Duration }

	tlsOption struct {
		value *tls.Config
		err   error
	}

	configOption  struct{ value Config }
	configOptions struct{ value []ConfigOption }
)

func WithHost(v string) ConfigOption                   { return hostOption{value: v} }
func WithPort(v int) ConfigOption                      { return portOption{value: v} }
func WithIdleTimeout(v time.Duration) ConfigOption     { return idleTimeoutOption{value: v} }
func WithReadTimeout(v time.Duration) ConfigOption     { return readTimeoutOption{value: v} }
func WithWriteTimeout(v time.Duration) ConfigOption    { return writeTimeoutOption{value: v} }
func WithShutdownTimeout(v time.Duration) ConfigOption { return shutdownTimeoutOption{value: v} }
func WithConfig(v Config) ConfigOption                 { return configOption{value: v} }
func WithConfigOptions(v ...ConfigOption) ConfigOption { return configOptions{value: v} }

func WithTLS(caFile, ceFile, keyFile string) ConfigOption {
	ce, err := tls.LoadX509KeyPair(ceFile, keyFile)
	if err != nil {
		return tlsOption{err: err}
	}

	ca, err := os.ReadFile(caFile)
	if err != nil {
		return tlsOption{err: err}
	}

	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(ca); !ok {
		return tlsOption{err: errors.New("unable to append certs from PEM")}
	}

	return tlsOption{
		value: &tls.Config{
			ClientAuth:   tls.RequireAndVerifyClientCert,
			Certificates: []tls.Certificate{ce},
			ClientCAs:    pool,
			MinVersion:   tls.VersionTLS12,
			NextProtos:   []string{"h2", "http/1.1"},
		},
	}
}

func (o hostOption) applyToConfig(cfg *Config)            { cfg.Host = o.value }
func (o portOption) applyToConfig(cfg *Config)            { cfg.Port = o.value }
func (o idleTimeoutOption) applyToConfig(cfg *Config)     { cfg.IdleTimeout = o.value }
func (o readTimeoutOption) applyToConfig(cfg *Config)     { cfg.ReadTimeout = o.value }
func (o writeTimeoutOption) applyToConfig(cfg *Config)    { cfg.WriteTimeout = o.value }
func (o shutdownTimeoutOption) applyToConfig(cfg *Config) { cfg.ShutdownTimeout = o.value }
func (o tlsOption) applyToConfig(cfg *Config)             { cfg.TLS, cfg.tlsErr = o.value, o.err }
func (o configOption) applyToConfig(cfg *Config)          { cfg.Override(o.value) }
func (o configOptions) applyToConfig(cfg *Config) {
	for _, opt := range o.value {
		opt.applyToConfig(cfg)
	}
}
