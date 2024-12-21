package client

import (
	"time"
)

type Options struct {
	// Basic connection options
	Addresses []string
	Database  string
	Username  string
	Password  string

	// Connection pool settings
	MaxOpenConns int
	MaxIdleConns int

	// Timeouts
	DialTimeout      time.Duration
	MaxExecutionTime time.Duration

	// TLS configuration
	SkipVerify bool
	CertPath   string
	KeyPath    string
	CAPath     string
}

func DefaultOptions() *Options {
	return &Options{
		MaxOpenConns:     10,
		MaxIdleConns:     5,
		DialTimeout:      10 * time.Second,
		MaxExecutionTime: 30 * time.Second,
	}
}
