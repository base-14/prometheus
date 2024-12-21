package client

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Client struct {
	conn driver.Conn
	opts *Options
}

// NewClient creates a new ClickHouse client
func NewClient(opts *Options) (*Client, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	// Build ClickHouse connection config
	config := &clickhouse.Options{
		Addr: opts.Addresses,
		Auth: clickhouse.Auth{
			Database: opts.Database,
			Username: opts.Username,
			Password: opts.Password,
		},
		MaxOpenConns:     opts.MaxOpenConns,
		MaxIdleConns:     opts.MaxIdleConns,
		ConnMaxLifetime:  time.Hour,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": opts.MaxExecutionTime.Seconds(),
		},
	}

	// Create connection
	conn, err := clickhouse.Open(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clickhouse connection: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), opts.DialTimeout)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	return &Client{
		conn: conn,
		opts: opts,
	}, nil
}

// Query executes a query and returns rows
func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	return &clickhouseRows{rows: rows}, nil
}

// Close closes the client connection
func (c *Client) Close() error {
	return c.conn.Close()
}

// Rows implementation
type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
	Columns() ([]string, error)
	ColumnTypes() ([]driver.ColumnType, error)
}

type clickhouseRows struct {
	rows driver.Rows
}

func (r *clickhouseRows) Next() bool {
	return r.rows.Next()
}

func (r *clickhouseRows) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

func (r *clickhouseRows) Close() error {
	return r.rows.Close()
}

func (r *clickhouseRows) Columns() ([]string, error) {
	return r.rows.Columns(), nil
}

func (r *clickhouseRows) ColumnTypes() ([]driver.ColumnType, error) {
	return r.rows.ColumnTypes(), nil
}

// Helper methods for specific query types
type MetricsClient struct {
	*Client
}

func NewMetricsClient(opts *Options) (*MetricsClient, error) {
	client, err := NewClient(opts)
	if err != nil {
		return nil, err
	}
	return &MetricsClient{Client: client}, nil
}

// QueryGauge queries gauge metrics
func (c *MetricsClient) QueryGauge(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return c.Query(ctx, query, args...)
}

// QueryHistogram queries histogram metrics
func (c *MetricsClient) QueryHistogram(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return c.Query(ctx, query, args...)
}

// QuerySum queries sum metrics
func (c *MetricsClient) QuerySum(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return c.Query(ctx, query, args...)
}

// QuerySummary queries summary metrics
func (c *MetricsClient) QuerySummary(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return c.Query(ctx, query, args...)
}
