package model

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

// MetricType represents different OTel metric types
type MetricType string

const (
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeSum       MetricType = "sum"
	MetricTypeHistogram MetricType = "histogram"
	MetricTypeSummary   MetricType = "summary"
)

// QueryBuilder defines interface for building ClickHouse queries
type QueryBuilder interface {
	// BuildQuery constructs a ClickHouse SQL query from Prometheus query params
	BuildQuery(ctx context.Context, mint, maxt time.Time, matchers []*labels.Matcher, metricType MetricType) (string, error)
}

// Client defines interface for ClickHouse operations
type Client interface {
	// Query executes a query and returns rows
	Query(ctx context.Context, query string) (Rows, error)
	// Close closes the client connection
	Close() error
}

// Rows represents a result set from ClickHouse
type Rows interface {
	// Next advances the cursor to next row
	Next() bool
	// Scan copies the current row into the provided destination
	Scan(dest ...interface{}) error
	// Close closes the rows iterator
	Close() error
}
