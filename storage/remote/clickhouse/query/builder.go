package query

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage/remote/clickhouse/model"
)

type Builder struct {
	// tableMapping maps metric types to their corresponding tables
	tableMapping map[model.MetricType]string
}

func NewBuilder() *Builder {
	return &Builder{
		tableMapping: map[model.MetricType]string{
			model.MetricTypeGauge:     "otel_metrics_gauge",
			model.MetricTypeSum:       "otel_metrics_sum",
			model.MetricTypeHistogram: "otel_metrics_histogram",
			model.MetricTypeSummary:   "otel_metrics_summary",
		},
	}
}

// BuildQuery implements QueryBuilder interface
func (b *Builder) BuildQuery(ctx context.Context, mint, maxt time.Time, matchers []*labels.Matcher, metricType model.MetricType) (string, error) {
	table, ok := b.tableMapping[metricType]
	if !ok {
		return "", fmt.Errorf("unsupported metric type: %s", metricType)
	}

	queryParts := []string{
		"SELECT TimeUnix as timestamp,",
	}

	// Add value selection based on metric type
	switch metricType {
	case model.MetricTypeGauge, model.MetricTypeSum:
		queryParts = append(queryParts, "Value as value,")
	case model.MetricTypeHistogram:
		// For histograms, we need to handle bucket counts
		queryParts = append(queryParts, "Sum as value, BucketCounts, ExplicitBounds,")
	case model.MetricTypeSummary:
		// For summaries, we need to handle quantiles
		queryParts = append(queryParts, "Sum as value, Count, ValueAtQuantiles.Quantile, ValueAtQuantiles.Value,")
	}

	// Add label columns
	queryParts = append(queryParts,
		"MetricName,",
		"ServiceName,",
		"Attributes,",
		"ResourceAttributes,",
	)

	// Add FROM clause
	queryParts = append(queryParts, fmt.Sprintf("FROM %s", table))

	// Add WHERE clause
	whereClauses := []string{
		fmt.Sprintf("TimeUnix BETWEEN toDateTime64('%s', 9) AND toDateTime64('%s', 9)",
			mint.Format(time.RFC3339Nano),
			maxt.Format(time.RFC3339Nano)),
	}

	// Handle label matchers
	labelClauses := b.buildLabelMatchers(matchers)
	if len(labelClauses) > 0 {
		whereClauses = append(whereClauses, labelClauses...)
	}

	queryParts = append(queryParts, "WHERE "+strings.Join(whereClauses, " AND "))

	// Add ORDER BY clause
	queryParts = append(queryParts, "ORDER BY TimeUnix")

	return strings.Join(queryParts, " "), nil
}

// buildLabelMatchers converts Prometheus label matchers to ClickHouse WHERE conditions
func (b *Builder) buildLabelMatchers(matchers []*labels.Matcher) []string {
	var conditions []string

	for _, m := range matchers {
		switch m.Name {
		case "__name__":
			// Handle metric name matcher
			conditions = append(conditions, b.buildMetricNameMatcher(m))
		case "service_name":
			// Handle service name matcher
			conditions = append(conditions, b.buildServiceNameMatcher(m))
		default:
			// Handle attribute matchers
			conditions = append(conditions, b.buildAttributeMatcher(m))
		}
	}

	return conditions
}

func (b *Builder) buildMetricNameMatcher(m *labels.Matcher) string {
	switch m.Type {
	case labels.MatchEqual:
		return fmt.Sprintf("MetricName = '%s'", escapeString(m.Value))
	case labels.MatchNotEqual:
		return fmt.Sprintf("MetricName != '%s'", escapeString(m.Value))
	case labels.MatchRegexp:
		return fmt.Sprintf("match(MetricName, '%s')", escapeString(m.Value))
	case labels.MatchNotRegexp:
		return fmt.Sprintf("NOT match(MetricName, '%s')", escapeString(m.Value))
	default:
		return ""
	}
}

func (b *Builder) buildServiceNameMatcher(m *labels.Matcher) string {
	switch m.Type {
	case labels.MatchEqual:
		return fmt.Sprintf("ServiceName = '%s'", escapeString(m.Value))
	case labels.MatchNotEqual:
		return fmt.Sprintf("ServiceName != '%s'", escapeString(m.Value))
	case labels.MatchRegexp:
		return fmt.Sprintf("match(ServiceName, '%s')", escapeString(m.Value))
	case labels.MatchNotRegexp:
		return fmt.Sprintf("NOT match(ServiceName, '%s')", escapeString(m.Value))
	default:
		return ""
	}
}

func (b *Builder) buildAttributeMatcher(m *labels.Matcher) string {
	// Check both ResourceAttributes and Attributes maps
	switch m.Type {
	case labels.MatchEqual:
		return fmt.Sprintf("(mapContains(ResourceAttributes, '%s') AND ResourceAttributes['%s'] = '%s') OR (mapContains(Attributes, '%s') AND Attributes['%s'] = '%s')",
			escapeString(m.Name), escapeString(m.Name), escapeString(m.Value),
			escapeString(m.Name), escapeString(m.Name), escapeString(m.Value))
	case labels.MatchNotEqual:
		return fmt.Sprintf("(NOT mapContains(ResourceAttributes, '%s') OR ResourceAttributes['%s'] != '%s') AND (NOT mapContains(Attributes, '%s') OR Attributes['%s'] != '%s')",
			escapeString(m.Name), escapeString(m.Name), escapeString(m.Value),
			escapeString(m.Name), escapeString(m.Name), escapeString(m.Value))
	case labels.MatchRegexp:
		return fmt.Sprintf("((mapContains(ResourceAttributes, '%s') AND match(ResourceAttributes['%s'], '%s')) OR (mapContains(Attributes, '%s') AND match(Attributes['%s'], '%s')))",
			escapeString(m.Name), escapeString(m.Name), escapeString(m.Value),
			escapeString(m.Name), escapeString(m.Name), escapeString(m.Value))
	case labels.MatchNotRegexp:
		return fmt.Sprintf("(NOT mapContains(ResourceAttributes, '%s') OR NOT match(ResourceAttributes['%s'], '%s')) AND (NOT mapContains(Attributes, '%s') OR NOT match(Attributes['%s'], '%s'))",
			escapeString(m.Name), escapeString(m.Name), escapeString(m.Value),
			escapeString(m.Name), escapeString(m.Name), escapeString(m.Value))
	default:
		return ""
	}
}

// escapeString escapes special characters in strings for ClickHouse SQL
func escapeString(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "'", "\\'"), "\\", "\\\\")
}
