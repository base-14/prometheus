package query

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage/remote/clickhouse/model"
)

func TestBuildQuery(t *testing.T) {
	builder := NewBuilder()
	ctx := context.Background()
	mint := time.Now().Add(-time.Hour)
	maxt := time.Now()

	tests := []struct {
		name       string
		metricType model.MetricType
		matchers   []*labels.Matcher
		wantErr    bool
		wantFrom   string
		wantWhere  []string
	}{
		{
			name:       "Gauge metric with no matchers",
			metricType: model.MetricTypeGauge,
			matchers:   []*labels.Matcher{},
			wantErr:    false,
			wantFrom:   "FROM otel_metrics_gauge",
			wantWhere: []string{
				"TimeUnix BETWEEN",
			},
		},
		{
			name:       "Sum metric with matchers",
			metricType: model.MetricTypeSum,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"),
				labels.MustNewMatcher(labels.MatchEqual, "service_name", "api_service"),
			},
			wantErr:  false,
			wantFrom: "FROM otel_metrics_sum",
			wantWhere: []string{
				"TimeUnix BETWEEN",
				"MetricName = 'http_requests_total'",
				"ServiceName = 'api_service'",
			},
		},
		{
			name:       "Histogram metric with matchers",
			metricType: model.MetricTypeHistogram,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "env", "prod|staging"),
			},
			wantErr:  false,
			wantFrom: "FROM otel_metrics_histogram",
			wantWhere: []string{
				"TimeUnix BETWEEN",
				"match(ResourceAttributes['env'], 'prod|staging')",
			},
		},
		{
			name:       "Summary metric with matchers",
			metricType: model.MetricTypeSummary,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "region", "us-west"),
			},
			wantErr:  false,
			wantFrom: "FROM otel_metrics_summary",
			wantWhere: []string{
				"TimeUnix BETWEEN",
				"ResourceAttributes['region'] != 'us-west'",
			},
		},
		{
			name:       "Unsupported metric type",
			metricType: model.MetricType("unsupported"),
			matchers:   []*labels.Matcher{},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := builder.BuildQuery(ctx, mint, maxt, tt.matchers, tt.metricType)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if query == "" {
					t.Errorf("BuildQuery() returned empty query")
					return
				}
				if !strings.Contains(query, tt.wantFrom) {
					t.Errorf("BuildQuery() query = %v, wantFrom %v", query, tt.wantFrom)
				}
				for _, clause := range tt.wantWhere {
					if !strings.Contains(query, clause) {
						t.Errorf("BuildQuery() query = %v, wantWhere %v", query, clause)
					}
				}
			}
		})
	}
}

func TestBuildLabelMatchers(t *testing.T) {
	builder := NewBuilder()

	tests := []struct {
		name     string
		matchers []*labels.Matcher
		want     []string
	}{
		{
			name: "Single matcher",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "service_name", "api_service"),
			},
			want: []string{"ServiceName = 'api_service'"},
		},
		{
			name: "Multiple matchers",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"),
				labels.MustNewMatcher(labels.MatchEqual, "service_name", "api_service"),
			},
			want: []string{"MetricName = 'http_requests_total'", "ServiceName = 'api_service'"},
		},
		{
			name: "Regexp matcher",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "env", "prod|staging"),
			},
			want: []string{"((mapContains(ResourceAttributes, 'env') AND match(ResourceAttributes['env'], 'prod|staging')) OR (mapContains(Attributes, 'env') AND match(Attributes['env'], 'prod|staging')))"},
		},
		{
			name: "NotEqual matcher",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "region", "us-west"),
			},
			want: []string{"(NOT mapContains(ResourceAttributes, 'region') OR ResourceAttributes['region'] != 'us-west') AND (NOT mapContains(Attributes, 'region') OR Attributes['region'] != 'us-west')"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := builder.buildLabelMatchers(tt.matchers)
			if len(got) != len(tt.want) {
				t.Errorf("buildLabelMatchers() got = %v, want %v", got, tt.want)
				return
			}
			for i, condition := range got {
				if condition != tt.want[i] {
					t.Errorf("buildLabelMatchers() got = %v, want %v", got, tt.want)
				}
			}
		})
	}
}
