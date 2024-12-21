package reader

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/clickhouse/model"
)

func TestConvertLabelMatchers(t *testing.T) {
	tests := []struct {
		name      string
		matchers  []*prompb.LabelMatcher
		want      []*labels.Matcher
		expectErr bool
	}{
		{
			name: "valid matchers",
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				{Type: prompb.LabelMatcher_NEQ, Name: "baz", Value: "qux"},
			},
			want: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchNotEqual, "baz", "qux"),
			},
			expectErr: false,
		},
		// {
		// 	name: "invalid matcher type",
		// 	matchers: []*prompb.LabelMatcher{
		// 		{Type: 999, Name: "foo", Value: "bar"},
		// 	},
		// 	want:      nil,
		// 	expectErr: true,
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertLabelMatchers(tt.matchers)
			if (err != nil) != tt.expectErr {
				t.Errorf("convertLabelMatchers() error = %v, expectErr %v", err, tt.expectErr)
				return
			}
			if !labelsMatchersEqual(got, tt.want) {
				t.Errorf("convertLabelMatchers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func labelsMatchersEqual(a, b []*labels.Matcher) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type || a[i].Name != b[i].Name || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}

func TestInferMetricType(t *testing.T) {
	tests := []struct {
		query      string
		metricName string
		labels     map[string]string
		expected   model.MetricType
	}{
		{
			query:      "rate(metric_total[5m])",
			metricName: "metric_total",
			labels:     map[string]string{},
			expected:   model.MetricTypeSum,
		},
		{
			query:      "histogram_quantile(0.95, metric_bucket)",
			metricName: "metric_bucket",
			labels:     map[string]string{"le": "0.95"},
			expected:   model.MetricTypeHistogram,
		},
		{
			query:      "sum_over_time(metric_sum[5m])",
			metricName: "metric_sum",
			labels:     map[string]string{},
			expected:   model.MetricTypeGauge,
		},
		{
			query:      "metric_sum[5m]",
			metricName: "metric_sum",
			labels:     map[string]string{},
			expected:   model.MetricTypeSum,
		},
		{
			query:      "deriv(metric)",
			metricName: "metric",
			labels:     map[string]string{},
			expected:   model.MetricTypeGauge,
		},
		{
			query:      "rate(metric[5m])",
			metricName: "metric",
			labels:     map[string]string{},
			expected:   model.MetricTypeGauge,
		},
		{
			query:      "metric",
			metricName: "metric",
			labels:     map[string]string{},
			expected:   model.MetricTypeGauge,
		},
		{
			query:      "rate(chi_clickhouse_metric_DiskDataBytes[5m])",
			metricName: "chi_clickhouse_metric_DiskDataBytes",
			labels:     map[string]string{},
			expected:   model.MetricTypeGauge,
		}, {
			query:      "sum(rate(node_network_receive_bytes_total{cluster=\"demo-acc-cluster\", job=\"integrations/node_exporter\"}[$__rate_interval])) by (instance)",
			metricName: "chi_clickhouse_metric_DiskDataBytes",
			labels:     map[string]string{},
			expected:   model.MetricTypeGauge,
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			result := inferMetricType(test.query, test.metricName, test.labels)
			if result != test.expected {
				t.Errorf("expected %v, got %v", test.expected, result)
			}
		})
	}
}
