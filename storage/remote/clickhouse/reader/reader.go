package reader

import (
	"context"
	"golang.org/x/exp/maps"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/clickhouse/client"
	"github.com/prometheus/prometheus/storage/remote/clickhouse/model"
	"github.com/prometheus/prometheus/storage/remote/clickhouse/query"
)

type Reader struct {
	client  client.Client
	builder query.Builder
}

func NewReader(client client.Client, builder query.Builder) *Reader {
	return &Reader{
		client:  client,
		builder: builder,
	}
}

// Read implements storage.QueryableClient
func (r *Reader) Read(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	response := &prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, 0, len(req.Queries)),
	}
	queries := req.Queries
	for _, promQLQuery := range queries {
		matchers, err := convertLabelMatchers(promQLQuery.Matchers)
		if err != nil {
			return nil, err
		}
		clickhouseQuery, err := r.builder.BuildQuery(ctx, time.UnixMilli(promQLQuery.StartTimestampMs),
			time.UnixMilli(promQLQuery.EndTimestampMs),
			matchers, getMetricType(promQLQuery))
		if err != nil {
			return nil, err
		}

		result, err := r.client.Query(ctx, clickhouseQuery)
		if err != nil {
			return nil, err
		}
		readResponse, err := convertToReadResponse(result)

		if err != nil {
			return nil, err
		}
		response.Results = append(response.Results, readResponse)
	}

	return response, nil
}

func getMetricType(pQuery *prompb.Query) model.MetricType {
	var pseudoQuery string
	var metricName string
	var queryLabels map[string]string
	matchers := pQuery.Matchers

	for _, matcher := range matchers {
		pseudoQuery += matcher.Value
		if matcher.Name == "__name__" {
			metricName = matcher.Value
		}
		queryLabels[matcher.Name] = matcher.Value
	}
	return inferMetricType(pseudoQuery, metricName, queryLabels)
}

// Here's how we will infer -
// Metric Names and Common Conventions:
// Counters => otel_metrics_sum
// Gauge => otel_metrics_gauge
// Histogram => otel_metric_histogram
// Summary => otel_metric_summary
//
// _total suffix: Metrics ending with _total are very often counters. This is a strong indicator that it is a _sum.
// _count suffix: Similar to _total, _count often suggests a counter or a histogram's count. we default to _sum.
// _sum suffix: This is often used for histograms and summaries to represent the sum of observed values.
// _bucket suffix: This is a clear indicator of a histogram.
// 2. PromQL Functions:
//
// rate() or irate(): These functions are specifically designed for calculating the per-second rate of increase of counters.
// If these functions are used, it's almost certainly a counter.
// increase(): This function calculates the increase in the value of a counter over a specified time range. Again, strongly indicates a counter.
// histogram_quantile(): This function is exclusively used with histograms.
// sum(rate(...)) or sum(increase(...)): Applying sum() after rate() or increase() suggests you're aggregating rates of multiple counters.
// count_over_time(): While applicable to any time series, it's often used with counters to count events over a time window.
// deriv(): This function calculates the per-second derivative of a time series. It can be applied to gauges, but it's less commonly used on counters.
// 3. Label Analysis:
// le label (in histograms): The presence of the le (less than or equal to) label is a definitive sign of a histogram's buckets.
// 4. Combining Clues:
//
// The most reliable approach is to combine these clues. For example:
//
// If a metric ends with _total and is used with rate(), it's almost certainly a counter.
// If a metric has the le label and is used with histogram_quantile(), it's definitely a histogram.
func inferMetricType(query string, metricName string, labels map[string]string) model.MetricType {
	query = strings.ToLower(query)

	if strings.HasSuffix(metricName, "_total") || strings.HasSuffix(metricName, "_count") {
		if strings.Contains(query, "rate(") || strings.Contains(query, "irate(") || strings.Contains(query, "increase(") {
			return model.MetricTypeSum
		}
		if strings.Contains(query, "sum_over_time(") || strings.Contains(query, "count_over_time(") {
			return model.MetricTypeSum
		}

	}

	// Check for common functions used with counters even without _total/_count
	// we do this at the last
	counterFunctionsRegexMap := map[string]string{
		"rate":            `rate\(`,
		"irate":           `irate\(`,
		"increase":        `increase\(`,
		"sum_over_time":   `sum_over_time\(`,
		"count_over_time": `count_over_time\(`,
	}
	for _, fn := range maps.Keys(counterFunctionsRegexMap) {
		if strings.Contains(query, fn) {
			// Use a regex to check if function is used on the current metric
			re := regexp.MustCompile(fn + `\s*\(\s*` + regexp.QuoteMeta(metricName) + `\b`)
			if re.MatchString(query) {
				return model.MetricTypeGauge
			}
		}
	}

	if strings.HasSuffix(metricName, "_bucket") || labels["le"] != "" {
		if strings.Contains(query, "histogram_quantile(") {
			return model.MetricTypeHistogram
		}
	}

	if strings.HasSuffix(metricName, "_sum") {
		// Could be summary or histogram, need more context if possible
		if strings.Contains(query, "histogram_quantile(") {
			return model.MetricTypeSummary
		}
		return model.MetricTypeSum
	}

	if strings.Contains(query, "deriv(") {
		return model.MetricTypeGauge
	}

	// Default to gauge if no strong indicators
	return model.MetricTypeGauge
}

func convertLabelMatchers(matchers []*prompb.LabelMatcher) ([]*labels.Matcher, error) {
	var result []*labels.Matcher
	for _, m := range matchers {
		matcher, err := labels.NewMatcher(labels.MatchType(m.Type), m.Name, m.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, matcher)
	}
	return result, nil
}

func convertToReadResponse(result client.Rows) (*prompb.QueryResult, error) {
	var tsResults []*prompb.TimeSeries
	for result.Next() {
		var samples []prompb.Sample
		var timestamp int64
		var value float64
		var metricName, serviceName, attributes, resourceAttributes string
		err := result.Scan(&timestamp, &value, &metricName, &serviceName, &attributes, &resourceAttributes)

		if err != nil {
			return nil, err
		}
		samples = append(samples, prompb.Sample{
			Timestamp: timestamp,
			Value:     value,
		})
		tsResults = append(tsResults, &prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "MetricName", Value: metricName},
				{Name: "ServiceName", Value: serviceName},
				{Name: "Attributes", Value: attributes},
				{Name: "ResourceAttributes", Value: resourceAttributes},
			},
			Samples: samples,
		})
	}
	return &prompb.QueryResult{
		Timeseries: tsResults,
	}, nil
}

func convertLabels(lbls labels.Labels) []*prompb.Label {
	var result []*prompb.Label
	for _, lbl := range lbls {
		result = append(result, &prompb.Label{
			Name:  lbl.Name,
			Value: lbl.Value,
		})
	}
	return result
}

// Type implements storage.QueryableClient
func (r *Reader) Type() string {
	return "clickhouse"
}
