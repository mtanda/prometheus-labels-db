package fresh_metrics

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/time/rate"
)

const (
	maxCacheSize = 100
	cacheTTL     = 5 * time.Minute
)

type CloudWatchAPI interface {
	cloudwatch.ListMetricsAPIClient
}

type FreshMetrics struct {
	CwClient         map[string]CloudWatchAPI
	limiter          *rate.Limiter
	cache            *expirable.LRU[string, []map[string]string]
	apiCallsTotal    *prometheus.CounterVec
	apiCallDurations prometheus.Histogram
}

func New(limiter *rate.Limiter, registry *prometheus.Registry) *FreshMetrics {
	apiCallsTotal := promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
		Name: "fresh_metrics_cloudwatch_api_calls_total",
		Help: "Total number of CloudWatch API calls",
	}, []string{"region", "api", "namespace", "status"})
	apiCallDurations := promauto.With(registry).NewHistogram(prometheus.HistogramOpts{
		Name:    "fresh_metrics_cloudwatch_api_call_duration_seconds",
		Help:    "Duration of CloudWatch API call in seconds",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 20),
	})
	cache := expirable.NewLRU[string, []map[string]string](maxCacheSize, nil, cacheTTL)
	return &FreshMetrics{
		CwClient:         make(map[string]CloudWatchAPI),
		limiter:          limiter,
		cache:            cache,
		apiCallsTotal:    apiCallsTotal,
		apiCallDurations: apiCallDurations,
	}
}

func (f *FreshMetrics) QueryMetrics(ctx context.Context, lm []*labels.Matcher, result map[string]*model.Metric) (map[string]*model.Metric, error) {
	namespace, metricName, region, dimConditions := parseMatcher(lm)
	if namespace == "" || metricName == "" || region == "" {
		slog.Error("namespace, metricName, and region are required")
		return result, nil
	}

	if _, ok := f.CwClient[region]; !ok {
		awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return nil, err
		}
		client := cloudwatch.NewFromConfig(awsCfg)
		f.CwClient[region] = client
	}

	allDimensions, err := f.getAllDimensions(ctx, region, namespace, metricName)
	if err != nil {
		return nil, err
	}

	// filter by dimension conditions
	filteredDimensions := make([]map[string]string, 0)
	for _, dims := range allDimensions {
		if len(dimConditions) > 0 && !matchAllConditions(dims, dimConditions) {
			continue
		}
		filteredDimensions = append(filteredDimensions, dims)
	}

	now := time.Now().UTC()
	for _, dims := range filteredDimensions {
		m := model.Metric{
			Namespace:  namespace,
			MetricName: metricName,
			Region:     region,
			FromTS:     now.Add(-(60*3 + 50) * time.Minute),
			ToTS:       now,
		}
		for k, v := range dims {
			m.Dimensions = append(m.Dimensions, model.Dimension{
				Name:  k,
				Value: v,
			})
		}
		result[m.UniqueKey()] = &m
	}

	return result, nil
}

func parseMatcher(lm []*labels.Matcher) (string, string, string, []*labels.Matcher) {
	namespace := ""
	metricName := ""
	region := ""
	dimConditions := make([]*labels.Matcher, 0)
	for _, m := range lm {
		switch m.Name {
		case "Namespace":
			namespace = m.Value
		case "__name__":
			metricName = m.Value
		case "MetricName":
			metricName = m.Value
		case "Region":
			region = m.Value
		default:
			dimConditions = append(dimConditions, m)
		}
	}
	return namespace, metricName, region, dimConditions
}

func matchAllConditions(dims map[string]string, dimConditions []*labels.Matcher) bool {
	for _, dc := range dimConditions {
		if _, ok := dims[dc.Name]; !ok {
			return false
		}
		switch dc.Type {
		case labels.MatchEqual:
			if dims[dc.Name] != dc.Value {
				return false
			}
		case labels.MatchNotEqual:
			if dims[dc.Name] == dc.Value {
				return false
			}
		case labels.MatchRegexp:
			r, err := regexp.Compile(dc.Value)
			if err != nil {
				slog.Error("failed to compile regexp", "error", err)
				return false
			}
			if r.Match([]byte(dims[dc.Name])) {
				return false
			}
		case labels.MatchNotRegexp:
			r, err := regexp.Compile(dc.Value)
			if err != nil {
				slog.Error("failed to compile regexp", "error", err)
				return false
			}
			if !r.Match([]byte(dims[dc.Name])) {
				return false
			}
		}
	}
	return true
}

func (f *FreshMetrics) getAllDimensions(ctx context.Context, region string, namespace string, metricName string) ([]map[string]string, error) {
	cacheKey := region + namespace + metricName
	if cache, ok := f.cache.Get(cacheKey); ok {
		return cache, nil
	}
	if rawResult, err := f.listMetrics(ctx, region, namespace, metricName); err != nil {
		return nil, err
	} else {
		result := f.convertResult(rawResult)
		f.cache.Add(cacheKey, result)
		return result, nil
	}
}

func (f *FreshMetrics) convertResult(output *cloudwatch.ListMetricsOutput) []map[string]string {
	result := make([]map[string]string, 0, len(output.Metrics))
	for _, m := range output.Metrics {
		dims := make(map[string]string)
		sort.Slice(m.Dimensions, func(i, j int) bool {
			return *m.Dimensions[i].Name < *m.Dimensions[j].Name
		})
		for _, d := range m.Dimensions {
			dims[*d.Name] = *d.Value
		}
		result = append(result, dims)
	}
	return result
}

func (f *FreshMetrics) listMetrics(ctx context.Context, region string, namespace string, metricName string) (*cloudwatch.ListMetricsOutput, error) {
	result := &cloudwatch.ListMetricsOutput{}

	input := &cloudwatch.ListMetricsInput{
		Namespace:      aws.String(namespace),
		MetricName:     aws.String(metricName),
		RecentlyActive: "PT3H",
	}
	client, ok := f.CwClient[region]
	if !ok {
		return nil, fmt.Errorf("CloudWatch client not found for region: %s", region)
	}
	now := time.Now().UTC()
	paginator := cloudwatch.NewListMetricsPaginator(client, input)
	for paginator.HasMorePages() {
		if err := f.limiter.Wait(ctx); err != nil {
			return result, err
		}
		output, err := paginator.NextPage(ctx)
		if err != nil {
			f.apiCallsTotal.WithLabelValues(region, "ListMetrics", namespace, "error").Inc()
			return result, err
		}
		f.apiCallsTotal.WithLabelValues(region, "ListMetrics", namespace, "success").Inc()
		result.Metrics = append(result.Metrics, output.Metrics...)
	}
	f.apiCallDurations.Observe(time.Since(now).Seconds())
	return result, nil
}
