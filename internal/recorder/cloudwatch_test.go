package recorder

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
)

type mockCloudWatchAPI struct {
}

func (*mockCloudWatchAPI) ListMetrics(ctx context.Context, params *cloudwatch.ListMetricsInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.ListMetricsOutput, error) {
	return &cloudwatch.ListMetricsOutput{
		Metrics: []types.Metric{
			{
				Namespace:  aws.String("test_namespace"),
				MetricName: aws.String("test_name"),
				Dimensions: []types.Dimension{
					{
						Name:  aws.String("dim1"),
						Value: aws.String("dim_value1"),
					},
				},
			},
		},
	}, nil
}

func TestScrape(t *testing.T) {
	// TODO rewrite by using synctest
	scrapeInterval = 10 * time.Second
	client := &mockCloudWatchAPI{}
	metricsCh := make(chan model.Metric, 10)
	limiter := rate.NewLimiter(10000, 1)
	reg := prometheus.NewRegistry()
	recorder := NewCloudWatchScraper(client, "test_region", []string{"test_namespace"}, metricsCh, limiter, reg)
	recorder.Run()
	time.Sleep(3 * time.Second)
	recorder.Stop()
	close(metricsCh)
	metrics := make([]model.Metric, 0, 10)
	for metric := range metricsCh {
		metrics = append(metrics, metric)
	}
	if len(metrics) != 1 {
		t.Fatalf("unexpected metrics count: %d", len(metrics))
	}
}
