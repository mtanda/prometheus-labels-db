package recorder

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"golang.org/x/time/rate"
)

var scrapeInterval = 10 * time.Minute

type CloudWatchAPI interface {
	cloudwatch.ListMetricsAPIClient
}

type CloudWatchScraper struct {
	cwClient   CloudWatchAPI
	region     string
	namespaces []string
	metricsCh  chan model.Metric
	limiter    *rate.Limiter
	cancel     context.CancelFunc
	done       chan struct{}
}

func NewCloudWatchScraper(client CloudWatchAPI, region string, ns []string, ch chan model.Metric, limiter *rate.Limiter) *CloudWatchScraper {
	return &CloudWatchScraper{
		cwClient:   client,
		region:     region,
		namespaces: ns,
		metricsCh:  ch,
		limiter:    limiter,
		done:       make(chan struct{}),
	}
}

func (c *CloudWatchScraper) Run() {
	var ctx context.Context
	ctx, c.cancel = context.WithCancel(context.Background())

	for _, ns := range c.namespaces {
		err := c.scrape(ctx, ns)
		if err != nil {
			slog.Error("failed to scrape metrics: %s, %v\n", ns, err)
		}
	}

	go func() {
		ticker := time.NewTicker(scrapeInterval)
		defer ticker.Stop()
		defer close(c.done)
		for {
			select {
			case <-ticker.C:
				for _, ns := range c.namespaces {
					err := c.scrape(ctx, ns)
					if err != nil {
						slog.Error("failed to scrape metrics: %s, %v\n", ns, err)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *CloudWatchScraper) scrape(ctx context.Context, ns string) error {
	slog.Info("scraping metrics", "namespace", ns)
	now := time.Now().UTC()

	paginator := cloudwatch.NewListMetricsPaginator(c.cwClient, &cloudwatch.ListMetricsInput{
		Namespace:      aws.String(ns),
		RecentlyActive: "PT3H",
	})
	for paginator.HasMorePages() {
		if err := c.limiter.Wait(ctx); err != nil {
			continue
		}
		output, err := paginator.NextPage(ctx)
		if err != nil {
			continue // ignore error
		}
		for _, m := range output.Metrics {
			dim := make([]model.Dimension, 0, len(m.Dimensions))
			for _, d := range m.Dimensions {
				dim = append(dim, model.Dimension{
					Name:  *d.Name,
					Value: *d.Value,
				})
			}
			c.metricsCh <- model.Metric{
				Namespace:  *m.Namespace,
				MetricName: *m.MetricName,
				Region:     c.region,
				Dimensions: dim,
				FromTS:     now.Add(-(60*3 + 50) * time.Minute),
				ToTS:       now,
				UpdatedAt:  now,
			}
		}
	}
	return nil
}

func (c *CloudWatchScraper) Stop() {
	c.cancel()
	<-c.done
}
