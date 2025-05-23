package recorder

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"
)

var scrapeInterval = 60 * time.Minute

type CloudWatchAPI interface {
	cloudwatch.ListMetricsAPIClient
}

type CloudWatchScraper struct {
	cwClient            CloudWatchAPI
	region              string
	namespaces          []string
	metricsCh           chan model.Metric
	limiter             *rate.Limiter
	cancel              context.CancelFunc
	done                chan struct{}
	scrapeMetricsTotal  *prometheus.CounterVec
	scrapeWarningsTotal prometheus.Counter
	apiCallsTotal       *prometheus.CounterVec
}

func NewCloudWatchScraper(client CloudWatchAPI, region string, ns []string, ch chan model.Metric, limiter *rate.Limiter, registry *prometheus.Registry) *CloudWatchScraper {
	reg := prometheus.WrapRegistererWith(
		prometheus.Labels{"region": region},
		registry,
	)
	scrapeMetricsTotal := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "scraper_metrics_total",
		Help: "Total number of scraped metrics",
	}, []string{"namespace"})
	scrapeWarningsTotal := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "scraper_scrape_warnings_total",
		Help: "Total number of metrics scrape warnings",
	})
	apiCallsTotal := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "scraper_cloudwatch_api_calls_total",
		Help: "Total number of CloudWatch API calls",
	}, []string{"api", "namespace", "status"})
	return &CloudWatchScraper{
		cwClient:            client,
		region:              region,
		namespaces:          ns,
		metricsCh:           ch,
		limiter:             limiter,
		done:                make(chan struct{}),
		scrapeMetricsTotal:  scrapeMetricsTotal,
		scrapeWarningsTotal: scrapeWarningsTotal,
		apiCallsTotal:       apiCallsTotal,
	}
}

func (c *CloudWatchScraper) Run() {
	var ctx context.Context
	ctx, c.cancel = context.WithCancel(context.Background())

	go func() {
		for _, ns := range c.namespaces {
			err := c.scrape(ctx, ns)
			if err != nil {
				// ignore error
				slog.Error("failed to scrape metrics", "error", err, "namespace", ns)
				c.scrapeWarningsTotal.Inc()
			}
		}

		ticker := time.NewTicker(scrapeInterval)
		defer ticker.Stop()
		defer close(c.done)
		for {
			select {
			case <-ticker.C:
				for _, ns := range c.namespaces {
					err := c.scrape(ctx, ns)
					if err != nil {
						// ignore error
						slog.Error("failed to scrape metrics", "error", err, "namespace", ns)
						c.scrapeWarningsTotal.Inc()
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *CloudWatchScraper) Oneshot(wg *sync.WaitGroup) {
	var ctx context.Context
	ctx, c.cancel = context.WithCancel(context.Background())

	wg.Add(1)
	go func() {
		defer close(c.done)
		defer wg.Done()

		// set initial counter value
		for _, ns := range c.namespaces {
			c.apiCallsTotal.WithLabelValues("ListMetrics", ns, "success")
			c.apiCallsTotal.WithLabelValues("ListMetrics", ns, "error")
			c.scrapeMetricsTotal.WithLabelValues(ns)
		}
		time.Sleep(60 * time.Second) // wait for 60 seconds to scrape metrics

		for _, ns := range c.namespaces {
			err := c.scrape(ctx, ns)
			if err != nil {
				// ignore error
				slog.Error("failed to scrape metrics", "error", err, "namespace", ns)
				c.scrapeWarningsTotal.Inc()
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
			// ignore error
			slog.Error("failed to wait for limiter", "error", err)
			c.scrapeWarningsTotal.Inc()
			continue
		}
		output, err := paginator.NextPage(ctx)
		if err != nil {
			// ignore error
			slog.Error("failed to list metrics", "error", err, "namespace", ns)
			c.apiCallsTotal.WithLabelValues("ListMetrics", ns, "error").Inc()
			c.scrapeWarningsTotal.Inc()
			break
		}
		c.apiCallsTotal.WithLabelValues("ListMetrics", ns, "success").Inc()
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
				// https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_ListMetrics.html
				// There is a low probability that the returned results include metrics with last published data as much as 50 minutes more than the specified time interval.
				FromTS:    now.Add(-(60*3 + 50) * time.Minute),
				ToTS:      now,
				UpdatedAt: now,
			}
			c.scrapeMetricsTotal.WithLabelValues(ns).Inc()
		}
	}
	return nil
}

func (c *CloudWatchScraper) Stop() {
	c.cancel()
	<-c.done
	slog.Info("stopped CloudWatch scraper", "region", c.region, "namespaces", c.namespaces)
}
