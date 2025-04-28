package main

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/mtanda/prometheus-labels-db/internal/database"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/mtanda/prometheus-labels-db/internal/recorder"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
)

type Recorder struct {
	metricsCh chan model.Metric
	limiter   *rate.Limiter
	registry  *prometheus.Registry
	ldb       *database.LabelDB
	scraper   []*recorder.CloudWatchScraper
	recorder  *recorder.Recorder
}

func newRecorder(ldb *database.LabelDB, registry *prometheus.Registry) (*Recorder, error) {
	metricsCh := make(chan model.Metric, 1000)
	ListMetricsDefaultMaxTPS := 25
	limiter := rate.NewLimiter(rate.Limit(ListMetricsDefaultMaxTPS/2), 1)

	recorder := recorder.New(ldb, metricsCh, registry)
	recorder.Run()

	return &Recorder{
		metricsCh: metricsCh,
		limiter:   limiter,
		registry:  registry,
		ldb:       ldb,
		recorder:  recorder,
	}, nil
}

func (r *Recorder) addTarget(target model.Target) error {
	awsCfg, err := config.LoadDefaultConfig(context.Background(), config.WithEC2IMDSRegion())
	if err != nil {
		return err
	}
	client := cloudwatch.NewFromConfig(awsCfg)

	scraper := recorder.NewCloudWatchScraper(client, target.Region, target.Namespace, r.metricsCh, r.limiter, r.registry)
	r.scraper = append(r.scraper, scraper)

	return nil
}

func (r *Recorder) run() {
	for _, s := range r.scraper {
		s.Run()
	}
}

func (r *Recorder) oneshot() {
	var wg sync.WaitGroup
	for _, s := range r.scraper {
		s.Oneshot(&wg)
	}
	wg.Wait()
}

func (r *Recorder) stop() {
	for _, s := range r.scraper {
		s.Stop()
	}
	close(r.metricsCh)
	r.recorder.Stop()
	r.ldb.Close()
}
