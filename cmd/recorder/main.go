package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/mtanda/prometheus-labels-db/internal/database"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/mtanda/prometheus-labels-db/internal/recorder"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

type Recorder struct {
	metricsCh chan model.Metric
	limiter   *rate.Limiter
	registry  *prometheus.Registry
	scraper   []*recorder.CloudWatchScraper
	recorder  *recorder.Recorder
}

func newRecorder(dbDir string, registry *prometheus.Registry) (*Recorder, error) {
	metricsCh := make(chan model.Metric, 1000)
	ListMetricsDefaultMaxTPS := 25
	limiter := rate.NewLimiter(rate.Limit(ListMetricsDefaultMaxTPS/2), 1)

	if stat, err := os.Stat(dbDir); os.IsNotExist(err) {
		if err := os.MkdirAll(dbDir, 0o777); err != nil {
			return nil, fmt.Errorf("failed to create directory: %v", err)
		}
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("path exists but is not a directory: %s", dbDir)
	}

	ldb, err := database.Open(dbDir)
	if err != nil {
		return nil, err
	}

	recorder := recorder.New(ldb, metricsCh, registry)
	recorder.Run()

	return &Recorder{
		metricsCh: metricsCh,
		limiter:   limiter,
		registry:  registry,
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
	scraper.Run()
	r.scraper = append(r.scraper, scraper)

	return nil
}

func (r *Recorder) stop() {
	for _, s := range r.scraper {
		s.Stop()
	}
	close(r.metricsCh)
	r.recorder.Stop()
}

func main() {
	var dbDir string
	flag.StringVar(&dbDir, "db.dir", "./data/", "Path to the database directory")
	var configFile string
	flag.StringVar(&configFile, "config.file", "config.yaml", "Path to the config file")
	var listenAddress string
	flag.StringVar(&listenAddress, "web.listen-address", "0.0.0.0:8081", "Address to listen")
	flag.Parse()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	reg := prometheus.NewRegistry()
	go func() {
		reg.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)
		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
		http.ListenAndServe(listenAddress, nil)
	}()

	recorder, err := newRecorder(dbDir, reg)
	if err != nil {
		slog.Error("failed to create recorder", "error", err)
		os.Exit(1)
	}

	cfg, err := model.LoadConfig(configFile)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var errgrp errgroup.Group
	for _, target := range cfg.Targets {
		target := target // capture range variable
		errgrp.Go(func() error {
			err := recorder.addTarget(target)
			if err != nil {
				slog.Error("failed to add target", "target", target, "error", err)
				return err
			}
			<-ctx.Done()
			return nil
		})
	}

	<-sig
	slog.Info("received signal, stopping the recorder...")
	cancel()
	err = errgrp.Wait()
	if err != nil {
		slog.Error("error adding target", "error", err)
		os.Exit(1)
	}
	recorder.stop()
	slog.Info("recorder stopped successfully")
}
