package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/mtanda/prometheus-labels-db/internal/database"
	"github.com/mtanda/prometheus-labels-db/internal/importer"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/mtanda/prometheus-labels-db/internal/recorder"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/tsdb"
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
}

func importOldData(dbDir string, importDB string, importSandbox string, logger *slog.Logger, reg *prometheus.Registry) error {
	ctx := context.Background()

	ldb, err := database.Open(dbDir)
	if err != nil {
		return err
	}
	defer ldb.Close()
	db, err := tsdb.OpenDBReadOnly(
		importDB,
		importSandbox,
		logger,
	)
	if err != nil {
		return err
	}
	defer db.Close()

	importer := importer.New(dbDir, ldb, db, reg)
	err = importer.Import(ctx)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	var dbDir string
	flag.StringVar(&dbDir, "db.dir", "./data/", "Path to the database directory")
	var configFile string
	flag.StringVar(&configFile, "config.file", "config.yaml", "Path to the config file")
	var listenAddress string
	flag.StringVar(&listenAddress, "web.listen-address", "0.0.0.0:8081", "Address to listen")
	var oneshot bool
	flag.BoolVar(&oneshot, "oneshot", false, "Run in oneshot mode")
	// importer
	var importDB string
	flag.StringVar(&importDB, "import.db", "./tsdb/", "Path to the import source database")
	var importSandbox string
	flag.StringVar(&importSandbox, "import.sandbox", "./tsdb_sandbox/", "Path to the sandbox of import source database")
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
		slog.Info("Starting server", "address", listenAddress)
		err := http.ListenAndServe(listenAddress, nil)
		if err != nil {
			slog.Error("failed to start server", "error", err)
			os.Exit(1)
		}
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

	for _, target := range cfg.Targets {
		err := recorder.addTarget(target)
		if err != nil {
			slog.Error("failed to add target", "target", target, "error", err)
			os.Exit(1)
		}
	}

	if oneshot {
		recordLastSuccess := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "recorder_last_record_success_timestamp_seconds",
			Help: "Last success timestamp of recording metrics operations",
		})
		recorder.oneshot()
		recorder.stop()
		recordLastSuccess.Set(float64(time.Now().UTC().Unix()))

		// TODO: remove importer when all imports are completed
		importLastSuccess := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "importer_last_import_success_timestamp_seconds",
			Help: "Last success timestamp of importing metrics operations",
		})
		err = importOldData(dbDir, importDB, importSandbox, logger, reg)
		if err != nil {
			// ignore error
			slog.Error("failed to import", "err", err)
		}
		importLastSuccess.Set(float64(time.Now().UTC().Unix()))

		time.Sleep(60 * time.Second) // wait for 60 seconds to scrape metrics
		slog.Info("oneshot completed")
	} else {
		recorder.run()

		<-sig
		slog.Info("received signal, stopping the recorder...")
		recorder.stop()
		slog.Info("recorder stopped successfully")
	}
}
