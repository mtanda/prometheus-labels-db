package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/mtanda/prometheus-labels-db/internal/database"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/promql/parser"
)

const (
	unusedDBCheckInterval = 10 * time.Minute
)

func seriesHandler(w http.ResponseWriter, r *http.Request, db *database.LabelDB) {
	query := r.URL.Query()
	matchParam := query["match[]"]
	matchers, err := parser.ParseMetricSelectors(matchParam)
	if err != nil {
		http.Error(w, "invalid match[] parameter: "+err.Error(), http.StatusBadRequest)
		return
	}

	startParam := query.Get("start")
	endParam := query.Get("end")
	parseTime := func(param string) (time.Time, error) {
		t, err := time.ParseInLocation(time.RFC3339, param, time.UTC)
		if err == nil {
			return t, nil
		}
		unixTime, err := strconv.ParseInt(param, 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		return time.Unix(unixTime, 0).UTC(), nil
	}

	start, err := parseTime(startParam)
	if err != nil {
		http.Error(w, "failed to parse start timestamp: "+err.Error(), http.StatusBadRequest)
		return
	}
	end, err := parseTime(endParam)
	if err != nil {
		http.Error(w, "failed to parse end timestamp: "+err.Error(), http.StatusBadRequest)
		return
	}

	ctx := context.Background()

	data := []map[string]string{}
	for _, matcher := range matchers {
		metrics, err := db.QueryMetrics(ctx, start, end, matcher)
		if err != nil {
			http.Error(w, "failed to query metrics: "+err.Error(), http.StatusInternalServerError)
			return
		}

		for _, metric := range metrics {
			data = append(data, metric.Labels())
		}
	}

	response := map[string]interface{}{
		"status": "success",
		"data":   data,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func main() {
	var dbDir string
	flag.StringVar(&dbDir, "db.dir", "./data/", "Path to the database directory")
	var listenAddress string
	flag.StringVar(&listenAddress, "web.listen-address", "0.0.0.0:8080", "Address to listen")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	db, err := database.Open(dbDir)
	if err != nil {
		slog.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// check unused db periodically
	ticker := time.NewTicker(unusedDBCheckInterval)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			err := db.CleanupUnusedDB(context.Background())
			if err != nil {
				slog.Error("failed to cleanup unused DB", "error", err)
			} else {
				slog.Info("cleanup unused DB completed")
			}
		}
	}()

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))

	counter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of requests",
	}, []string{"code", "method"})
	duration := promauto.With(reg).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "A histogram of latencies for requests.",
			Buckets: prometheus.ExponentialBuckets(0.0625, 2, 10),
		}, []string{"handler", "method"})
	responseSize := promauto.With(reg).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_response_size_bytes",
			Help:    "A histogram of response sizes for requests.",
			Buckets: prometheus.ExponentialBuckets(100, 2, 10),
		}, []string{"handler"})
	http.HandleFunc("/api/v1/series", promhttp.InstrumentHandlerDuration(
		duration.MustCurryWith(prometheus.Labels{"handler": "/api/v1/series"}),
		promhttp.InstrumentHandlerCounter(
			counter,
			promhttp.InstrumentHandlerResponseSize(
				responseSize.MustCurryWith(prometheus.Labels{"handler": "/api/v1/series"}),
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					seriesHandler(w, r, db)
				}),
			),
		),
	))
	slog.Info("Starting server", "address", listenAddress)
	err = http.ListenAndServe(listenAddress, nil)
	if err != nil {
		slog.Error("failed to start server", "error", err)
		os.Exit(1)
	}
}
