package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/mtanda/prometheus-labels-db/internal/database"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/promql/parser"
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
	start, err := time.ParseInLocation(time.RFC3339, startParam, time.UTC)
	if err != nil {
		http.Error(w, "failed to parse start timestamp: "+err.Error(), http.StatusBadRequest)
		return
	}
	end, err := time.ParseInLocation(time.RFC3339, endParam, time.UTC)
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

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))

	http.HandleFunc("/api/v1/series", func(w http.ResponseWriter, r *http.Request) {
		seriesHandler(w, r, db)
	})
	http.ListenAndServe(listenAddress, nil)
}
