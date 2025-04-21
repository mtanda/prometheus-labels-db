package recorder

import (
	"context"
	"log/slog"
	"time"

	"github.com/mtanda/prometheus-labels-db/internal/database"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	MaxRetry              = 3
	WALCheckpointInterval = 6 * 60 * time.Minute
)

type Recorder struct {
	ldb                    *database.LabelDB
	metricsCh              chan model.Metric
	done                   chan struct{}
	recordTotal            *prometheus.CounterVec
	recordDurations        prometheus.Histogram
	walCheckpointTotal     *prometheus.CounterVec
	walCheckpointDurations prometheus.Histogram
}

func New(ldb *database.LabelDB, ch chan model.Metric, registry *prometheus.Registry) *Recorder {
	recordTotal := promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
		Name: "recorder_record_total",
		Help: "Total number of recording metrics operations",
	}, []string{"status"})
	recordDurations := promauto.With(registry).NewHistogram(prometheus.HistogramOpts{
		Name:    "recorder_record_duration_seconds",
		Help:    "Duration of recording metrics in seconds",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 20),
	})
	walCheckpointTotal := promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
		Name: "recorder_wal_checkpoint_total",
		Help: "Total number of wal checkpoint operations",
	}, []string{"status"})
	walCheckpointDurations := promauto.With(registry).NewHistogram(prometheus.HistogramOpts{
		Name:    "recorder_wal_checkpoint_duration_seconds",
		Help:    "Duration of wal checkpoint in seconds",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 20),
	})
	return &Recorder{
		ldb:                    ldb,
		metricsCh:              ch,
		done:                   make(chan struct{}),
		recordTotal:            recordTotal,
		recordDurations:        recordDurations,
		walCheckpointTotal:     walCheckpointTotal,
		walCheckpointDurations: walCheckpointDurations,
	}
}

func (r *Recorder) Run() {
	ctx := context.TODO()
	go func() {
		defer close(r.done)
		checkpointTicker := time.NewTicker(WALCheckpointInterval)
		defer checkpointTicker.Stop()

		// set initial counter value
		r.recordTotal.WithLabelValues("success")
		r.recordTotal.WithLabelValues("error")
		r.walCheckpointTotal.WithLabelValues("success")
		r.walCheckpointTotal.WithLabelValues("error")

		for {
			select {
			case metric, ok := <-r.metricsCh:
				if !ok {
					// channel is closed, stop the recorder
					return
				}
				for i := 0; i < MaxRetry; i++ {
					now := time.Now()
					err := r.ldb.RecordMetric(ctx, metric)
					if err != nil {
						// ignore error
						slog.Error("failed to record metric", "metric", metric, "error", err, "retry", i+1)
						r.recordTotal.WithLabelValues("error").Inc()
					} else {
						r.recordTotal.WithLabelValues("success").Inc()
						r.recordDurations.Observe(time.Since(now).Seconds())
						break
					}
				}
			case <-checkpointTicker.C:
				slog.Info("WAL checkpoint triggered")
				now := time.Now()
				err := r.ldb.WalCheckpoint(ctx)
				if err != nil {
					// ignore error
					slog.Error("failed to WAL checkpoint", "error", err)
					r.walCheckpointTotal.WithLabelValues("error").Inc()
				} else {
					slog.Info("WAL checkpoint completed")
					r.walCheckpointTotal.WithLabelValues("success").Inc()
					r.walCheckpointDurations.Observe(time.Since(now).Seconds())
				}
			}
		}
	}()
}

func (r *Recorder) Stop() {
	<-r.done
	slog.Info("stopped recorder")
}
