package recorder

import (
	"context"
	"log/slog"
	"time"

	"github.com/mtanda/prometheus-labels-db/internal/database"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"
)

const (
	MaxRetry              = 3
	WALCheckpointInterval = 6 * 60 * time.Minute
	recordRateLimit       = 200
)

type Recorder struct {
	ldb                    *database.LabelDB
	metricsCh              chan model.Metric
	limiter                *rate.Limiter
	done                   chan struct{}
	recordTotal            *prometheus.CounterVec
	recordWarningsTotal    prometheus.Counter
	recordDurations        prometheus.Histogram
	walCheckpointTotal     *prometheus.CounterVec
	walCheckpointDurations prometheus.Histogram
}

func New(ldb *database.LabelDB, ch chan model.Metric, registry *prometheus.Registry) *Recorder {
	recordTotal := promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
		Name: "recorder_record_total",
		Help: "Total number of recording metrics operations",
	}, []string{"status"})
	recordWarningsTotal := promauto.With(registry).NewCounter(prometheus.CounterOpts{
		Name: "recorder_record_warnings_total",
		Help: "Total number of recording metrics warnings",
	})
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
	limiter := rate.NewLimiter(rate.Limit(recordRateLimit), 1)
	return &Recorder{
		ldb:                    ldb,
		metricsCh:              ch,
		limiter:                limiter,
		done:                   make(chan struct{}),
		recordTotal:            recordTotal,
		recordWarningsTotal:    recordWarningsTotal,
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
				if err := r.limiter.Wait(ctx); err != nil {
					// ignore error
					slog.Error("failed to wait for limiter", "error", err)
					r.recordWarningsTotal.Inc()
					continue
				}
				for i := 0; i < MaxRetry; i++ {
					now := time.Now().UTC()
					err := r.ldb.RecordMetric(ctx, metric)
					if err != nil {
						// ignore error
						slog.Error("failed to record metric", "error", err, "metric", metric, "retry", i+1)
						r.recordTotal.WithLabelValues("error").Inc()
						sleepDuration := time.Duration(100*(1<<i)) * time.Millisecond // 0.1s, 0.2s, 0.4s, etc.
						time.Sleep(sleepDuration)
					} else {
						r.recordTotal.WithLabelValues("success").Inc()
						r.recordDurations.Observe(time.Since(now).Seconds())
						break
					}
				}
			case <-checkpointTicker.C:
				slog.Info("WAL checkpoint triggered")
				now := time.Now().UTC()
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

				err = r.ldb.CleanupUnusedDB(ctx)
				if err != nil {
					// ignore error
					slog.Error("failed to cleanup unused DB", "error", err)
				} else {
					slog.Info("cleanup unused DB completed")
				}
			}
		}
	}()
}

func (r *Recorder) Stop() {
	<-r.done
	slog.Info("stopped recorder")
}
