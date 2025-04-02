package recorder

import (
	"context"
	"fmt"
	"time"

	"github.com/mtanda/prometheus-labels-db/internal/database"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	MaxRetry = 3
)

type Recorder struct {
	ldb             *database.LabelDB
	metricsCh       chan model.Metric
	done            chan struct{}
	recordTotal     *prometheus.CounterVec
	recordDurations prometheus.Histogram
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
	return &Recorder{
		ldb:             ldb,
		metricsCh:       ch,
		done:            make(chan struct{}),
		recordTotal:     recordTotal,
		recordDurations: recordDurations,
	}
}

func (r *Recorder) Run() {
	ctx := context.TODO()
	go func() {
		defer close(r.done)
		for metric := range r.metricsCh {
			for i := 0; i < MaxRetry; i++ {
				now := time.Now()
				err := r.ldb.RecordMetric(ctx, metric)
				if err != nil {
					fmt.Printf("failed to record metric: %v, %v\n", metric, err)
					r.recordTotal.WithLabelValues("error").Inc()
				} else {
					r.recordTotal.WithLabelValues("success").Inc()
					r.recordDurations.Observe(time.Since(now).Seconds())
					break
				}
			}
		}
	}()
}

func (r *Recorder) Stop() {
	<-r.done
}
