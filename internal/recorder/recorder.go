package recorder

import (
	"context"
	"fmt"

	"github.com/mtanda/prometheus-labels-db/internal/database"
	"github.com/mtanda/prometheus-labels-db/internal/model"
)

const (
	MaxRetry = 3
)

type Recorder struct {
	ldb       *database.LabelDB
	metricsCh chan model.Metric
	done      chan struct{}
}

func New(ldb *database.LabelDB, ch chan model.Metric) *Recorder {
	return &Recorder{
		ldb:       ldb,
		metricsCh: ch,
		done:      make(chan struct{}),
	}
}

func (r *Recorder) Run() {
	ctx := context.TODO()
	go func() {
		defer close(r.done)
		for metric := range r.metricsCh {
			for i := 0; i < MaxRetry; i++ {
				err := r.ldb.RecordMetric(ctx, metric)
				if err != nil {
					fmt.Printf("failed to record metric: %v, %v\n", metric, err)
				} else {
					break
				}
			}
		}
	}()
}

func (r *Recorder) Stop() {
	<-r.done
}
