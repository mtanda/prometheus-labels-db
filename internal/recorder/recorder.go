package recorder

import (
	"context"
	"fmt"
	"sync"

	"github.com/mtanda/prometheus-labels-db/internal/database"
	"github.com/mtanda/prometheus-labels-db/internal/model"
)

const (
	MaxRetry = 3
)

type Recorder struct {
	ldb       *database.LabelDB
	metricsCh chan model.Metric
}

func New(ldb *database.LabelDB, ch chan model.Metric) *Recorder {
	return &Recorder{
		ldb:       ldb,
		metricsCh: ch,
	}
}

func (r *Recorder) Run(wg *sync.WaitGroup) {
	ctx := context.TODO()
	wg.Add(1)
	go func() {
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
		wg.Done()
	}()
}
