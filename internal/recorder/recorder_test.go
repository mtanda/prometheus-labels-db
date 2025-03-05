package recorder

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mtanda/prometheus-labels-db/internal/database"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/prometheus/prometheus/model/labels"
)

func TestRecord(t *testing.T) {
	ctx := context.Background()
	chanLength := 10
	metricsCount := chanLength * 2

	var wg sync.WaitGroup
	dbDir := t.TempDir()
	ldb, err := database.Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	metricsCh := make(chan model.Metric, chanLength)
	recorder := New(ldb, metricsCh)
	recorder.Run(&wg)

	now := time.Now().UTC()
	from := now.Add(-1 * time.Hour)
	to := now
	for i := 0; i < metricsCount; i++ {
		metricsCh <- model.Metric{
			Namespace:  "test_namespace",
			MetricName: "test_name",
			Region:     "test_region",
			Dimensions: model.Dimensions{
				{
					Name:  "dim1",
					Value: fmt.Sprintf("dim_value%d", i),
				},
			},
			FromTS:    from,
			ToTS:      to,
			UpdatedAt: now,
		}
	}
	close(metricsCh)
	wg.Wait()

	result, err := ldb.QueryMetrics(ctx, from, to, []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "namespace", "test_namespace"),
	})
	if len(result) != metricsCount {
		t.Fatalf("unexpected metrics count: %d", len(result))
	}
}
