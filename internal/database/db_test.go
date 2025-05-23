package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"math/rand"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/prometheus/prometheus/model/labels"
)

func TestInsertMetric(t *testing.T) {
	ctx := context.Background()
	dbDir := t.TempDir()
	db, err := Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fromTS, err := time.ParseInLocation(time.RFC3339, "2025-01-01T00:00:00Z", time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	toTS, err := time.ParseInLocation(time.RFC3339, "2025-01-02T00:00:00Z", time.UTC)
	if err != nil {
		t.Fatal(err)
	}

	namespace := "test_namespace"
	err = db.RecordMetric(ctx, model.Metric{
		Namespace:  namespace,
		MetricName: "test_name",
		Region:     "test_region",
		Dimensions: []model.Dimension{
			{
				Name:  "dim1",
				Value: "dim_value1",
			},
		},
		FromTS: fromTS,
		ToTS:   toTS,
	})
	if err != nil {
		t.Fatal(err)
	}

	// check metrics table
	var rows *sql.Rows
	dbPath := fmt.Sprintf(DbPathPattern, "_20241111_20250202")
	rows, err = db.dbCache[dbPath].db.QueryContext(ctx, "SELECT * FROM metrics_20241111_20250202")
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()

	var metric model.Metric
	var dim []byte
	var from int64
	var to int64
	var updatedAt int64
	err = rows.Scan(&metric.MetricID, &metric.Namespace, &metric.MetricName, &metric.Region, &dim, &from, &to, &updatedAt)
	if err != nil {
		t.Fatal(err)
	}

	err = json.Unmarshal(dim, &metric.Dimensions)
	if err != nil {
		t.Fatal(err)
	}
	metric.FromTS = time.Unix(from, 0).UTC()
	metric.ToTS = time.Unix(to, 0).UTC()
	metric.UpdatedAt = time.Unix(updatedAt, 0).UTC()
	if metric.MetricID != 1 ||
		metric.Namespace != namespace ||
		metric.MetricName != "test_name" ||
		metric.Region != "test_region" ||
		len(metric.Dimensions) != 1 ||
		metric.Dimensions[0].Name != "dim1" ||
		metric.Dimensions[0].Value != "dim_value1" ||
		!metric.FromTS.Equal(fromTS) ||
		!metric.ToTS.Equal(toTS) {
		t.Fatalf("unexpected row: %+v", metric)
	}

	hasNext := rows.Next()
	if hasNext != false {
		t.Fatal("expected no more rows")
	}

	// check metrics_lifetime table
	rows, err = db.dbCache[dbPath].db.QueryContext(ctx, "SELECT * FROM metrics_lifetime_20241111_20250202"+"_"+namespace)
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()

	var lifetime model.MetricLifetime
	err = rows.Scan(&lifetime.MetricID, &from, &to)
	if err != nil {
		t.Fatal(err)
	}
	lifetime.FromTS = time.Unix(from, 0).UTC()
	lifetime.ToTS = time.Unix(to, 0).UTC()

	if lifetime.MetricID != 1 || !lifetime.FromTS.Equal(fromTS) || !lifetime.ToTS.Equal(toTS) {
		t.Fatalf("unexpected row: %+v", lifetime)
	}

	hasNext = rows.Next()
	if hasNext != false {
		t.Fatal("expected no more rows")
	}
}

func TestUpdateMetric(t *testing.T) {
	ctx := context.Background()
	dbDir := t.TempDir()
	db, err := Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fromTS, err := time.ParseInLocation(time.RFC3339, "2025-01-01T00:00:00Z", time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	toTS, err := time.ParseInLocation(time.RFC3339, "2025-01-02T00:00:00Z", time.UTC)
	if err != nil {
		t.Fatal(err)
	}

	namespace := "test_namespace"
	err = db.RecordMetric(ctx, model.Metric{
		Namespace:  namespace,
		MetricName: "test_name",
		Region:     "test_region",
		Dimensions: []model.Dimension{
			{
				Name:  "dim1",
				Value: "dim_value1",
			},
		},
		FromTS: fromTS.Add(1 * time.Second),
		ToTS:   toTS.Add(-1 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.RecordMetric(ctx, model.Metric{
		Namespace:  namespace,
		MetricName: "test_name",
		Region:     "test_region",
		Dimensions: []model.Dimension{
			{
				Name:  "dim1",
				Value: "dim_value1",
			},
		},
		FromTS: fromTS,
		ToTS:   toTS,
	})
	if err != nil {
		t.Fatal(err)
	}

	// check metrics table
	var rows *sql.Rows
	dbPath := fmt.Sprintf(DbPathPattern, "_20241111_20250202")
	rows, err = db.dbCache[dbPath].db.QueryContext(ctx, "SELECT * FROM metrics_20241111_20250202")
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()

	var metric model.Metric
	var dim []byte
	var from int64
	var to int64
	var updatedAt int64
	err = rows.Scan(&metric.MetricID, &metric.Namespace, &metric.MetricName, &metric.Region, &dim, &from, &to, &updatedAt)
	if err != nil {
		t.Fatal(err)
	}

	err = json.Unmarshal(dim, &metric.Dimensions)
	if err != nil {
		t.Fatal(err)
	}
	metric.FromTS = time.Unix(from, 0).UTC()
	metric.ToTS = time.Unix(to, 0).UTC()
	metric.UpdatedAt = time.Unix(updatedAt, 0).UTC()
	if metric.MetricID != 1 ||
		metric.Namespace != namespace ||
		metric.MetricName != "test_name" ||
		metric.Region != "test_region" ||
		len(metric.Dimensions) != 1 ||
		metric.Dimensions[0].Name != "dim1" ||
		metric.Dimensions[0].Value != "dim_value1" ||
		!metric.FromTS.Equal(fromTS) ||
		!metric.ToTS.Equal(toTS) {
		t.Fatalf("unexpected row: %+v", metric)
	}

	hasNext := rows.Next()
	if hasNext != false {
		t.Fatal("expected no more rows")
	}

	// check metrics_lifetime table
	rows, err = db.dbCache[dbPath].db.QueryContext(ctx, "SELECT * FROM metrics_lifetime_20241111_20250202"+"_"+namespace)
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()

	var lifetime model.MetricLifetime
	err = rows.Scan(&lifetime.MetricID, &from, &to)
	if err != nil {
		t.Fatal(err)
	}
	lifetime.FromTS = time.Unix(from, 0).UTC()
	lifetime.ToTS = time.Unix(to, 0).UTC()

	if lifetime.MetricID != 1 || !lifetime.FromTS.Equal(fromTS) || !lifetime.ToTS.Equal(toTS) {
		t.Fatalf("unexpected row: %+v", lifetime)
	}

	hasNext = rows.Next()
	if hasNext != false {
		t.Fatal("expected no more rows")
	}
}

func TestInsertInvalidMetric(t *testing.T) {
	ctx := context.Background()
	dbDir := t.TempDir()
	db, err := Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fromTS, err := time.ParseInLocation(time.RFC3339, "2025-01-01T00:00:00Z", time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	toTS, err := time.ParseInLocation(time.RFC3339, "2025-01-02T00:00:00Z", time.UTC)
	if err != nil {
		t.Fatal(err)
	}

	namespace := "test_namespace"
	err = db.RecordMetric(ctx, model.Metric{
		Namespace:  namespace,
		MetricName: "test_name",
		Region:     "test_region",
		Dimensions: []model.Dimension{
			{
				Name:  "dim1",
				Value: "dim_value1",
			},
		},
		FromTS: fromTS,
		ToTS:   toTS,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.RecordMetric(ctx, model.Metric{
		Namespace:  namespace,
		MetricName: "test_name",
		Region:     "test_region",
		Dimensions: []model.Dimension{
			{
				Name:  "dim1",
				Value: "dim_value1",
			},
		},
		FromTS: toTS,
		ToTS:   fromTS,
	})
	if err == nil {
		t.Fatal("expected rtree constraint failed")
	}
}

func TestQueryMetrics(t *testing.T) {
	ctx := context.Background()
	dbDir := t.TempDir()
	db, err := Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	baseFromTS, err := time.ParseInLocation(time.RFC3339, "2025-01-01T00:00:00Z", time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	baseToTS, err := time.ParseInLocation(time.RFC3339, "2025-01-02T00:00:00Z", time.UTC)
	if err != nil {
		t.Fatal(err)
	}

	generateMetrics := func(ns, n, r, dn, dv string, f, t time.Time) model.Metric {
		return model.Metric{
			Namespace:  ns,
			MetricName: n,
			Region:     r,
			Dimensions: []model.Dimension{
				{
					Name:  dn,
					Value: dv,
				},
			},
			FromTS: f,
			ToTS:   t,
		}
	}

	fromTS := baseFromTS
	toTS := baseToTS
	fromTS2 := baseFromTS.Add(1 * 12 * time.Hour)
	toTS2 := baseToTS.Add(1 * 12 * time.Hour)
	fromTS3 := baseFromTS.Add(-PartitionInterval * 6)
	toTS3 := fromTS3.Add(PartitionInterval * 3)
	metrics := map[string]model.Metric{
		"lm1": generateMetrics("label_match", "test_name", "test_region", "dim1", "dim_value1", fromTS, toTS),
		"lm2": generateMetrics("label_match2", "test_name2", "test_region2", "dim2", "dim_value2", fromTS, toTS),
		"lm3": generateMetrics("label_match2", "test_name2", "test_region2", "dim2", "dim_value3", fromTS, toTS),
		"lm4": generateMetrics("label_match", "test_name", "test_region", "dim3", "dim_value3", fromTS2, toTS2),
		"tm1": generateMetrics("time_range_match", "test_name", "test_region", "dim1", "dim_value1", fromTS, toTS),
		"tm2": generateMetrics("time_range_match", "test_name", "test_region", "dim3", "dim_value3", fromTS2, toTS2),
		"tm3": generateMetrics("time_range_match", "test_name", "test_region", "dim4", "dim_value4", fromTS3, toTS3),
		"im1": generateMetrics("safe_metric_name_match", "0test-name", "test_region", "dim1", "dim_value1", fromTS, toTS),
	}
	for _, m := range metrics {
		err = db.RecordMetric(ctx, m)
		if err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name string
		from time.Time
		to   time.Time
		lm   []*labels.Matcher
		want []model.Metric
	}{
		{
			name: "[label] exact match 1",
			from: fromTS,
			to:   toTS,
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "label_match"),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_name"),
				labels.MustNewMatcher(labels.MatchEqual, "Region", "test_region"),
				labels.MustNewMatcher(labels.MatchEqual, "dim1", "dim_value1"),
			},
			want: []model.Metric{
				metrics["lm1"],
			},
		},
		{
			name: "[label] exact match 2",
			from: fromTS,
			to:   toTS,
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "label_match2"),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_name2"),
				labels.MustNewMatcher(labels.MatchEqual, "Region", "test_region2"),
				labels.MustNewMatcher(labels.MatchEqual, "dim2", "dim_value2"),
			},
			want: []model.Metric{
				metrics["lm2"],
			},
		},
		{
			name: "[label] partly match 1",
			from: fromTS,
			to:   toTS,
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "label_match2"),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_name2"),
				labels.MustNewMatcher(labels.MatchEqual, "Region", "test_region2"),
			},
			want: []model.Metric{
				metrics["lm2"],
				metrics["lm3"],
			},
		},
		{
			name: "[label] regex match 1",
			from: fromTS,
			to:   toTS,
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "label_match"),
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "^test_.*$"),
				labels.MustNewMatcher(labels.MatchRegexp, "Region", "^test_.*$"),
				labels.MustNewMatcher(labels.MatchRegexp, "dim1", "^dim_value.*$"),
			},
			want: []model.Metric{
				metrics["lm1"],
			},
		},
		{
			name: "[label] regex match 2",
			from: fromTS,
			to:   toTS,
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "label_match2"),
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "^test_.*$"),
				labels.MustNewMatcher(labels.MatchRegexp, "Region", "^test_.*$"),
			},
			want: []model.Metric{
				metrics["lm2"],
				metrics["lm3"],
			},
		},
		{
			name: "[time range] match 1",
			from: fromTS2,
			to:   fromTS2.Add(1 * time.Hour),
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "time_range_match"),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_name"),
				labels.MustNewMatcher(labels.MatchEqual, "Region", "test_region"),
			},
			want: []model.Metric{
				metrics["tm1"],
				metrics["tm2"],
			},
		},
		{
			name: "[time range] match 2",
			from: fromTS.Add(-2 * time.Second),
			to:   fromTS,
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "time_range_match"),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_name"),
				labels.MustNewMatcher(labels.MatchEqual, "Region", "test_region"),
			},
			want: []model.Metric{
				metrics["tm1"],
			},
		},
		{
			name: "[time range] match 3",
			from: fromTS.Add(-2 * time.Second),
			to:   fromTS.Add(-1 * time.Second),
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "time_range_match"),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_name"),
				labels.MustNewMatcher(labels.MatchEqual, "Region", "test_region"),
			},
			want: []model.Metric{},
		},
		{
			name: "[time range] match 4",
			from: toTS2,
			to:   toTS2.Add(2 * time.Second),
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "time_range_match"),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_name"),
				labels.MustNewMatcher(labels.MatchEqual, "Region", "test_region"),
			},
			want: []model.Metric{
				metrics["tm2"],
			},
		},
		{
			name: "[time range] match 5",
			from: toTS2.Add(1 * time.Second),
			to:   toTS2.Add(2 * time.Second),
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "time_range_match"),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_name"),
				labels.MustNewMatcher(labels.MatchEqual, "Region", "test_region"),
			},
			want: []model.Metric{},
		},
		{
			name: "[time range] match long lifetime",
			from: fromTS3,
			to:   toTS3,
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "time_range_match"),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_name"),
				labels.MustNewMatcher(labels.MatchEqual, "Region", "test_region"),
				labels.MustNewMatcher(labels.MatchEqual, "dim4", "dim_value4"),
			},
			want: []model.Metric{
				metrics["tm3"],
			},
		},
		{
			name: "[safe metric name] exact match 1",
			from: fromTS,
			to:   toTS,
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "safe_metric_name_match"),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "0test-name"),
				labels.MustNewMatcher(labels.MatchEqual, "Region", "test_region"),
				labels.MustNewMatcher(labels.MatchEqual, "dim1", "dim_value1"),
			},
			want: []model.Metric{
				metrics["im1"],
			},
		},
		{
			name: "[safe metric name] exact match 2",
			from: fromTS,
			to:   toTS,
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "Namespace", "safe_metric_name_match"),
				labels.MustNewMatcher(labels.MatchEqual, "MetricName", "0test-name"),
				labels.MustNewMatcher(labels.MatchEqual, "Region", "test_region"),
				labels.MustNewMatcher(labels.MatchEqual, "dim1", "dim_value1"),
			},
			want: []model.Metric{
				metrics["im1"],
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.QueryMetrics(ctx, tt.from, tt.to, tt.lm, 0, map[string]*model.Metric{})
			if err != nil {
				t.Fatal(err)
			}
			got := []*model.Metric{}
			for _, metric := range result {
				got = append(got, metric)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("unexpected length: got=%d, want=%d", len(got), len(tt.want))
			}
			sort.Slice(got, func(i, j int) bool {
				return got[i].MetricID < got[j].MetricID
			})
			for i := range got {
				if !got[i].Equal(tt.want[i]) {
					t.Fatalf("unexpected query results: got=%+v, want=%+v", got[i], tt.want[i])
				}
			}
		})
	}
}

func BenchmarkInsert10000Metrics(b *testing.B) {
	ctx := context.Background()
	dbDir := b.TempDir()
	db, err := Open(dbDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	for i := 0; i < 2; i++ {
		for j := 0; j < 10000; j++ {
			fromTS := now.Add(-1 * time.Duration(rand.Intn(365*24*60*60)) * time.Second)
			if i == 0 {
				fromTS = fromTS.Add(-365 * 24 * 60 * 60 * time.Second)
			}
			toTS := fromTS.Add(time.Duration(rand.Intn(60*60)+1) * time.Second)
			err = db.RecordMetric(ctx, model.Metric{
				Namespace:  "test_namespace",
				MetricName: "test_name",
				Region:     "test_region",
				Dimensions: []model.Dimension{
					{
						Name:  "dim1",
						Value: fmt.Sprintf("dim_value%d", j),
					},
				},
				FromTS: fromTS,
				ToTS:   toTS,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
