package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"math/rand"

	_ "github.com/mattn/go-sqlite3"
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
	err = db.RecordMetric(ctx, Metric{
		Namespace: namespace,
		Name:      "test_name",
		Region:    "test_region",
		Dimensions: []Dimension{
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
	rows, err = db.db.QueryContext(ctx, "SELECT * FROM metrics")
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()

	var metric Metric
	var dim []byte
	var from int64
	var to int64
	var updatedAt int64
	err = rows.Scan(&metric.MetricID, &metric.Namespace, &metric.Name, &metric.Region, &dim, &from, &to, &updatedAt)
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
		metric.Name != "test_name" ||
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
	rows, err = db.db.QueryContext(ctx, "SELECT * FROM metrics_lifetime_20241111_20250202"+"_"+namespace)
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()

	var lifetime MetricLifetime
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
	err = db.RecordMetric(ctx, Metric{
		Namespace: namespace,
		Name:      "test_name",
		Region:    "test_region",
		Dimensions: []Dimension{
			{
				Name:  "dim1",
				Value: "dim_value1",
			},
		},
		FromTS: fromTS,
		ToTS:   toTS.Add(-1 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.RecordMetric(ctx, Metric{
		Namespace: namespace,
		Name:      "test_name",
		Region:    "test_region",
		Dimensions: []Dimension{
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
	rows, err = db.db.QueryContext(ctx, "SELECT * FROM metrics")
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()

	var metric Metric
	var dim []byte
	var from int64
	var to int64
	var updatedAt int64
	err = rows.Scan(&metric.MetricID, &metric.Namespace, &metric.Name, &metric.Region, &dim, &from, &to, &updatedAt)
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
		metric.Name != "test_name" ||
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
	rows, err = db.db.QueryContext(ctx, "SELECT * FROM metrics_lifetime_20241111_20250202"+"_"+namespace)
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()

	var lifetime MetricLifetime
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
	err = db.RecordMetric(ctx, Metric{
		Namespace: namespace,
		Name:      "test_name",
		Region:    "test_region",
		Dimensions: []Dimension{
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
	err = db.RecordMetric(ctx, Metric{
		Namespace: namespace,
		Name:      "test_name",
		Region:    "test_region",
		Dimensions: []Dimension{
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

	fromTS, err := time.ParseInLocation(time.RFC3339, "2025-01-01T00:00:00Z", time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	toTS, err := time.ParseInLocation(time.RFC3339, "2025-01-02T00:00:00Z", time.UTC)
	if err != nil {
		t.Fatal(err)
	}

	generateMetrics := func(ns, n, r, dn, dv string, f, t time.Time) Metric {
		return Metric{
			Namespace: ns,
			Name:      n,
			Region:    r,
			Dimensions: []Dimension{
				{
					Name:  dn,
					Value: dv,
				},
			},
			FromTS: f,
			ToTS:   t,
		}
	}

	fromTS2 := fromTS.Add(1 * 24 * time.Hour)
	toTS2 := toTS.Add(1 * 24 * time.Hour)
	metrics := []Metric{
		generateMetrics("test_namespace", "test_name", "test_region", "dim1", "dim_value1", fromTS, toTS),
		generateMetrics("test_namespace2", "test_name2", "test_region2", "dim2", "dim_value2", fromTS, toTS),
		generateMetrics("test_namespace", "test_name", "test_region", "dim3", "dim_value3", fromTS2, toTS2),
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
		want []Metric
	}{
		{
			name: "match1",
			from: fromTS,
			to:   toTS,
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "test_namespace"),
				labels.MustNewMatcher(labels.MatchEqual, "name", "test_name"),
				labels.MustNewMatcher(labels.MatchEqual, "region", "test_region"),
				labels.MustNewMatcher(labels.MatchEqual, "dim1", "dim_value1"),
			},
			want: []Metric{
				generateMetrics("test_namespace", "test_name", "test_region", "dim1", "dim_value1", fromTS, toTS),
			},
		},
		{
			name: "match2",
			from: fromTS,
			to:   toTS,
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "test_namespace2"),
				labels.MustNewMatcher(labels.MatchEqual, "name", "test_name2"),
				labels.MustNewMatcher(labels.MatchEqual, "region", "test_region2"),
				labels.MustNewMatcher(labels.MatchEqual, "dim2", "dim_value2"),
			},
			want: []Metric{
				generateMetrics("test_namespace2", "test_name2", "test_region2", "dim2", "dim_value2", fromTS, toTS),
			},
		},
		{
			name: "match3",
			from: fromTS2,
			to:   toTS2,
			lm: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "test_namespace"),
				labels.MustNewMatcher(labels.MatchEqual, "name", "test_name"),
				labels.MustNewMatcher(labels.MatchEqual, "region", "test_region"),
			},
			want: []Metric{
				generateMetrics("test_namespace", "test_name", "test_region", "dim3", "dim_value3", fromTS2, toTS2),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.QueryMetrics(ctx, tt.from, tt.to, tt.lm)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("unexpected length: got=%d, want=%d", len(got), len(tt.want))
			}
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
			toTS := fromTS.Add(time.Duration(rand.Intn(60*60)) * time.Second)
			err = db.RecordMetric(ctx, Metric{
				Namespace: "test_namespace",
				Name:      "test_name",
				Region:    "test_region",
				Dimensions: []Dimension{
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
