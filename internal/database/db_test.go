package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"math/rand"

	_ "github.com/mattn/go-sqlite3"
)

func TestInsertMetric(t *testing.T) {
	ctx := context.Background()
	dbDir := t.TempDir()
	db, err := Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Init(ctx)
	if err != nil {
		t.Fatal(err)
	}

	fromTS := int64(5)
	toTS := int64(10)
	err = db.RecordMetric(ctx, Metric{
		Namespace: "test_namespace",
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
	err = rows.Scan(&metric.MetricID, &metric.Namespace, &metric.Name, &metric.Region, &dim, &metric.FromTS, &metric.ToTS, &metric.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}

	err = json.Unmarshal(dim, &metric.Dimensions)
	if err != nil {
		t.Fatal(err)
	}
	if metric.MetricID != 1 ||
		metric.Namespace != "test_namespace" ||
		metric.Name != "test_name" ||
		metric.Region != "test_region" ||
		len(metric.Dimensions) != 1 ||
		metric.Dimensions[0].Name != "dim1" ||
		metric.Dimensions[0].Value != "dim_value1" ||
		metric.FromTS != fromTS ||
		metric.ToTS != toTS {
		t.Fatalf("unexpected row: %+v", metric)
	}

	hasNext := rows.Next()
	if hasNext != false {
		t.Fatal("expected no more rows")
	}

	// check metrics_lifetime table
	rows, err = db.db.QueryContext(ctx, "SELECT * FROM metrics_lifetime")
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()

	var lifetime MetricLifetime
	err = rows.Scan(&lifetime.MetricID, &lifetime.FromTS, &lifetime.ToTS)
	if err != nil {
		t.Fatal(err)
	}

	if lifetime.MetricID != 1 || lifetime.FromTS != int32(fromTS) || lifetime.ToTS != int32(toTS) {
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

	err = db.Init(ctx)
	if err != nil {
		t.Fatal(err)
	}

	fromTS := int64(5)
	toTS := int64(10)
	err = db.RecordMetric(ctx, Metric{
		Namespace: "test_namespace",
		Name:      "test_name",
		Region:    "test_region",
		Dimensions: []Dimension{
			{
				Name:  "dim1",
				Value: "dim_value1",
			},
		},
		FromTS: fromTS,
		ToTS:   toTS - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.RecordMetric(ctx, Metric{
		Namespace: "test_namespace",
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
	err = rows.Scan(&metric.MetricID, &metric.Namespace, &metric.Name, &metric.Region, &dim, &metric.FromTS, &metric.ToTS, &metric.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}

	err = json.Unmarshal(dim, &metric.Dimensions)
	if err != nil {
		t.Fatal(err)
	}
	if metric.MetricID != 1 ||
		metric.Namespace != "test_namespace" ||
		metric.Name != "test_name" ||
		metric.Region != "test_region" ||
		len(metric.Dimensions) != 1 ||
		metric.Dimensions[0].Name != "dim1" ||
		metric.Dimensions[0].Value != "dim_value1" ||
		metric.FromTS != fromTS ||
		metric.ToTS != toTS {
		t.Fatalf("unexpected row: %+v", metric)
	}

	hasNext := rows.Next()
	if hasNext != false {
		t.Fatal("expected no more rows")
	}

	// check metrics_lifetime table
	rows, err = db.db.QueryContext(ctx, "SELECT * FROM metrics_lifetime")
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()

	var lifetime MetricLifetime
	err = rows.Scan(&lifetime.MetricID, &lifetime.FromTS, &lifetime.ToTS)
	if err != nil {
		t.Fatal(err)
	}

	if lifetime.MetricID != 1 || lifetime.FromTS != int32(fromTS) || lifetime.ToTS != int32(toTS) {
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

	err = db.Init(ctx)
	if err != nil {
		t.Fatal(err)
	}

	fromTS := int64(10)
	toTS := int64(5)
	err = db.RecordMetric(ctx, Metric{
		Namespace: "test_namespace",
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
	if err == nil {
		t.Fatal("expected rtree constraint failed")
	}

	row := db.db.QueryRowContext(ctx, "SELECT * FROM metrics")
	if !errors.Is(row.Scan(), sql.ErrNoRows) {
		t.Fatal("expected no rows")
	}
	row = db.db.QueryRowContext(ctx, "SELECT * FROM metrics_lifetime")
	if !errors.Is(row.Scan(), sql.ErrNoRows) {
		t.Fatal("expected no rows")
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

	err = db.Init(ctx)
	if err != nil {
		b.Fatal(err)
	}

	now := time.Now().Unix()
	for i := 0; i < 2; i++ {
		for j := 0; j < 10000; j++ {
			fromTS := now - int64(rand.Intn(365*24*60*60))
			if i == 0 {
				fromTS -= 365 * 24 * 60 * 60
			}
			toTS := fromTS + int64(rand.Intn(60*60))
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
