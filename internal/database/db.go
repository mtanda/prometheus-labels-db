package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"text/template"
	"time"

	_ "embed"

	_ "github.com/mattn/go-sqlite3"
)

const (
	DbPath            = "labels.db"
	PartitionInterval = 3 * 4 * 7 * 24 * time.Hour
)

type LabelDB struct {
	db *sql.DB
}

//go:embed sql/table.sql
var createTableStmt string

func Open(dir string) (*LabelDB, error) {
	db, err := sql.Open("sqlite3", dir+"/"+DbPath)
	if err != nil {
		return nil, err
	}
	return &LabelDB{
		db: db,
	}, nil
}

func (ldb *LabelDB) Close() error {
	return ldb.db.Close()
}

func (ldb *LabelDB) Init(ctx context.Context, t time.Time) error {
	data := struct {
		MetricsLifetimePreSuffix string
		MetricsLifetimeCurSuffix string
	}{
		MetricsLifetimePreSuffix: getPartitionSuffix(t.Add(-1 * PartitionInterval)),
		MetricsLifetimeCurSuffix: getPartitionSuffix(t),
	}
	tmpl, err := template.New("").Parse(createTableStmt)
	if err != nil {
		return err
	}
	var sb strings.Builder
	if err = tmpl.Execute(&sb, data); err != nil {
		return err
	}

	err = withTx(ctx, ldb.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, sb.String())
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func (ldb *LabelDB) RecordMetric(ctx context.Context, metric Metric) error {
	d, err := json.Marshal(metric.Dimensions)
	if err != nil {
		return err
	}
	if metric.ToTS.Before(metric.FromTS) {
		return errors.New("from timestamp is greater than to timestamp")
	}

	err = withTx(ctx, ldb.db, func(tx *sql.Tx) error {
		row := ldb.db.QueryRowContext(ctx, `SELECT metric_id, from_timestamp, to_timestamp FROM metrics
		WHERE
			namespace = ? AND
			name = ? AND
			region = ? AND
			dimensions = ?
		`, metric.Namespace, metric.Name, metric.Region, d)

		var metricID int64
		var fromTS int64
		var toTS int64
		err := row.Scan(&metricID, &fromTS, &toTS)
		if errors.Is(err, sql.ErrNoRows) {
			res, err := tx.ExecContext(ctx, `INSERT INTO metrics (
				namespace,
				name,
				region,
				dimensions,
				from_timestamp,
				to_timestamp,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?);`,
				metric.Namespace,
				metric.Name,
				metric.Region,
				d,
				metric.FromTS.Unix(),
				metric.ToTS.Unix(),
				time.Now().UTC().Unix(),
			)
			if err != nil {
				return err
			}

			metricID, err = res.LastInsertId()
			if err != nil {
				return err
			}
			fromTS = metric.FromTS.Unix()
			toTS = metric.ToTS.Unix()
		} else if err == nil && metricID > 0 {
			toTS = max(toTS, metric.ToTS.Unix())
			_, err := tx.ExecContext(ctx, `UPDATE metrics SET
				to_timestamp = ?,
				updated_at = ?
			WHERE metric_id = ?;`,
				toTS,
				time.Now().UTC().Unix(),
				metricID,
			)
			if err != nil {
				return err
			}
		} else {
			return err
		}

		trs := getLifetimeRanges(time.Unix(fromTS, 0).UTC(), time.Unix(toTS, 0).UTC())
		for _, tr := range trs {
			s := getPartitionSuffix(tr.From)
			res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO metrics_lifetime`+s+`(
				metric_id,
				from_timestamp,
				to_timestamp
			) VALUES (?, ?, ?);`,
				metricID,
				tr.From.Unix(),
				tr.To.Unix(),
			)
			if err != nil {
				return err
			}
			rowsAffected, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if rowsAffected == 0 {
				_, err = tx.ExecContext(ctx, `UPDATE metrics_lifetime`+s+` SET
					to_timestamp = ?
				WHERE metric_id = ?;`,
					tr.To.Unix(),
					metricID,
				)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})

	return err
}

func withTx(ctx context.Context, db *sql.DB, f func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	err = f(tx)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

type timeRange struct {
	From time.Time
	To   time.Time
}

func getPartition(t time.Time) timeRange {
	from := t.Truncate(PartitionInterval)
	to := from.Add(PartitionInterval).Add(-1 * time.Second)
	return timeRange{
		From: from,
		To:   to,
	}
}

func getPartitionSuffix(t time.Time) string {
	p := getPartition(t)
	return "_" + p.From.Format("20060102") + "_" + p.To.Format("20060102")
}

func getLifetimeRanges(from time.Time, to time.Time) []timeRange {
	var partitions []timeRange
	for t := from; t.Before(to); t = t.Add(PartitionInterval) {
		partitions = append(partitions, getPartition(t))
	}
	partitions[0].From = from
	partitions[len(partitions)-1].To = to
	return partitions
}
