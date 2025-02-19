package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	_ "embed"

	_ "github.com/mattn/go-sqlite3"
)

const (
	DbPath = "labels.db"
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

func (ldb *LabelDB) Init(ctx context.Context) error {
	err := withTx(ctx, ldb.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, createTableStmt)
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

	err = withTx(ctx, ldb.db, func(tx *sql.Tx) error {
		row := ldb.db.QueryRowContext(ctx, `SELECT metric_id FROM metrics
		WHERE
			namespace = ? AND
			name = ? AND
			region = ? AND
			dimensions = ?
		`, metric.Namespace, metric.Name, metric.Region, d)

		var metricID int
		err := row.Scan(&metricID)
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
				metric.FromTS,
				metric.ToTS,
				time.Now().Unix(),
			)
			if err != nil {
				return err
			}

			metricID, err := res.LastInsertId()
			if err != nil {
				return err
			}

			_, err = tx.ExecContext(ctx, `INSERT INTO metrics_lifetime (
				metric_id,
				from_timestamp,
				to_timestamp
			) VALUES (?, ?, ?);`,
				metricID,
				metric.FromTS,
				metric.ToTS,
			)
			if err != nil {
				return err
			}
		} else if err == nil && metricID > 0 {
			_, err := tx.ExecContext(ctx, `UPDATE metrics SET
				to_timestamp = ?,
				updated_at = ?
			WHERE metric_id = ?;`,
				metric.ToTS,
				time.Now().Unix(),
				metricID,
			)
			if err != nil {
				return err
			}

			_, err = tx.ExecContext(ctx, `UPDATE metrics_lifetime SET
				to_timestamp = ?
			WHERE metric_id = ?;`,
				metric.ToTS,
				metricID,
			)
			if err != nil {
				return err
			}
		} else {
			return err
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
