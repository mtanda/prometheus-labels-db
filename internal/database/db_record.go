package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"text/template"
	"time"

	"github.com/mtanda/prometheus-labels-db/internal/model"
)

func (ldb *LabelDB) init(ctx context.Context, tx *sql.Tx, t time.Time, namespace string) error {
	suffix := getTableSuffix(t)
	lsuffix := getLifetimeTableSuffix(t, namespace)
	_, found := ldb.initialized.Get(lsuffix)
	if found {
		return nil
	}

	data := struct {
		MetricsCurSuffix         string
		MetricsLifetimeCurSuffix string
	}{
		MetricsCurSuffix:         suffix,
		MetricsLifetimeCurSuffix: lsuffix,
	}
	tmpl, err := template.New("").Parse(createTableStmt)
	if err != nil {
		return err
	}
	var sb strings.Builder
	if err = tmpl.Execute(&sb, data); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, sb.String())
	if err != nil {
		return err
	}

	ldb.initialized.Add(lsuffix, struct{}{})

	return nil
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

func (ldb *LabelDB) RecordMetric(ctx context.Context, metric model.Metric) error {
	if metric.ToTS.Before(metric.FromTS) {
		return errors.New("from timestamp is greater than to timestamp")
	}

	trs := getLifetimeRanges(metric.FromTS, metric.ToTS)
	for _, tr := range trs {
		db, err := ldb.getDB(tr.From)
		if err != nil {
			return err
		}
		err = withTx(ctx, db, func(tx *sql.Tx) error {
			err := ldb.init(ctx, tx, tr.From, metric.Namespace)
			if err != nil {
				return err
			}
			return ldb.recordMetricToPartition(ctx, tx, metric, tr)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (ldb *LabelDB) recordMetricToPartition(ctx context.Context, tx *sql.Tx, metric model.Metric, tr timeRange) error {
	d, err := json.Marshal(metric.Dimensions)
	if err != nil {
		return err
	}

	// metrics
	s := getTableSuffix(tr.From)
	row := tx.QueryRowContext(ctx, `
		SELECT metric_id, from_timestamp, to_timestamp FROM metrics`+s+`
		WHERE
			namespace = ? AND
			metric_name = ? AND
			region = ? AND
			dimensions = ?
	`, metric.Namespace, metric.MetricName, metric.Region, d)

	var metricID int64
	var fromTS int64
	var toTS int64
	err = row.Scan(&metricID, &fromTS, &toTS)
	if errors.Is(err, sql.ErrNoRows) {
		res, err := tx.ExecContext(ctx, `
			INSERT INTO metrics`+s+` (
				namespace,
				metric_name,
				region,
				dimensions,
				from_timestamp,
				to_timestamp,
				updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?);
			`,
			metric.Namespace,
			metric.MetricName,
			metric.Region,
			d,
			tr.From.Unix(),
			tr.To.Unix(),
			time.Now().UTC().Unix(),
		)
		if err != nil {
			return err
		}

		metricID, err = res.LastInsertId()
		if err != nil {
			return err
		}
	} else if err == nil && metricID > 0 {
		_, err := tx.ExecContext(ctx, `
			UPDATE metrics`+s+` SET
				from_timestamp = ?,
				to_timestamp = ?,
				updated_at = ?
			WHERE metric_id = ?;
			`,
			min(tr.From.Unix(), fromTS),
			max(tr.To.Unix(), toTS),
			time.Now().UTC().Unix(),
			metricID,
		)
		if err != nil {
			return err
		}
	} else {
		return err
	}

	// metrics_lifetime
	ls := getLifetimeTableSuffix(tr.From, metric.Namespace)
	res, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO metrics_lifetime`+ls+`(
			metric_id,
			from_timestamp,
			to_timestamp
		) VALUES (?, ?, ?);
		`,
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
		_, err = tx.ExecContext(ctx, `
			UPDATE metrics_lifetime`+ls+` SET
				from_timestamp = ?,
				to_timestamp = ?
			WHERE metric_id = ?;
			`,
			min(tr.From.Unix(), fromTS),
			max(tr.To.Unix(), toTS),
			metricID,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ldb *LabelDB) WalCheckpoint(ctx context.Context) error {
	checkpointPRAGMA := `PRAGMA wal_checkpoint(TRUNCATE)`
	var ok, pages, moved int
	for _, dbCache := range ldb.dbCache {
		if err := dbCache.db.QueryRow(checkpointPRAGMA).Scan(&ok, &pages, &moved); err != nil {
			return err
		}
	}
	slog.Debug("WAL checkpoint", "ok", ok, "pages", pages, "moved", moved)
	return nil
}

func setAutoCheckpoint(db *sql.DB, n int) error {
	_, err := db.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", n))
	if err != nil {
		return err
	}
	return nil
}
