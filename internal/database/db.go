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

	lru "github.com/hashicorp/golang-lru/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	DbPath            = "labels.db"
	PartitionInterval = 3 * 4 * 7 * 24 * time.Hour
	InitCacheSize     = 1000
)

type LabelDB struct {
	db    *sql.DB
	cache *lru.Cache[string, struct{}]
}

//go:embed sql/table.sql
var createTableStmt string

func Open(dir string) (*LabelDB, error) {
	db, err := sql.Open("sqlite3", dir+"/"+DbPath)
	if err != nil {
		return nil, err
	}
	cache, err := lru.New[string, struct{}](InitCacheSize)
	if err != nil {
		return nil, err
	}
	return &LabelDB{
		db:    db,
		cache: cache,
	}, nil
}

func (ldb *LabelDB) Close() error {
	return ldb.db.Close()
}

func (ldb *LabelDB) init(ctx context.Context, t time.Time, namespace string) error {
	suffix := getLifetimeTableSuffix(t, namespace)
	_, found := ldb.cache.Get(suffix)
	if found {
		return nil
	}

	data := struct {
		MetricsLifetimePreSuffix string
		MetricsLifetimeCurSuffix string
	}{
		MetricsLifetimePreSuffix: getLifetimeTableSuffix(t.Add(-1*PartitionInterval), namespace),
		MetricsLifetimeCurSuffix: suffix,
	}
	tmpl, err := template.New("").Parse(createTableStmt)
	if err != nil {
		return err
	}
	var sb strings.Builder
	if err = tmpl.Execute(&sb, data); err != nil {
		return err
	}

	_, err = ldb.db.ExecContext(ctx, sb.String())
	if err != nil {
		return err
	}

	ldb.cache.Add(suffix, struct{}{})

	return nil
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
		return ldb.init(ctx, metric.FromTS, metric.Namespace)
	})
	if err != nil {
		return err
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
			s := getLifetimeTableSuffix(tr.From, metric.Namespace)
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

func (ldb *LabelDB) QueryMetrics(ctx context.Context, from, to time.Time, lm []*labels.Matcher) ([]Metric, error) {
	ms := []Metric{}
	var whereClause []string
	var args []interface{}

	// set time range
	whereClause = append(whereClause, "ml.from_timestamp <= ?")
	args = append(args, from.Unix())
	whereClause = append(whereClause, "ml.to_timestamp >= ?")
	args = append(args, to.Unix())

	// convert prometheus label matchers to sql where clause
	var namespace string
	for _, m := range lm {
		ln := m.Name
		lv := m.Value
		if ln == "namespace" {
			namespace = lv
		}
		if ln == "namespace" || ln == "name" || ln == "region" {
			ln = `m.` + ln
		} else {
			ln = `m.dimensions->>'$.` + ln + `'`
		}
		switch m.Type {
		case labels.MatchEqual:
			whereClause = append(whereClause, ln+" = ?")
			args = append(args, lv)
		case labels.MatchNotEqual:
			whereClause = append(whereClause, ln+" != ?")
			args = append(args, lv)
		}
	}
	if namespace == "" {
		return ms, errors.New("namespace label matcher is required")
	}

	s := getLifetimeTableSuffix(from, namespace)
	q := `SELECT m.*
FROM metrics_lifetime` + s + ` ml
JOIN metrics m ON ml.metric_id = m.metric_id
WHERE ` + strings.Join(whereClause, " AND ")
	rows, err := ldb.db.QueryContext(ctx, q, args...)
	if err != nil {
		return ms, err
	}
	defer rows.Close()

	for rows.Next() {
		var m Metric
		var dim []byte
		var fromTS int64
		var toTS int64
		var updatedAt int64
		rows.Scan(&m.MetricID, &m.Namespace, &m.Name, &m.Region, &dim, &fromTS, &toTS, &updatedAt)
		err = json.Unmarshal(dim, &m.Dimensions)
		if err != nil {
			return ms, err
		}
		m.FromTS = time.Unix(fromTS, 0).UTC()
		m.ToTS = time.Unix(toTS, 0).UTC()
		m.UpdatedAt = time.Unix(updatedAt, 0).UTC()
		ms = append(ms, m)
	}

	return ms, nil
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

func getLifetimeTableSuffix(t time.Time, namespace string) string {
	p := getPartition(t)
	return "_" + p.From.Format("20060102") + "_" + p.To.Format("20060102") + "_" + namespace
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
