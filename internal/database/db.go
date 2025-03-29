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

	_ "github.com/mtanda/prometheus-labels-db/internal/database/regexp"
	"github.com/mtanda/prometheus-labels-db/internal/model"
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
	db, err := sql.Open("sqlite3", dir+"/"+DbPath+"?cache=shared&_journal_mode=WAL&_sync=FULL&_busy_timeout=10000")
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
	suffix := getTableSuffix(t)
	lsuffix := getLifetimeTableSuffix(t, namespace)
	_, found := ldb.cache.Get(lsuffix)
	if found {
		return nil
	}

	data := struct {
		MetricsPreSuffix         string
		MetricsCurSuffix         string
		MetricsLifetimePreSuffix string
		MetricsLifetimeCurSuffix string
	}{
		MetricsPreSuffix:         getTableSuffix(t.Add(-1 * PartitionInterval)),
		MetricsCurSuffix:         suffix,
		MetricsLifetimePreSuffix: getLifetimeTableSuffix(t.Add(-1*PartitionInterval), namespace),
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

	_, err = ldb.db.ExecContext(ctx, sb.String())
	if err != nil {
		return err
	}

	ldb.cache.Add(lsuffix, struct{}{})

	return nil
}

func (ldb *LabelDB) RecordMetric(ctx context.Context, metric model.Metric) error {
	if metric.ToTS.Before(metric.FromTS) {
		return errors.New("from timestamp is greater than to timestamp")
	}

	err := withTx(ctx, ldb.db, func(tx *sql.Tx) error {
		trs := getLifetimeRanges(metric.FromTS, metric.ToTS)
		for _, tr := range trs {
			err := ldb.init(ctx, tr.From, metric.Namespace)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	err = withTx(ctx, ldb.db, func(tx *sql.Tx) error {
		trs := getLifetimeRanges(metric.FromTS, metric.ToTS)
		for _, tr := range trs {
			err = ldb.recordMetricToPartition(ctx, tx, metric, tr)
			if err != nil {
				return err
			}
		}

		return nil
	})

	return err
}

func (ldb *LabelDB) recordMetricToPartition(ctx context.Context, tx *sql.Tx, metric model.Metric, tr timeRange) error {
	d, err := json.Marshal(metric.Dimensions)
	if err != nil {
		return err
	}

	// metrics
	s := getTableSuffix(tr.From)
	row := ldb.db.QueryRowContext(ctx, `
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

func (ldb *LabelDB) QueryMetrics(ctx context.Context, from, to time.Time, lm []*labels.Matcher) ([]model.Metric, error) {
	ms := []model.Metric{}

	// convert prometheus label matchers to sql where clause
	labelCondition, labelArgs, namespace, err := buildLabelConditions(lm)
	if err != nil {
		return ms, err
	}

	mm := make(map[string]*model.Metric)
	trs := getLifetimeRanges(from, to)
	for _, tr := range trs {
		timeCondition, timeArgs := buildTimeConditions(tr)

		s := getTableSuffix(tr.From)
		ls := getLifetimeTableSuffix(tr.From, namespace)
		q := `SELECT m.*
FROM metrics_lifetime` + ls + ` ml
JOIN metrics` + s + ` m ON ml.metric_id = m.metric_id
WHERE ` + strings.Join(append(timeCondition, labelCondition...), " AND ")
		rows, err := ldb.db.QueryContext(ctx, q, append(timeArgs, labelArgs...)...)
		if err != nil {
			return ms, err
		}
		defer rows.Close()

		for rows.Next() {
			var m model.Metric
			var dim []byte
			var fromTS int64
			var toTS int64
			var updatedAt int64
			rows.Scan(&m.MetricID, &m.Namespace, &m.MetricName, &m.Region, &dim, &fromTS, &toTS, &updatedAt)
			err = json.Unmarshal(dim, &m.Dimensions)
			if err != nil {
				return ms, err
			}
			m.FromTS = time.Unix(fromTS, 0).UTC()
			m.ToTS = time.Unix(toTS, 0).UTC()
			m.UpdatedAt = time.Unix(updatedAt, 0).UTC()
			k := m.UniqueKey()
			if _, ok := mm[k]; ok {
				mm[k].FromTS = time.Unix(min(m.FromTS.Unix(), mm[k].FromTS.Unix()), 0).UTC()
				mm[k].ToTS = time.Unix(max(m.ToTS.Unix(), mm[k].ToTS.Unix()), 0).UTC()
			} else {
				mm[k] = &m
			}
		}
	}
	for _, m := range mm {
		ms = append(ms, *m)
	}

	return ms, nil
}

func buildLabelConditions(lm []*labels.Matcher) ([]string, []interface{}, string, error) {
	var labelCondition []string
	var labelArgs []interface{}
	var namespace string
	for _, m := range lm {
		ln := m.Name
		lv := m.Value
		if ln == "namespace" {
			namespace = lv
		}
		if ln == "namespace" || ln == "metric_name" || ln == "region" {
			ln = `m.` + ln
		} else {
			ln = `IFNULL(m.dimensions->>'$.` + ln + `', "")`
		}
		switch m.Type {
		case labels.MatchEqual:
			labelCondition = append(labelCondition, ln+" = ?")
			labelArgs = append(labelArgs, lv)
		case labels.MatchNotEqual:
			labelCondition = append(labelCondition, ln+" != ?")
			labelArgs = append(labelArgs, lv)
		case labels.MatchRegexp:
			labelCondition = append(labelCondition, ln+" REGEXP ?")
			labelArgs = append(labelArgs, lv)
		case labels.MatchNotRegexp:
			labelCondition = append(labelCondition, ln+" NOT REGEXP ?")
			labelArgs = append(labelArgs, lv)
		}
	}
	if namespace == "" {
		return nil, nil, "", errors.New("namespace label matcher is required")
	}
	return labelCondition, labelArgs, namespace, nil
}

func buildTimeConditions(tr timeRange) ([]string, []interface{}) {
	var timeCondition []string
	var timeArgs []interface{}
	timeCondition = append(timeCondition, "ml.from_timestamp <= ?")
	timeArgs = append(timeArgs, tr.To.Unix())
	timeCondition = append(timeCondition, "ml.to_timestamp >= ?")
	timeArgs = append(timeArgs, tr.From.Unix())
	return timeCondition, timeArgs
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

func getTableSuffix(t time.Time) string {
	p := getPartition(t)
	return "_" + p.From.Format("20060102") + "_" + p.To.Format("20060102")
}

func getLifetimeTableSuffix(t time.Time, namespace string) string {
	namespace = strings.ReplaceAll(namespace, "/", "_")
	return getTableSuffix(t) + "_" + namespace
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
