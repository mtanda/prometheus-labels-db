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

	_ "embed"

	lru "github.com/hashicorp/golang-lru/v2"
	_ "github.com/mattn/go-sqlite3"

	_ "github.com/mtanda/prometheus-labels-db/internal/database/regexp"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	DbPathPattern     = "labels%s.db"
	PartitionInterval = 3 * 4 * 7 * 24 * time.Hour
	InitCacheSize     = 1000
	WalAutoCheckpoint = 100
	IdleTimeout       = 1 * time.Hour
)

type LabelDB struct {
	dir         string
	db          map[string]*sql.DB
	lastUsed    map[string]time.Time
	initialized *lru.Cache[string, struct{}]
}

//go:embed sql/table.sql
var createTableStmt string

func Open(dir string) (*LabelDB, error) {
	cache, err := lru.New[string, struct{}](InitCacheSize)
	if err != nil {
		return nil, err
	}
	return &LabelDB{
		dir:         dir,
		db:          make(map[string]*sql.DB),
		lastUsed:    make(map[string]time.Time),
		initialized: cache,
	}, nil
}

func (ldb *LabelDB) getDB(t time.Time) (*sql.DB, error) {
	suffix := getTableSuffix(t)
	ldb.lastUsed[suffix] = time.Now()

	dbPath := fmt.Sprintf(DbPathPattern, suffix)
	if db, ok := ldb.db[dbPath]; ok {
		return db, nil
	}

	// TODO: support mode=ro for query command
	db, err := sql.Open("sqlite3", "file:"+ldb.dir+"/"+dbPath+"?_journal_mode=WAL&_sync=NORMAL&_busy_timeout=10000")
	if err != nil {
		return nil, err
	}
	setAutoCheckpoint(db, WalAutoCheckpoint)
	ldb.db[dbPath] = db

	return db, nil
}

func setAutoCheckpoint(db *sql.DB, n int) error {
	_, err := db.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", n))
	if err != nil {
		return err
	}
	return nil
}

func (ldb *LabelDB) Close() error {
	var allErr error
	for _, db := range ldb.db {
		if err := db.Close(); err != nil {
			// ignore error
			slog.Error("failed to close db", "err", err)
			allErr = errors.Join(allErr, err)
		}
	}
	return allErr
}

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

func (ldb *LabelDB) QueryMetrics(ctx context.Context, from, to time.Time, lm []*labels.Matcher, limit int) ([]model.Metric, error) {
	ms := []model.Metric{}

	// convert prometheus label matchers to sql where clause
	labelCondition, labelArgs, namespace, err := buildLabelConditions(lm)
	if err != nil {
		return ms, err
	}

	// TODO: support multiple namespaces
	mm := make(map[string]*model.Metric)
	trs := getLifetimeRanges(from, to)
	for _, tr := range trs {
		err = func() error {
			db, err := ldb.getDB(tr.From)
			if err != nil {
				return err
			}
			timeCondition, timeArgs := buildTimeConditions(tr)

			s := getTableSuffix(tr.From)
			ls := getLifetimeTableSuffix(tr.From, namespace)
			q := `SELECT m.*
FROM metrics_lifetime` + ls + ` ml
JOIN metrics` + s + ` m ON ml.metric_id = m.metric_id
WHERE ` + strings.Join(append(timeCondition, labelCondition...), " AND ")
			var limitArgs []interface{}
			if limit > 0 {
				q += ` LIMIT ?`
				limitArgs = append(limitArgs, limit)
			}
			rows, err := db.QueryContext(ctx, q, append(append(timeArgs, labelArgs...), limitArgs...)...)
			if err != nil {
				return err
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
					return err
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
			return nil
		}()
		if err != nil {
			if strings.Contains(err.Error(), "no such table: ") {
				continue
			}
			return ms, err
		}
	}
	for _, m := range mm {
		ms = append(ms, *m)
	}
	if limit > 0 && len(ms) > limit {
		ms = ms[:limit]
	}

	return ms, nil
}

func (ldb *LabelDB) WalCheckpoint(ctx context.Context) error {
	checkpointPRAGMA := `PRAGMA wal_checkpoint(TRUNCATE)`
	var ok, pages, moved int
	for _, db := range ldb.db {
		if err := db.QueryRow(checkpointPRAGMA).Scan(&ok, &pages, &moved); err != nil {
			return err
		}
	}
	slog.Debug("WAL checkpoint", "ok", ok, "pages", pages, "moved", moved)
	return nil
}

func (ldb *LabelDB) CleanupUnusedDB(ctx context.Context) error {
	for suffix, db := range ldb.db {
		if ldb.lastUsed[suffix].Add(IdleTimeout).After(time.Now()) {
			// still used
			continue
		}

		if err := db.Close(); err != nil {
			// ignore error
			slog.Error("failed to close db", "err", err)
			continue
		}
		delete(ldb.db, suffix)
		delete(ldb.lastUsed, suffix)
		slog.Info("close unused db", "suffix", suffix)
	}
	return nil
}

func buildLabelConditions(lm []*labels.Matcher) ([]string, []interface{}, string, error) {
	var labelCondition []string
	var labelArgs []interface{}
	var namespace string
	for _, m := range lm {
		ln := m.Name
		lv := m.Value
		if ln == "Namespace" {
			namespace = lv
		}
		switch ln {
		case "Namespace":
			ln = `m.namespace`
		case "__name__":
			ln = `m.metric_name`
		case "MetricName":
			ln = `m.metric_name`
		case "Region":
			ln = `m.region`
		default:
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
