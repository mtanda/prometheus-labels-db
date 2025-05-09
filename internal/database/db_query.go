package database

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/prometheus/prometheus/model/labels"
)

func (ldb *LabelDB) QueryMetrics(ctx context.Context, from, to time.Time, lm []*labels.Matcher, limit int, result map[string]*model.Metric) (map[string]*model.Metric, error) {
	// convert prometheus label matchers to sql where clause
	labelCondition, labelArgs, namespace, err := buildLabelConditions(lm)
	if err != nil {
		return result, err
	}

	// TODO: support multiple namespaces
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
				if _, ok := result[k]; ok {
					result[k].FromTS = time.Unix(min(m.FromTS.Unix(), result[k].FromTS.Unix()), 0).UTC()
					result[k].ToTS = time.Unix(max(m.ToTS.Unix(), result[k].ToTS.Unix()), 0).UTC()
				} else {
					result[k] = &m
				}
			}
			return nil
		}()
		if err != nil {
			if strings.Contains(err.Error(), "no such table: ") {
				continue
			}
			return result, err
		}

		// check if we have enough results
		if limit != 0 && len(result) >= limit {
			break
		}
	}

	// trim result to limit at the caller side
	return result, nil
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
