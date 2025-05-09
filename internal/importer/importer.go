package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/mtanda/prometheus-labels-db/internal/database"
	"github.com/mtanda/prometheus-labels-db/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
)

const (
	MaxRetry          = 3
	reportInterval    = 1000
	importerStatePath = "importer_state.json"
	// https://aws.amazon.com/about-aws/whats-new/2016/11/cloudwatch-extends-metrics-retention-and-new-user-interface/
	cloudwatchExpireDays = 455
)

type importerState struct {
	Day string `json:"day"`
}

type Importer struct {
	ldb         *database.LabelDB
	db          *tsdb.DBReadOnly
	statePath   string
	state       importerState
	importTotal *prometheus.CounterVec
}

func New(baseDir string, ldb *database.LabelDB, db *tsdb.DBReadOnly, registry *prometheus.Registry) *Importer {
	importTotal := promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
		Name: "importer_import_total",
		Help: "Total number of importing metrics operations",
	}, []string{"status"})

	statePath := fmt.Sprintf("%s/%s", baseDir, importerStatePath)
	state, err := loadState(statePath)
	if err != nil {
		panic(err)
	}

	return &Importer{
		ldb:         ldb,
		db:          db,
		statePath:   statePath,
		state:       state,
		importTotal: importTotal,
	}
}

func (im *Importer) Import(ctx context.Context) error {
	// set initial counter value
	im.importTotal.WithLabelValues("success")
	im.importTotal.WithLabelValues("error")

	stateDay, err := time.ParseInLocation(time.RFC3339, im.state.Day, time.UTC)
	if err != nil {
		return err
	}
	stateDay = stateDay.Truncate(time.Hour * 24) // ensure it's at the start of the day
	now := time.Now().UTC()
	start := stateDay.Add(-time.Hour * 24)
	end := start.Add(time.Hour * 24)
	if end.After(now) {
		end = now
	}
	if now.Add(-time.Hour * 24 * cloudwatchExpireDays).After(end) {
		slog.Info("all imports are completed")
		return nil
	}

	slog.Info("import start", "day", start)
	querier, err := im.db.Querier(start.UnixMilli(), end.UnixMilli())
	if err != nil {
		return err
	}
	defer querier.Close()

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchNotEqual, "Namespace", ""),
	}
	ss := querier.Select(ctx, false, nil, matchers...)

	c := 0
	importStartTime := time.Now().UTC()
	lastReportTime := time.Now().UTC()
	for ss.Next() {
		series := ss.At()
		ls := series.Labels()
		if len(ls) == 0 {
			continue
		}

		namespace := ""
		metricName := ""
		region := ""
		dimensions := make([]model.Dimension, 0)
		for _, l := range ls {
			switch l.Name {
			case "Namespace":
				namespace = l.Value
			case "MetricName":
				metricName = l.Value
			case "Region":
				region = l.Value
			default:
				dimensions = append(dimensions, model.Dimension{
					Name:  l.Name,
					Value: l.Value,
				})
			}
		}

		metric := model.Metric{
			Namespace:  namespace,
			MetricName: metricName,
			Region:     region,
			Dimensions: dimensions,
			FromTS:     start,
			ToTS:       end,
			UpdatedAt:  end,
		}

		i := 0
		for ; i < MaxRetry; i++ {
			err := im.ldb.RecordMetric(ctx, metric)
			if err != nil {
				im.importTotal.WithLabelValues("error").Inc()
				sleepDuration := time.Duration(100*(1<<i)) * time.Millisecond // 0.1s, 0.2s, 0.4s, etc.
				time.Sleep(sleepDuration)
			} else {
				im.importTotal.WithLabelValues("success").Inc()
				break
			}
		}
		if i == MaxRetry {
			slog.Error("import failed", "day", start, "metric", metric)
			return fmt.Errorf("import failed")
		}

		c++
		if c%reportInterval == 0 {
			slog.Info(fmt.Sprintf("import %d records", reportInterval), "day", start, "durationSec", time.Since(lastReportTime).Seconds(), "count", c)
			lastReportTime = time.Now().UTC()
		}
	}
	if ss.Err() != nil {
		return err
	}

	slog.Info("import 1 day records", "day", start, "durationSec", time.Since(importStartTime).Seconds(), "count", c)

	// move to next day
	im.state.Day = start.Format(time.RFC3339)
	err = saveState(im.statePath, im.state)
	if err != nil {
		// ignore error
		slog.Error("failed to save import state", "error", err, "day", start)
	}

	return nil
}

func loadState(statePath string) (importerState, error) {
	now := time.Now().UTC().Truncate(time.Hour * 24).Add(+time.Hour * 24)
	state := importerState{
		Day: now.Format(time.RFC3339),
	}
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		f, err := os.Create(statePath)
		if err != nil {
			return state, err
		}
		defer f.Close()

		jsonData, err := json.Marshal(state)
		if err != nil {
			return state, err
		}
		_, err = f.Write(jsonData)
		if err != nil {
			return state, err
		}
	} else {
		f, err := os.Open(statePath)
		if err != nil {
			return state, err
		}
		defer f.Close()

		err = json.NewDecoder(f).Decode(&state)
		if err != nil {
			return state, err
		}
	}

	return state, nil
}

func saveState(statePath string, state importerState) error {
	f, err := os.OpenFile(statePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	jsonData, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = f.Write(jsonData)
	if err != nil {
		return err
	}

	return nil
}
