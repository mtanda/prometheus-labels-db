package model

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"time"
)

type Metric struct {
	MetricID   int64
	Namespace  string
	MetricName string
	Region     string
	Dimensions Dimensions
	FromTS     time.Time
	ToTS       time.Time
	UpdatedAt  time.Time
}

type Dimensions []Dimension

type Dimension struct {
	Name  string
	Value string
}

func (ds Dimensions) MarshalJSON() ([]byte, error) {
	s := make([]string, 0, len(ds))
	sort.Slice(ds, func(i, j int) bool {
		return ds[i].Name < ds[j].Name
	})
	for _, d := range ds {
		// if __name__ is accidentally included, skip it
		if d.Name == "__name__" {
			continue
		}
		s = append(s, `"`+d.Name+`": "`+d.Value+`"`)
	}
	return []byte("{" + strings.Join(s, ", ") + "}"), nil
}

func (ds *Dimensions) UnmarshalJSON(b []byte) error {
	var data map[string]interface{}

	err := json.Unmarshal([]byte(b), &data)
	if err != nil {
		return err
	}

	for k, v := range data {
		// if __name__ is accidentally included, skip it
		if k == "__name__" {
			continue
		}
		*ds = append(*ds, Dimension{
			Name:  k,
			Value: v.(string),
		})
	}

	return nil
}

func (a Metric) Equal(b Metric) bool {
	if a.Namespace != b.Namespace ||
		a.MetricName != b.MetricName ||
		a.Region != b.Region ||
		len(a.Dimensions) != len(b.Dimensions) ||
		!a.FromTS.Equal(b.FromTS) ||
		!a.ToTS.Equal(b.ToTS) {
		return false
	}

	// sort before comparing
	sort.Slice(a.Dimensions, func(i, j int) bool {
		return a.Dimensions[i].Name < a.Dimensions[j].Name
	})
	sort.Slice(b.Dimensions, func(i, j int) bool {
		return b.Dimensions[i].Name < b.Dimensions[j].Name
	})

	for i := range a.Dimensions {
		if a.Dimensions[i].Name != b.Dimensions[i].Name ||
			a.Dimensions[i].Value != b.Dimensions[i].Value {
			return false
		}
	}

	return true
}

func (a Metric) UniqueKey() string {
	key := a.Namespace + a.MetricName + a.Region
	// should sort dimensions by name to ensure consistent key generation
	sort.Slice(a.Dimensions, func(i, j int) bool {
		return a.Dimensions[i].Name < a.Dimensions[j].Name
	})
	for _, d := range a.Dimensions {
		key += d.Name + d.Value
	}
	return key
}

func (a Metric) Labels() map[string]string {
	labels := map[string]string{
		"__name__":   safeMetricName(a.MetricName),
		"MetricName": a.MetricName, // store original metric name
		"Namespace":  a.Namespace,
		"Region":     a.Region,
	}
	for _, d := range a.Dimensions {
		labels[d.Name] = d.Value
	}
	return labels
}

var (
	// Prometheus metric names must match the regex [a-zA-Z_:][a-zA-Z0-9_:]*.
	invalidMetricNamePattern = regexp.MustCompile(`[^a-zA-Z0-9_:]`)
)

func safeMetricName(name string) string {
	if len(name) == 0 {
		return ""
	}
	name = invalidMetricNamePattern.ReplaceAllString(name, "_")
	if '0' <= name[0] && name[0] <= '9' {
		name = "_" + name[1:]
	}
	return name
}

type MetricLifetime struct {
	MetricID int64
	FromTS   time.Time
	ToTS     time.Time
}
