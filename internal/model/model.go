package model

import (
	"encoding/json"
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
	for _, d := range ds {
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
	for _, d := range a.Dimensions {
		key += d.Name + d.Value
	}
	return key
}

type MetricLifetime struct {
	MetricID int64
	FromTS   time.Time
	ToTS     time.Time
}
