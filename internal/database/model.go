package database

import (
	"strings"
	"time"
)

type Metric struct {
	MetricID   int64
	Namespace  string
	Name       string
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

type MetricLifetime struct {
	MetricID int64
	FromTS   time.Time
	ToTS     time.Time
}
