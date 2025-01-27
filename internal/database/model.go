package database

import (
	"strings"
)

type Metric struct {
	MetricID   int32
	Namespace  string
	Name       string
	Region     string
	Dimensions Dimensions
	FromTS     int64
	ToTS       int64
	UpdatedAt  int64
}

type Dimensions []Dimension

type Dimension struct {
	Name  string
	Value string
}

func (ds Dimensions) marsharlJSON() ([]byte, error) {
	s := make([]string, 0, len(ds))
	for _, d := range ds {
		s = append(s, `"`+d.Name+`": "`+d.Value+`"`)
	}
	return []byte("{" + strings.Join(s, ", ") + "}"), nil
}

type MetricLifetime struct {
	MetricID int64
	FromTS   int32
	ToTS     int32
}
