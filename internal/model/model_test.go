package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLabels_WithSafeMetricName(t *testing.T) {
	metric := Metric{
		MetricName: "0test-name",
		Namespace:  "test_namespace",
		Region:     "test_region",
		Dimensions: Dimensions{
			{Name: "dim1", Value: "dim_value1"},
		},
	}

	expectedLabels := map[string]string{
		"__name__":   "_test_name",
		"MetricName": "0test-name",
		"Namespace":  "test_namespace",
		"Region":     "test_region",
		"dim1":       "dim_value1",
	}

	labels := metric.Labels()
	assert.Equal(t, expectedLabels, labels, "Labels should correctly replace invalid characters in metric name")
}
