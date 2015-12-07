package sfxclient

import (
	"sync"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/errors"
)

// A Timeseries is an object that holds some value over time, reportable to signalfx
type Timeseries struct {
	metric     string
	dimensions map[string]string
	metricType datapoint.MetricType
	value      Value

	mu sync.Mutex
}

// Metric sets the metric name for this time series
func (s *Timeseries) Metric(metric string) *Timeseries {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metric = metric
	return s
}

// Dimensions sets dimensions to the timeseries.
func (s *Timeseries) Dimensions(dimensions map[string]string) *Timeseries {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dimensions = make(map[string]string)
	for k, v := range dimensions {
		s.dimensions[k] = v
	}
	return s
}

// AppendDimensions will append the given dimensions to any dimensions of the timeseries
func (s *Timeseries) AppendDimensions(dimensions map[string]string) *Timeseries {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range dimensions {
		s.dimensions[k] = v
	}
	return s
}

// MetricType changes the metric type of the time series
func (s *Timeseries) MetricType(metricType datapoint.MetricType) *Timeseries {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metricType = metricType
	return s
}

// convertToDatapoint is an internal helper that converts the time series at some point in time
// into a datapoint
func (s *Timeseries) convertToDatapoint(extraDimensions map[string]string, curTime time.Time) (*datapoint.Datapoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, err := s.value.GetValue()
	if err != nil {
		return nil, errors.Annotate(err, "cannot fetch value")
	}
	dims := make(map[string]string, len(extraDimensions)+len(s.dimensions))
	for k, v := range extraDimensions {
		dims[k] = v
	}
	for k, v := range s.dimensions {
		dims[k] = v
	}
	return datapoint.New(s.metric, dims, val, s.metricType, curTime), nil
}
