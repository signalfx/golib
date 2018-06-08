package sfxclient

import (
	"sync/atomic"

	"github.com/signalfx/golib/datapoint"
)

// A CumulativeCollector tracks an ever-increasing cumulative counter
type CumulativeCollector struct {
	MetricName string
	Dimensions map[string]string

	count int64
}

var _ Collector = &CumulativeCollector{}

// Add an item to the bucket, later reporting the result in the next report cycle.
func (c *CumulativeCollector) Add(val int64) {
	atomic.AddInt64(&c.count, val)
}

// Datapoints returns the counter datapoint, or nil if there is no set metric name
func (c *CumulativeCollector) Datapoints() []*datapoint.Datapoint {
	if c.MetricName == "" {
		return []*datapoint.Datapoint{}
	}
	return []*datapoint.Datapoint{
		CumulativeP(c.MetricName, c.Dimensions, &c.count),
	}
}
