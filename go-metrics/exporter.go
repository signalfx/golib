// Deprecated: this package is no longer supported.
package go_metrics

import (
	"github.com/rcrowley/go-metrics"
	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/sfxclient"
)

var defaultQuantiles = []float64{.25, .5, .9, .99}

type memoryInner struct {
	name string
	dims map[string]string
}

// Harvester provides a way to extract dimensions from a metric name
type Harvester func(name string) (bool, string, map[string]string)

// Exporter wraps the go-metrics registry as an sfxclient.Collector
type Exporter struct {
	r        metrics.Registry
	lastSize int
	dims     map[string]string

	// this will be a leak if you are constantly adding and removing topics, my use case doesn't do that but if yours does...
	memory      map[string]*memoryInner
	zHarvesters []Harvester
}

func (e *Exporter) metricToDatapoints(dps []*datapoint.Datapoint, name string, i interface{}) []*datapoint.Datapoint {
	metricName, dims := e.harvestMetricInfo(name)
	switch metric := i.(type) {
	case metrics.Counter:
		dps = append(dps, sfxclient.Cumulative(metricName, dims, metric.Count()))

	case metrics.Gauge:
		dps = append(dps, sfxclient.Gauge(metricName, dims, metric.Value()))

	case metrics.GaugeFloat64:
		dps = append(dps, sfxclient.GaugeF(metricName, dims, metric.Value()))

	case metrics.Histogram:
		h := metric.Snapshot()
		dps = e.harvestHistoTypeThing(dps, metricName, h, dims)

	case metrics.Meter:
		m := metric.Snapshot()
		dps = append(dps,
			sfxclient.Cumulative(metricName+".count", dims, m.Count()),
			sfxclient.GaugeF(metricName+".1m", dims, m.Rate1()),
			sfxclient.GaugeF(metricName+".5m", dims, m.Rate5()),
			sfxclient.GaugeF(metricName+".15m", dims, m.Rate15()),
			sfxclient.GaugeF(metricName+".meanRate", dims, m.RateMean()),
		)

	case metrics.Timer:
		t := metric.Snapshot()
		dps = e.harvestHistoTypeThing(dps, metricName, t, dims)
	}
	return dps
}

type histoTypeThing interface {
	Count() int64
	Max() int64
	Mean() float64
	Min() int64
	Percentile(float64) float64
	Percentiles([]float64) []float64
	StdDev() float64
	Sum() int64
	Variance() float64
}

func (e *Exporter) harvestMetricInfo(name string) (ret string, retm map[string]string) {
	ret = name
	retm = e.dims
	if len(e.zHarvesters) > 0 {
		var mem *memoryInner
		var ok, broken bool
		if mem, ok = e.memory[name]; !ok {
			for _, h := range e.zHarvesters {
				if ok, s, m := h(name); ok {
					mem = &memoryInner{
						name: s,
						dims: datapoint.AddMaps(m, e.dims),
					}
					broken = true
					break
				}
			}
			if !broken {
				mem = &memoryInner{
					name: name,
					dims: e.dims,
				}
			}
			e.memory[name] = mem
		}
		ret = mem.name
		retm = mem.dims
	}
	return ret, retm
}

func (e *Exporter) harvestHistoTypeThing(dps []*datapoint.Datapoint, metric string, h histoTypeThing, dims map[string]string) []*datapoint.Datapoint {
	ps := h.Percentiles(defaultQuantiles)
	dps = append(dps,
		sfxclient.Cumulative(metric+".count", dims, h.Count()),
		sfxclient.Gauge(metric+".min", dims, h.Min()),
		sfxclient.Gauge(metric+".max", dims, h.Max()),
		sfxclient.GaugeF(metric+".mean", dims, h.Mean()),
		sfxclient.GaugeF(metric+".stdDev", dims, h.StdDev()),
		sfxclient.GaugeF(metric+".p25", dims, ps[0]),
		sfxclient.GaugeF(metric+".median", dims, ps[1]),
		sfxclient.GaugeF(metric+".p90", dims, ps[2]),
		sfxclient.GaugeF(metric+".p99", dims, ps[3]),
	)
	return dps
}

// Datapoints implements sfxclient.Collector
func (e *Exporter) Datapoints() []*datapoint.Datapoint {
	dps := make([]*datapoint.Datapoint, 0, e.lastSize)
	e.r.Each(func(name string, i interface{}) {
		dps = e.metricToDatapoints(dps, name, i)
	})
	e.lastSize = len(dps)

	return dps
}

// New returns a new &Exporter
func New(r metrics.Registry, dims map[string]string, zHarvesters ...Harvester) *Exporter {
	return &Exporter{
		r:           r,
		dims:        dims,
		zHarvesters: zHarvesters,
		memory:      make(map[string]*memoryInner),
	}
}
