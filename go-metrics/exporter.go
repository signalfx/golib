package go_metrics

import (
	"github.com/rcrowley/go-metrics"
	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/sfxclient"
)

var defaultQuantiles = []float64{.25, .5, .9, .99}

// Exporter wraps the go-metrics registry as an sfxclient.Collector
type Exporter struct {
	r        metrics.Registry
	lastSize int
	dims     map[string]string
}

func (e *Exporter) metricToDatapoints(dps []*datapoint.Datapoint, name string, i interface{}) []*datapoint.Datapoint {
	switch metric := i.(type) {
	case metrics.Counter:
		dps = append(dps, sfxclient.Cumulative(name, e.dims, metric.Count()))

	case metrics.Gauge:
		dps = append(dps, sfxclient.Gauge(name, e.dims, metric.Value()))

	case metrics.GaugeFloat64:
		dps = append(dps, sfxclient.GaugeF(name, e.dims, metric.Value()))

	case metrics.Histogram:
		h := metric.Snapshot()
		dps = e.harvestHistoTypeThing(dps, name, h)

	case metrics.Meter:
		m := metric.Snapshot()
		dps = append(dps,
			sfxclient.Cumulative(name+".count", e.dims, m.Count()),
			sfxclient.GaugeF(name+".1m", e.dims, m.Rate1()),
			sfxclient.GaugeF(name+".5m", e.dims, m.Rate5()),
			sfxclient.GaugeF(name+".15m", e.dims, m.Rate15()),
			sfxclient.GaugeF(name+".meanRate", e.dims, m.RateMean()),
		)

	case metrics.Timer:
		t := metric.Snapshot()
		dps = e.harvestHistoTypeThing(dps, name, t)
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

func (e *Exporter) harvestHistoTypeThing(dps []*datapoint.Datapoint, name string, h histoTypeThing) []*datapoint.Datapoint {
	ps := h.Percentiles(defaultQuantiles)
	dps = append(dps,
		sfxclient.Cumulative(name+".count", e.dims, h.Count()),
		sfxclient.Gauge(name+".min", e.dims, h.Min()),
		sfxclient.Gauge(name+".max", e.dims, h.Max()),
		sfxclient.GaugeF(name+".mean", e.dims, h.Mean()),
		sfxclient.GaugeF(name+".stdDev", e.dims, h.StdDev()),
		sfxclient.GaugeF(name+".p25", e.dims, ps[0]),
		sfxclient.GaugeF(name+".median", e.dims, ps[1]),
		sfxclient.GaugeF(name+".p90", e.dims, ps[2]),
		sfxclient.GaugeF(name+".p99", e.dims, ps[3]),
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
func New(r metrics.Registry, dims map[string]string) *Exporter {
	return &Exporter{r: r, dims: dims}
}
