package sfxclient

import (
	"runtime"
	"time"
)

// GolangMetricSource gathers and reports generally useful golang system stats for the client
type GolangMetricSource struct {
	ts     []*Timeseries
	client *Reporter
	mstat  runtime.MemStats
}

// NewGolangMetricSource registers the client to report golang system metrics
func NewGolangMetricSource(client *Reporter) *GolangMetricSource {
	dims := map[string]string{
		"instance": "global_stats",
		"stattype": "golang_sys",
	}
	start := time.Now()
	ret := &GolangMetricSource{
		client: client,
	}
	ts := []*Timeseries{
		client.Gauge("Alloc", UInt(&ret.mstat.Alloc)).Dimensions(dims),
		client.Cumulative("TotalAlloc", UInt(&ret.mstat.TotalAlloc)).Dimensions(dims),
		client.Gauge("Sys", UInt(&ret.mstat.Sys)).Dimensions(dims),
		client.Cumulative("Lookups", UInt(&ret.mstat.Lookups)).Dimensions(dims),
		client.Cumulative("Mallocs", UInt(&ret.mstat.Mallocs)).Dimensions(dims),
		client.Cumulative("Frees", UInt(&ret.mstat.Frees)).Dimensions(dims),
		client.Gauge("HeapAlloc", UInt(&ret.mstat.HeapAlloc)).Dimensions(dims),
		client.Gauge("HeapSys", UInt(&ret.mstat.HeapSys)).Dimensions(dims),
		client.Gauge("HeapIdle", UInt(&ret.mstat.HeapIdle)).Dimensions(dims),
		client.Gauge("HeapInuse", UInt(&ret.mstat.HeapInuse)).Dimensions(dims),
		client.Gauge("HeapReleased", UInt(&ret.mstat.HeapReleased)).Dimensions(dims),
		client.Gauge("HeapObjects", UInt(&ret.mstat.HeapObjects)).Dimensions(dims),
		client.Gauge("StackInuse", UInt(&ret.mstat.StackInuse)).Dimensions(dims),
		client.Gauge("StackSys", UInt(&ret.mstat.StackSys)).Dimensions(dims),
		client.Gauge("MSpanInuse", UInt(&ret.mstat.MSpanInuse)).Dimensions(dims),
		client.Gauge("MSpanSys", UInt(&ret.mstat.MSpanSys)).Dimensions(dims),
		client.Gauge("MCacheInuse", UInt(&ret.mstat.MCacheInuse)).Dimensions(dims),
		client.Gauge("MCacheSys", UInt(&ret.mstat.MCacheSys)).Dimensions(dims),
		client.Gauge("BuckHashSys", UInt(&ret.mstat.BuckHashSys)).Dimensions(dims),
		client.Gauge("GCSys", UInt(&ret.mstat.GCSys)).Dimensions(dims),
		client.Gauge("OtherSys", UInt(&ret.mstat.OtherSys)).Dimensions(dims),
		client.Gauge("NextGC", UInt(&ret.mstat.NextGC)).Dimensions(dims),
		client.Gauge("LastGC", UInt(&ret.mstat.LastGC)).Dimensions(dims),
		client.Cumulative("PauseTotalNs", UInt(&ret.mstat.PauseTotalNs)).Dimensions(dims),
		client.Gauge("NumGC", IntVal(func() int64 { return int64(ret.mstat.NumGC) })).Dimensions(dims),

		client.Gauge("GOMAXPROCS", IntVal(func() int64 { return int64(runtime.GOMAXPROCS(0)) })).Dimensions(dims),
		client.Gauge("process.uptime.ns", IntVal(func() int64 { return time.Now().Sub(start).Nanoseconds() })).Dimensions(dims),
		client.Gauge("num_cpu", IntVal(func() int64 { return int64(runtime.NumCPU()) })).Dimensions(dims),
		client.Cumulative("num_cgo_call", IntVal(runtime.NumCgoCall)).Dimensions(dims),
		client.Gauge("num_goroutine", IntVal(func() int64 { return int64(runtime.NumGoroutine()) })).Dimensions(dims),
	}
	ret.ts = ts
	client.PrecollectCallback(ret.precollectCallback)
	return ret
}

func (g *GolangMetricSource) precollectCallback() {
	runtime.ReadMemStats(&g.mstat)
}

// Close the metric source and will stop reporting these system stats to the client
func (g *GolangMetricSource) Close() error {
	g.client.Remove(g.ts...)
	return nil
}
