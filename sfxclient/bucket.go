package sfxclient

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/signalfx/golib/datapoint"
)

// A Bucket trakcs groups of values, reporting the min/max as gauges, and count/sum/sum of squares
// as a cumulative counter
type Bucket struct {
	metricName   string
	dimensions   map[string]string
	Count        int64
	Min          int64
	Max          int64
	Sum          int64
	SumOfSquares int64
}

// Result is a cumulated result of items that can be added to the bucket
type Result struct {
	Count        int64
	Sum          int64
	SumOfSquares int64
	Min          int64
	Max          int64
}

// Add a single number to the bucket.
func (r *Result) Add(val int64) {
	r.Count++
	r.Sum += val
	r.SumOfSquares += val * val
	if r.Count == 1 {
		r.Min = val
		r.Max = val
	} else {
		if r.Min > val {
			r.Min = val
		}
		if r.Max < val {
			r.Max = val
		}
	}
}

// Add an item to the bucket, later reporting the result in the next report cycle.
func (b *Bucket) Add(val int64) {
	b.MultiAdd(&Result{
		Count:        1,
		Sum:          val,
		SumOfSquares: val * val,
		Min:          val,
		Max:          val,
	})
}

// MultiAdd many items into the bucket at once
func (b *Bucket) MultiAdd(res *Result) {
	if res.Count == 0 {
		return
	}
	newCount := atomic.AddInt64(&b.Count, res.Count)
	atomic.AddInt64(&b.Sum, res.Sum)
	atomic.AddInt64(&b.SumOfSquares, res.SumOfSquares)
	for {
		oldMin := atomic.LoadInt64(&b.Min)
		if (newCount != res.Count && oldMin <= res.Min) || atomic.CompareAndSwapInt64(&b.Min, oldMin, res.Min) {
			break
		}
	}

	for {
		oldMax := atomic.LoadInt64(&b.Max)
		if (newCount != 1 && oldMax >= res.Max) || atomic.CompareAndSwapInt64(&b.Max, oldMax, res.Max) {
			break
		}
	}
}

func (b *Bucket) result() Result {
	return Result{
		Min:          atomic.SwapInt64(&b.Min, math.MaxInt64),
		Max:          atomic.SwapInt64(&b.Max, math.MinInt64),
		Count:        atomic.LoadInt64(&b.Count),
		Sum:          atomic.LoadInt64(&b.Sum),
		SumOfSquares: atomic.LoadInt64(&b.SumOfSquares),
	}
}

func (b *Bucket) dimFor(defaultDims map[string]string, rollup string) map[string]string {
	dims := make(map[string]string, len(defaultDims)+len(b.dimensions)+1)
	for k, v := range defaultDims {
		dims[k] = v
	}
	for k, v := range b.dimensions {
		dims[k] = v
	}
	dims["rollup"] = rollup
	return dims
}

func (b *Bucket) datapoints(defaultDims map[string]string, now time.Time) []*datapoint.Datapoint {
	r := b.result()
	res := make([]*datapoint.Datapoint, 0, 5)

	res = append(res,
		datapoint.New(b.metricName, b.dimFor(defaultDims, "count"), datapoint.NewIntValue(r.Count), datapoint.Counter, now))

	res = append(res,
		datapoint.New(b.metricName, b.dimFor(defaultDims, "sum"), datapoint.NewIntValue(r.Sum), datapoint.Counter, now))

	res = append(res,
		datapoint.New(b.metricName, b.dimFor(defaultDims, "sumsquare"), datapoint.NewIntValue(r.SumOfSquares), datapoint.Counter, now),
	)

	if r.Count != 0 && r.Min != math.MaxInt64 {
		res = append(res,
			datapoint.New(b.metricName+".min", b.dimFor(defaultDims, "min"), datapoint.NewIntValue(r.Min), datapoint.Gauge, now))
	}

	if r.Count != 0 && r.Max != math.MinInt64 {
		res = append(res,
			datapoint.New(b.metricName+".max", b.dimFor(defaultDims, "max"), datapoint.NewIntValue(r.Max), datapoint.Gauge, now))
	}
	return res
}
