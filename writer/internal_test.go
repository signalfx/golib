package writer

import "github.com/signalfx/golib/datapoint"

func findInternalMetricWithName(writer Writer, name string) int64 {
	dps := writer.InternalMetrics()
	for _, dp := range dps {
		if dp.Metric == name {
			return dp.Value.(datapoint.IntValue).Int()
		}
	}
	panic("internal metric not found: " + name)
}
