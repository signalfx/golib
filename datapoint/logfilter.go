package datapoint

import (
	"expvar"
	"sync"
)

// LogFilter is a log.Filter type that allows filtering datapoints
type LogFilter struct {
	MetricKeyName  string
	CheckLogSignal bool
	dimensions     map[string]string
	mu             sync.RWMutex
}

// LogKey is the expected Key datapoints should be sent as
var LogKey = "dp"

// SetLogSignal sets a Meta field on the datapoint that makes LogSignal return true for the DP
func SetLogSignal(dp *Datapoint) *Datapoint {
	dp.Meta[logProperties] = struct{}{}
	return dp
}

// HasLogSignal returns true if SetLogSignal was previously called on the datapoint
func HasLogSignal(dp *Datapoint) bool {
	_, exists := dp.Meta[logProperties]
	return exists
}

// WouldLog returns true if there is a dimension "dp" that matches dimensions in LogFilter set
// dimensions
func (l *LogFilter) WouldLog(keyvals ...interface{}) bool {
	l.mu.RLock()
	dimsToCheck := l.dimensions
	l.mu.RUnlock()
	if !l.CheckLogSignal && len(dimsToCheck) == 0 {
		return false
	}
	for i := 1; i < len(keyvals); i += 2 {
		keyname, ok := keyvals[i-1].(string)
		if ok && keyname == LogKey {
			dp, ok := keyvals[i].(*Datapoint)
			if !ok {
				return false
			}
			if l.CheckLogSignal && HasLogSignal(dp) {
				return true
			}
			return DPMatches(dimsToCheck, dp, l.MetricKeyName)
		}
	}
	return false
}

// DPMatches returns true if dimsToCheck is non empty, dp is not nil, and the given dimensions
// are all inside dp.Dimensions
func DPMatches(dimsToCheck map[string]string, dp *Datapoint, metricKeyName string) bool {
	if len(dimsToCheck) == 0 {
		return false
	}
	for k, v := range dimsToCheck {
		if metricKeyName != "" && k == metricKeyName {
			if dp.Metric != v {
				return false
			}
			continue
		}
		dimVal, exists := dp.Dimensions[k]
		if !exists || dimVal != v {
			return false
		}
	}
	return true
}

// SetDimensions controls which dimensions are filtered
func (l *LogFilter) SetDimensions(dims map[string]string) {
	l.mu.Lock()
	l.dimensions = dims
	l.mu.Unlock()
}

// Var returns the dimensions that are being filtered
func (l *LogFilter) Var() expvar.Var {
	return expvar.Func(func() interface{} {
		l.mu.RLock()
		ret := l.dimensions
		l.mu.RUnlock()
		return ret
	})
}

// Disabled returns true if there are no filtered dimensions set
func (l *LogFilter) Disabled() bool {
	if l.CheckLogSignal {
		return false
	}
	l.mu.RLock()
	dims := l.dimensions
	l.mu.RUnlock()
	return len(dims) == 0
}
