package log

import (
	"time"
	"gopkg.in/stack.v1"
	"github.com/signalfx/golib/timekeeper"
)

type Dynamic interface {
	LogValue() interface{}
}

type DynamicFunc func() interface{}

func (d DynamicFunc) LogValue() interface{} {
	return d()
}

func copyIfDynamic(keyvals []interface{}) []interface{} {
	var newArray []interface{}
	for i := range keyvals {
		if v, ok := keyvals[i].(Dynamic); ok {
			if newArray == nil {
				newArray = make([]interface{}, len(keyvals))
				copy(newArray, keyvals[0:i])
			}
			newArray[i] = v.LogValue()
			continue
		}
		if newArray != nil {
			newArray[i] = keyvals[i]
		}
	}
	if newArray == nil {
		return keyvals
	}
	return newArray
}

type Caller struct {
	Depth int
}

func (c *Caller) LogValue() interface{} {
	return stack.Caller(c.Depth)
}

type TimeDynamic struct {
	Layout string
	TimeKeeper timekeeper.TimeKeeper
	UTC bool
	AsString bool
}

var _ Dynamic = &TimeDynamic{}

func (t *TimeDynamic) LogValue() interface{} {
	var now time.Time
	if t.TimeKeeper == nil {
		now = time.Now()
	} else {
		now = t.TimeKeeper.Now()
	}
	if t.UTC {
		now = now.UTC()
	}
	if !t.AsString {
		return now
	}
	if t.Layout == "" {
		return now.Format(time.RFC3339)
	}
	return now.Format(t.Layout)
}

var (
	DefaultTimestamp *TimeDynamic = &TimeDynamic{AsString: true}
	DefaultTimestampUTC *TimeDynamic = &TimeDynamic{UTC: true, AsString: true}
	DefaultCaller = &Caller{Depth: 3}
)
