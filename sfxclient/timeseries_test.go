package sfxclient

import (
	"bytes"
	"testing"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/stretchr/testify/assert"
)

func TestSettersTs(t *testing.T) {
	ts := &Timeseries{}
	ts.Metric("A")
	ts.Dimensions(map[string]string{"A": "A"})
	ts.MetricType(datapoint.Gauge)
	assert.Equal(t, "A", ts.metric)
	assert.Equal(t, map[string]string{"A": "A"}, ts.dimensions)
	assert.Equal(t, map[string]string{"A": "A"}, ts.dimensions)
	assert.Equal(t, datapoint.Gauge, ts.metricType)
	i := int64(0)
	ts.value = &IntAddr{&i}
	at := time.Now()
	dp, err := ts.convertToDatapoint(map[string]string{"B": "B"}, at)
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"A": "A", "B": "B"}, dp.Dimensions)

	ts.value = Val(bytes.NewBufferString("abc"))
	dp, err = ts.convertToDatapoint(map[string]string{"B": "B"}, at)
	assert.Error(t, err)

	ts.value = Val(bytes.NewBufferString("123"))
	dp, err = ts.convertToDatapoint(map[string]string{"B": "B"}, at)
	assert.NoError(t, err)
	assert.Equal(t, "123", dp.Value.String())

	ts.value = Val(bytes.NewBufferString("123.5"))
	dp, err = ts.convertToDatapoint(map[string]string{"B": "B"}, at)
	assert.NoError(t, err)
	assert.Equal(t, "123.5", dp.Value.String())

	ts.value = FloatVal(func() float64 { return 4.5 })
	dp, err = ts.convertToDatapoint(map[string]string{"B": "B"}, at)
	assert.NoError(t, err)
	assert.Equal(t, "4.5", dp.Value.String())

	ts.Dimensions(map[string]string{"c": "C"})
	ts.AppendDimensions(map[string]string{"a": "b"})
	assert.Equal(t, map[string]string{"c": "C", "a": "b"}, ts.dimensions)
}
