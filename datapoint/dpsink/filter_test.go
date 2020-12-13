package dpsink

import (
	"context"
	"runtime"
	"testing"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/datapoint/dptest"
	"github.com/stretchr/testify/assert"
)

func TestEmptyMetricFilter(t *testing.T) {
	end := dptest.NewBasicSink()
	end.Resize(1)
	ctx := context.Background()

	filt := EmptyMetricFilter{}

	p1 := dptest.DP()
	p2 := dptest.DP()
	p1.Metric = ""
	assert.NoError(t, filt.AddDatapoints(ctx, []*datapoint.Datapoint{p1, p2}, end))
	out := <-end.PointsChan
	assert.Equal(t, 1, len(out))
	assert.Equal(t, int64(1), filt.EmptyMetricFiltered)

	assert.NoError(t, filt.AddDatapoints(ctx, []*datapoint.Datapoint{p1}, end))
	runtime.Gosched()
	assert.Equal(t, 0, len(end.PointsChan))
}
