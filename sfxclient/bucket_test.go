package sfxclient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBucket(t *testing.T) {
	b := &Bucket{}
	assert.Equal(t, 3, len(b.datapoints(nil, time.Now())))
	b.Add(1)
	b.Add(2)
	b.Add(3)

	r := b.result()
	assert.Equal(t, int64(3), r.Max)
	assert.Equal(t, int64(1), r.Min)
	assert.Equal(t, int64(3), r.Count)
	assert.Equal(t, int64(6), r.Sum)
	assert.Equal(t, int64(14), r.SumOfSquares)

	b.Add(4)
	b.Add(10)
	b.Add(1)
	r = b.result()
	assert.Equal(t, int64(10), r.Max)
	assert.Equal(t, int64(1), r.Min)

	dims := map[string]string{"a": "b"}
	b.Add(1)
	b.dimensions = map[string]string{"c": "c"}
	assert.Equal(t, 5, len(b.datapoints(dims, time.Now())))
}

func TestResult(t *testing.T) {
	b := &Bucket{}
	r := &Result{}
	b.MultiAdd(r)
	assert.Equal(t, b.result().Count, int64(0))
	r.Add(1)
	r.Add(2)
	r.Add(0)
	b.MultiAdd(r)
	rOut := b.result()
	assert.Equal(t, rOut.Count, int64(3))
	assert.Equal(t, rOut.Sum, int64(3))
	assert.Equal(t, rOut.Min, int64(0))
}
