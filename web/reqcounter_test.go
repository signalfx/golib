package web

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRequestCounter(t *testing.T) {
	m := RequestCounter{}
	f := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		assert.EqualValues(t, 1, m.ActiveConnections)
		assert.EqualValues(t, 1, m.TotalConnections)
		assert.EqualValues(t, 0, m.TotalProcessingTimeNs)
		time.Sleep(time.Nanosecond)
	})
	m.Wrap(f).ServeHTTP(nil, nil)

	f = func(rw http.ResponseWriter, r *http.Request) {
		assert.EqualValues(t, 1, m.ActiveConnections)
		assert.EqualValues(t, 2, m.TotalConnections)
		assert.NotEqual(t, 0, m.TotalProcessingTimeNs)
	}
	m.ServeHTTP(nil, nil, f)

	assert.Equal(t, 3, len(m.Datapoints()))

	resp, err := m.GRPCInterceptor(context.Background(), nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		assert.EqualValues(t, 1, m.ActiveConnections)
		assert.EqualValues(t, 3, m.TotalConnections)
		assert.NotEqual(t, 0, m.TotalProcessingTimeNs)

		return 5, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 5, resp)
}
