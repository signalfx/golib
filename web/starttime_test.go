package web

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAddRequestTime(t *testing.T) {
	now := time.Now()
	time.Sleep(time.Nanosecond)
	f := HandlerFunc(func(ctx context.Context, rw http.ResponseWriter, r *http.Request) {
		rt := RequestTime(ctx)
		assert.True(t, now.Before(rt))
		time.Sleep(time.Nanosecond)
		assert.True(t, time.Now().After(rt))
	})
	AddRequestTime(context.Background(), nil, nil, f)
}
