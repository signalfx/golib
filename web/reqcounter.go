package web

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/sfxclient"
	"google.golang.org/grpc"
)

// RequestCounter is a negroni handler that tracks connection stats
type RequestCounter struct {
	TotalConnections      int64
	ActiveConnections     int64
	TotalProcessingTimeNs int64
}

var (
	_ HTTPConstructor = (&RequestCounter{}).Wrap
	_ NextHTTP        = (&RequestCounter{}).ServeHTTP
)

// Wrap returns a handler that forwards calls to next and counts the calls forwarded
func (m *RequestCounter) Wrap(next http.Handler) http.Handler {
	f := func(w http.ResponseWriter, r *http.Request) {
		m.ServeHTTP(w, r, next)
	}
	return http.HandlerFunc(f)
}

// wrapRequest will wrap the request handler func in a generic way that times the request and
// maintains counts for org metrics.
func (m *RequestCounter) wrapRequest(handler func()) {
	atomic.AddInt64(&m.TotalConnections, 1)
	atomic.AddInt64(&m.ActiveConnections, 1)
	defer atomic.AddInt64(&m.ActiveConnections, -1)
	start := time.Now()

	handler()

	reqDuration := time.Since(start)
	atomic.AddInt64(&m.TotalProcessingTimeNs, reqDuration.Nanoseconds())
}

// ServeHTTP makes an HTTP handler that tracks the requests.
func (m *RequestCounter) ServeHTTP(rw http.ResponseWriter, r *http.Request, next http.Handler) {
	m.wrapRequest(func() { next.ServeHTTP(rw, r) })
}

// GRPCInterceptor makes a unary GRPC interceptor to track requests.
func (m *RequestCounter) GRPCInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	var resp interface{}
	var err error

	m.wrapRequest(func() {
		// This is safe because WrapRequest should always call this func synchronously and never in
		// a separate goroutine.
		resp, err = handler(ctx, req)
	})

	return resp, err
}

// Datapoints returns stats on total connections, active connections, and total processing time
func (m *RequestCounter) Datapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.Cumulative("total_connections", nil, atomic.LoadInt64(&m.TotalConnections)),
		sfxclient.Cumulative("total_time_ns", nil, atomic.LoadInt64(&m.TotalProcessingTimeNs)),
		sfxclient.Gauge("active_connections", nil, atomic.LoadInt64(&m.ActiveConnections)),
	}
}
