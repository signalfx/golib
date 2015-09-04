package sfxclient

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/logherd"
	// TODO: Figure out a way to not have this dependency
	"expvar"

	"github.com/signalfx/metricproxy/dp/dpsink"
	"github.com/signalfx/metricproxy/protocol/signalfx"
	"golang.org/x/net/context"
)

var log *logrus.Logger

func init() {
	log = logherd.New()
}

// ClientSink is the expected interface that recieves reported metrics
type ClientSink interface {
	dpsink.Sink
	Endpoint(endpoint string)
	AuthToken(endpoint string)
}

// New creates a new SignalFx client
func New(authToken string) *Reporter {
	forwarder := signalfx.NewSignalfxJSONForwarder("https://ingest.signalfx.com/v2/datapoint", time.Second*10, authToken, 10, "", "", "")
	forwarder.UserAgent(fmt.Sprintf("SignalFxGo/0.2 (gover %s)", runtime.Version()))
	return &Reporter{
		defaultDimensions:  make(map[string]string),
		previousDatapoints: []*datapoint.Datapoint{},
		metrics:            make(map[*Timeseries]struct{}),
		preCallbacks:       []func(){},
		buckets:            make(map[*Bucket]struct{}),
		forwarder:          forwarder,
	}
}

// ErrNoauthToken is returned by Report() if no auth token has been set.
var ErrNoauthToken = errors.New("no auth token")

// DirectCallback is a functional callback that can be passed to DirectDatapointCallback as a way
// to have the caller calculate and return their own datapoints
type DirectCallback func(defaultDims map[string]string) []*datapoint.Datapoint

// A Reporter reports metrics to SignalFx
type Reporter struct {
	defaultDimensions map[string]string

	forwarder     ClientSink
	forwarderLock sync.Mutex

	metrics                  map[*Timeseries]struct{}
	buckets                  map[*Bucket]struct{}
	preCallbacks             []func()
	directDatapointCallbacks []DirectCallback

	previousDatapoints []*datapoint.Datapoint
	mu                 sync.Mutex
}

// Endpoint controls which URL metrics are reported to
func (s *Reporter) Endpoint(endpoint string) {
	s.forwarderLock.Lock()
	defer s.forwarderLock.Unlock()
	s.forwarder.Endpoint(endpoint)
}

// Forwarder controls where metrics are reported to
func (s *Reporter) Forwarder(forwarder ClientSink) *Reporter {
	s.forwarderLock.Lock()
	defer s.forwarderLock.Unlock()
	s.forwarder = forwarder
	return s
}

// AuthToken sets the auth token used to authenticate this session.
func (s *Reporter) AuthToken(authToken string) {
	s.forwarderLock.Lock()
	defer s.forwarderLock.Unlock()
	s.forwarder.AuthToken(authToken)
}

// DefaultDimensions sets default dimensions that are added to any datapoints reported
func (s *Reporter) DefaultDimensions(defaultDimensions map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultDimensions = defaultDimensions
}

// Cumulative creates a new cumulative counter timeseries
func (s *Reporter) Cumulative(metricName string, value Value) *Timeseries {
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := &Timeseries{
		metricType: datapoint.Counter,
		metric:     metricName,
		value:      value,
	}
	s.metrics[ret] = struct{}{}
	return ret
}

// Gauge creates a new Gauge timeseries
func (s *Reporter) Gauge(metricName string, value Value) *Timeseries {
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := &Timeseries{
		metricType: datapoint.Gauge,
		metric:     metricName,
		value:      value,
	}
	s.metrics[ret] = struct{}{}
	return ret
}

// Counter creates a new Counter timeseries
func (s *Reporter) Counter(metricName string, value Value) *Timeseries {
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := &Timeseries{
		metricType: datapoint.Count,
		metric:     metricName,
		value:      value,
	}
	s.metrics[ret] = struct{}{}
	return ret
}

// Bucket creates a new value bucket and associates it with the Reporter
func (s *Reporter) Bucket(metricName string, dimensions map[string]string) *Bucket {
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := &Bucket{
		metricName: metricName,
		dimensions: dimensions,
	}
	s.buckets[ret] = struct{}{}
	return ret
}

// Remove any time series from this client
func (s *Reporter) Remove(timeseries ...*Timeseries) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range timeseries {
		delete(s.metrics, t)
	}
}

// PrecollectCallback adds a function that is called before Report().  This is useful for refetching
// things like runtime.Memstats() so they are only fetched once per report() call
func (s *Reporter) PrecollectCallback(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.preCallbacks = append(s.preCallbacks, f)
}

// DirectDatapointCallback adds a callback that itself will generate datapoints to report
func (s *Reporter) DirectDatapointCallback(f DirectCallback) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.directDatapointCallbacks = append(s.directDatapointCallbacks, f)
}

func (s *Reporter) collectDatapoints(ctx context.Context) ([]*datapoint.Datapoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	now := time.Now()
	for _, f := range s.preCallbacks {
		f()
	}
	datapoints := make([]*datapoint.Datapoint, 0, len(s.metrics))
	for metric := range s.metrics {
		dp, err := metric.convertToDatapoint(s.defaultDimensions, now)
		if err == nil {
			datapoints = append(datapoints, dp)
		}
	}
	for _, c := range s.directDatapointCallbacks {
		// Note: Add default dimensions
		datapoints = append(datapoints, c(s.defaultDimensions)...)
	}
	for b := range s.buckets {
		datapoints = append(datapoints, b.datapoints(s.defaultDimensions, now)...)
	}
	return datapoints, nil
}

// Var returns an expvar variable that prints the values of the previously reported datapoints
func (s *Reporter) Var() expvar.Var {
	return expvar.Func(func() interface{} {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.previousDatapoints
	})
}

// Report any metrics saved in this reporter to SignalFx
func (s *Reporter) Report(ctx context.Context) error {
	datapoints, err := s.collectDatapoints(ctx)
	if err != nil {
		return err
	}
	if len(datapoints) == 0 {
		return nil
	}
	s.forwarderLock.Lock()
	defer s.forwarderLock.Unlock()
	s.mu.Lock()
	s.previousDatapoints = datapoints
	s.mu.Unlock()
	return s.forwarder.AddDatapoints(ctx, datapoints)
}
