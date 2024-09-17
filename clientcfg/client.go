// Deprecated: This package will be removed in a future release.
package clientcfg

import (
	"context"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/distconf"
	"github.com/signalfx/golib/v3/errors"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/log"
	"github.com/signalfx/golib/v3/logkey"
	"github.com/signalfx/golib/v3/sfxclient"
	"github.com/signalfx/golib/v3/timekeeper"
	"github.com/signalfx/golib/v3/trace"
)

// ClientConfig configures a SfxClient
type ClientConfig struct {
	SourceName               *distconf.Str
	AuthToken                *distconf.Str
	Endpoint                 *distconf.Str
	ReportingInterval        *distconf.Duration
	ReportingTimeoutInterval *distconf.Duration
	TimeKeeper               timekeeper.TimeKeeper
	OsHostname               func() (name string, err error)
	DisableCompression       *distconf.Bool
	DebugCollectDatapoints   *distconf.Bool
}

// Load the client config values from distconf
func (c *ClientConfig) Load(d *distconf.Distconf) {
	c.SourceName = d.Str("signalfuse.sourceName", "")
	c.AuthToken = d.Str("sf.metrics.auth_token", "")
	c.Endpoint = d.Str("sf.metrics.statsendpoint", "")
	c.ReportingInterval = d.Duration("sf.metrics.report_interval", time.Second)
	c.TimeKeeper = timekeeper.RealTime{}
	c.OsHostname = os.Hostname
	c.DisableCompression = d.Bool("sf.metrics.disableCompression", false)
	c.ReportingTimeoutInterval = d.Duration("sf.metrics.report_timeout_interval", time.Second)
	c.DebugCollectDatapoints = d.Bool("sf.metrics.debug.collect.datapoints", false)
}

// DefaultDimensions extracts default sfxclient dimensions that identify the host
func DefaultDimensions(conf *ClientConfig) (map[string]string, error) {
	signalfuseSourceName := conf.SourceName.Get()
	if signalfuseSourceName != "" {
		return map[string]string{"sf_source": signalfuseSourceName}, nil
	}
	hostname, err := conf.OsHostname()
	if err != nil {
		return nil, errors.Annotate(err, "unable to get hostname for default source")
	}
	return map[string]string{"sf_source": hostname}, nil
}

// ClientConfigChangerSink is a sink that can update auth and endpoint values
type ClientConfigChangerSink struct {
	Destination *sfxclient.HTTPSink
	mu          sync.RWMutex
	logger      log.Logger
	urlParse    func(string) (url *url.URL, err error)
}

// clientCfgChangerSinkInitHelper is a common init for ClientConfigChangerSink
// This api accepts "authToken func()" which if not set, will fall back to default behavior of setting up watcher for
// auth token, otherwise it will get the auth token to be used and do not set a watcher
func clientCfgChangerSinkInitHelper(httpSink *sfxclient.HTTPSink, conf *ClientConfig, authToken func() string, logger log.Logger) *ClientConfigChangerSink {
	ret := &ClientConfigChangerSink{
		Destination: httpSink,
		urlParse:    url.Parse,
		logger:      logger,
	}

	ret.endpointWatch(conf.Endpoint, "")
	ret.disableCompressionWatch(conf.DisableCompression, false)
	conf.Endpoint.Watch(ret.endpointWatch)
	conf.DisableCompression.Watch(ret.disableCompressionWatch)
	if authToken != nil {
		ret.mu.Lock()
		ret.Destination.AuthToken = authToken()
		ret.mu.Unlock()
	} else {
		ret.authTokenWatch(conf.AuthToken, "")
		conf.AuthToken.Watch(ret.authTokenWatch)
	}
	return ret
}

// WatchSinkChanges returns a new ClientConfigChangerSink that wraps a sink with auth/endpoint changes from distconf
func WatchSinkChanges(sink sfxclient.Sink, conf *ClientConfig, logger log.Logger) sfxclient.Sink {
	httpSink, ok := sink.(*sfxclient.HTTPSink)
	if !ok {
		return sink
	}
	return WatchHTTPSinkChange(httpSink, conf, logger)
}

// WatchHTTPSinkChange returns anew ClientConfigChangerSink that takes an http sink, instead of a regular sink
func WatchHTTPSinkChange(httpSink *sfxclient.HTTPSink, conf *ClientConfig, logger log.Logger) *ClientConfigChangerSink {
	return clientCfgChangerSinkInitHelper(httpSink, conf, nil, logger)
}

// SetupSinkClientChanger returns a new ClientConfigChangerSink that wraps a sink with endpoint changes from distconf
// This API should be used if you do not want to have distconf auth watcher
func SetupSinkClientChanger(sink sfxclient.Sink, conf *ClientConfig, authToken func() string, logger log.Logger) sfxclient.Sink {
	if httpSink, ok := sink.(*sfxclient.HTTPSink); ok {
		ret := clientCfgChangerSinkInitHelper(httpSink, conf, authToken, logger)
		return ret
	}
	return sink
}

// AuthUpdate will return a func which can be used for updating auth token if default watcher is not used for token update
func (s *ClientConfigChangerSink) AuthUpdate() func(string) {
	return s.updateToken
}

// AddDatapoints forwards the call to Destination
func (s *ClientConfigChangerSink) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Destination.AddDatapoints(ctx, points)
}

// AddSpans forwards the call to Destination
func (s *ClientConfigChangerSink) AddSpans(ctx context.Context, spans []*trace.Span) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Destination.AddSpans(ctx, spans)
}

// AddEvents forwards the call to Destination
func (s *ClientConfigChangerSink) AddEvents(ctx context.Context, events []*event.Event) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Destination.AddEvents(ctx, events)
}

func (s *ClientConfigChangerSink) disableCompressionWatch(newValue *distconf.Bool, _ bool) {
	s.logger.Log("disableCompression watch")
	s.mu.Lock()
	s.Destination.DisableCompression = newValue.Get()
	s.mu.Unlock()
}

func (s *ClientConfigChangerSink) updateToken(newToken string) {
	s.mu.Lock()
	s.Destination.AuthToken = newToken
	s.mu.Unlock()
}

func (s *ClientConfigChangerSink) authTokenWatch(str *distconf.Str, _ string) {
	s.logger.Log("auth watch")
	s.updateToken(str.Get())
}

// endpointWatch returns a distconf watch that sets the correct ingest endpoint for a signalfx
// client
func (s *ClientConfigChangerSink) endpointWatch(str *distconf.Str, _ string) {
	s.logger.Log("endpoint watch")
	e := str.Get()
	if e == "" {
		return
	}
	if !strings.HasPrefix(e, "http") {
		e = "http://" + e
	}
	u, err := s.urlParse(e)
	if err != nil {
		s.logger.Log(logkey.Endpoint, e, log.Err, err, "unable to parse URL")
		return
	}
	if u.Path == "" {
		u.Path = "/v2/datapoint"
	}
	if e != "" {
		s.logger.Log(logkey.Endpoint, e, logkey.URL, u, "updating reporter endpoint")
		s.mu.Lock()
		s.Destination.DatapointEndpoint = u.String()
		s.mu.Unlock()
	}
}
