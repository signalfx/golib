package writer

import (
	"sync/atomic"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/sfxclient"
)

// InternalMetrics about the datapoint writer
func (w *DatapointRingWriter) InternalMetrics() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.CumulativeP("datapoint_writer.sent", nil, &w.totalSent),
		sfxclient.CumulativeP("datapoint_writer.filtered", nil, &w.totalFilteredOut),
		sfxclient.CumulativeP("datapoint_writer.received", nil, &w.totalReceived),
		sfxclient.CumulativeP("datapoint_writer.potentially_dropped", nil, &w.totalPotentiallyDropped),
		sfxclient.Gauge("datapoint_writer.channel_len", nil, int64(len(w.inputChan))),
		sfxclient.Gauge("datapoint_writer.in_flight", nil, atomic.LoadInt64(&w.totalInFlight)),
		sfxclient.Gauge("datapoint_writer.waiting", nil, atomic.LoadInt64(&w.requestsWaiting)),
		sfxclient.Gauge("datapoint_writer.requests_active", nil, atomic.LoadInt64(&w.requestsActive)),
	}
}

// InternalMetrics about the datapoint writer
func (w *SpanRingWriter) InternalMetrics() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.CumulativeP("span_writer.sent", nil, &w.totalSent),
		sfxclient.CumulativeP("span_writer.filtered", nil, &w.totalFilteredOut),
		sfxclient.CumulativeP("span_writer.received", nil, &w.totalReceived),
		sfxclient.CumulativeP("span_writer.potentially_dropped", nil, &w.totalPotentiallyDropped),
		sfxclient.Gauge("span_writer.channel_len", nil, int64(len(w.inputChan))),
		sfxclient.Gauge("span_writer.in_flight", nil, atomic.LoadInt64(&w.totalInFlight)),
		sfxclient.Gauge("span_writer.waiting", nil, atomic.LoadInt64(&w.requestsWaiting)),
		sfxclient.Gauge("span_writer.requests_active", nil, atomic.LoadInt64(&w.requestsActive)),
	}
}
