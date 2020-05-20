package logsink

import (
	"context"
)

// Logs is a list of logs
type Logs []*Log

// Log is a record or event that has occurred
type Log struct {
	TraceID        string                 `json:"traceId,omitempty"` // byte sequences
	SpanID         string                 `json:"spanId,omitempty"`  // byte sequences
	SeverityText   string                 `json:"severityText,omitempty"`
	ShortName      string                 `json:"shortName,omitempty"`
	Body           interface{}            `json:"body"`
	TimeStamp      uint64                 `json:"timestamp"`
	Resource       map[string]interface{} `json:"resource,omitempty"`
	Attributes     map[string]interface{} `json:"attributes,omitempty"`
	TraceFlag      byte                   `json:"traceFlags,omitempty"`
	SeverityNumber int8                   `json:"severityNumber,omitempty"`
}

// A Sink is an object that can accept log record and do something with them, life forward them to some endpoint
type Sink interface {
	AddLogs(ctx context.Context, logs []*Log) error
}

// A MiddlewareConstructor is used by FromChain to chain together a bunch of sinks that forward to each other
type MiddlewareConstructor func(sendTo Sink) Sink

// FromChain creates an endpoint Sink that sends calls between multiple middlewares for things like counting traces in between.
func FromChain(endSink Sink, sinks ...MiddlewareConstructor) Sink {
	for i := len(sinks) - 1; i >= 0; i-- {
		endSink = sinks[i](endSink)
	}
	return endSink
}

// NextSink is a special case of a sink that forwards to another sink
type NextSink interface {
	AddLogs(ctx context.Context, logs []*Log, next Sink) error
}

type nextWrapped struct {
	forwardTo Sink
	wrapping  NextSink
}

func (n *nextWrapped) AddLogs(ctx context.Context, logs []*Log) error {
	return n.wrapping.AddLogs(ctx, logs, n.forwardTo)
}

// NextWrap wraps a NextSink to make it usable by MiddlewareConstructor
func NextWrap(wrapping NextSink) MiddlewareConstructor {
	return func(sendTo Sink) Sink {
		return &nextWrapped{
			forwardTo: sendTo,
			wrapping:  wrapping,
		}
	}
}
