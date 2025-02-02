// Deprecated: this package is no longer supported.
package traceformat

import "github.com/signalfx/golib/v3/trace"

// This package is here to provide classes for easy json code generation and so we can isolate those generated classes
// which do not adhere to our strict coding standards for test coverage or linting

// Span is an alias
//
//easyjson:json
type Span trace.Span

// Trace is an alias
//
//easyjson:json
type Trace trace.Trace

// Endpoint is an alias
//
//easyjson:json
type Endpoint trace.Endpoint

// Annotation is an alias
//
//easyjson:json
type Annotation trace.Annotation
