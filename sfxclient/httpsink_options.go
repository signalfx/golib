package sfxclient

// HTTPSinkOption can be passed to NewHTTPSink to customize it's behaviour
type HTTPSinkOption func(*HTTPSink)

// WithZipkinTraceExporter takes a reference to HTTPSink and configures it to export using the Zipkin protocol.
func WithZipkinTraceExporter() HTTPSinkOption {
	return func(s *HTTPSink) {
		s.traceMarshal = jsonMarshal
		s.contentTypeHeader = contentTypeHeaderJSON
		s.TraceEndpoint = TraceIngestEndpointV1
	}
}
