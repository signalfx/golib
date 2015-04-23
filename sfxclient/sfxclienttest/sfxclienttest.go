package sfxclienttest

import (
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/metricproxy/dp/dptest"
)

// MemoryForwarder is an implementation of a sfxclient ClientSink that stores datapoints in
// memory
type MemoryForwarder struct {
	*dptest.BasicSink
}

var _ sfxclient.ClientSink = &MemoryForwarder{}

// AuthToken does nothing
func (m *MemoryForwarder) AuthToken(_ string) {
}

// Endpoint does nothing
func (m *MemoryForwarder) Endpoint(_ string) {
}

// NewMemoryForwarder creates a MemoryForwarder with a zero sink size
func NewMemoryForwarder() *MemoryForwarder {
	return &MemoryForwarder{
		BasicSink: dptest.NewBasicSink(),
	}
}
