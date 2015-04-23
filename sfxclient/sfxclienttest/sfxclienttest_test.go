package sfxclienttest

import "testing"

func TestNewMemoryForwarder(t *testing.T) {
	// Small sacrifice for the god of code coverage
	m := NewMemoryForwarder()
	m.AuthToken("")
	m.Endpoint("")
}
