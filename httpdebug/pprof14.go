//go:build !go1.5
// +build !go1.5

// Deprecated: this package is no longer supported.
package httpdebug

import (
	"net/http"
)

func setupTrace(m *http.ServeMux) {
	// Ignored.  Trace not supported in < 1.5
}
