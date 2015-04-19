package expvar2

import (
	"expvar"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

var toFind = "asdfasdfdsa"

func init() {
	expvar.NewString("expvar2.TestExpvarHandler.a").Set("abc")
	expvar.NewString("expvar2.TestExpvarHandler.b").Set(toFind)
}

func TestExpvarHandler(t *testing.T) {
	w := httptest.NewRecorder()
	ExpvarHandler(w, nil)
	assert.Contains(t, w.Body.String(), toFind)
}
