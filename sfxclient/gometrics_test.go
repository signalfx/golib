package sfxclient

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestNewGolangMetricSource(t *testing.T) {
	x := New("ABCD")

	testSrvr := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte(`"OK"`))
	}))
	defer testSrvr.Close()
	ctx := context.Background()
	x.Endpoint(testSrvr.URL)

	assert.Equal(t, 0, len(x.metrics))
	s := NewGolangMetricSource(x)
	assert.NotEqual(t, 0, len(x.metrics))
	assert.NoError(t, x.Report(ctx))
	s.Close()
	assert.Equal(t, 0, len(x.metrics))
}
