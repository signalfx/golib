package sfxclient

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/metricproxy/protocol/signalfx"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

type roundTripTest func(r *http.Request) (*http.Response, error)

func (r roundTripTest) RoundTrip(req *http.Request) (*http.Response, error) {
	return r(req)
}

func TestReporter(t *testing.T) {
	x := New("ABCD")
	testSrvr := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		assert.Equal(t, "abcdefg", req.Header.Get(signalfx.TokenHeaderName))
		rw.Write([]byte(`"OK"`))
	}))
	defer testSrvr.Close()
	ctx := context.Background()
	x.Endpoint(testSrvr.URL)
	x.DefaultDimensions(map[string]string{})
	x.AuthToken("abcdefg")

	v := int64(0)
	met := x.Cumulative("", &IntAddr{&v})
	cb := 0
	x.PrecollectCallback(func() {
		cb++
	})
	x.DirectDatapointCallback(func(map[string]string) []*datapoint.Datapoint {
		cb++
		return []*datapoint.Datapoint{}
	})
	x.Bucket("name", map[string]string{})
	assert.NoError(t, x.Report(ctx))
	assert.NotNil(t, met)
	assert.Equal(t, 2, cb)
	x.Forwarder(nil)
}

func TestBadContext(t *testing.T) {
	x := New("ABCD")
	testSrvr := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		panic("Should not happen!")
	}))
	defer testSrvr.Close()
	ctx, cancelFunc := context.WithCancel(context.Background())
	x.Endpoint(testSrvr.URL)
	cancelFunc()
	<-ctx.Done()
	assert.Equal(t, context.Canceled, x.Report(ctx))
}

func TestNoMetrics(t *testing.T) {
	x := New("ABC")
	ctx := context.Background()
	assert.NoError(t, x.Report(ctx))
}

func TestRemove(t *testing.T) {
	x := New("ABCD")
	v := int64(0)
	met := x.Cumulative("", &IntAddr{&v})
	assert.NotNil(t, met)
	assert.Equal(t, 1, len(x.metrics))
	x.Remove(met)
	assert.Equal(t, 0, len(x.metrics))
}

func TestGauge(t *testing.T) {
	x := New("ABCD")
	v := int64(0)
	met := x.Gauge("", &IntAddr{&v})
	assert.Equal(t, datapoint.Gauge, met.metricType)
}

func TestCounter(t *testing.T) {
	x := New("ABCD")
	v := int64(0)
	met := x.Counter("", &IntAddr{&v})
	assert.Equal(t, datapoint.Count, met.metricType)
}

type readError struct {
}

func (r *readError) Read([]byte) (int, error) {
	return 0, errors.New("read error")
}

func TestVals(t *testing.T) {
	v := int64(0)
	x := Int(&v)
	d, err := x.GetValue()
	assert.Equal(t, "0", d.String())
	assert.NoError(t, err)

	v++
	d, err = x.GetValue()
	assert.Equal(t, "1", d.String())
	assert.NoError(t, err)

	u := uint64(0)
	z := UInt(&u)

	d, err = z.GetValue()
	assert.Equal(t, "0", d.String())
	assert.NoError(t, err)

	u++
	d, err = z.GetValue()
	assert.Equal(t, "1", d.String())
	assert.NoError(t, err)

	iv := IntVal(func() int64 {
		return v
	})
	d, err = iv.GetValue()
	assert.Equal(t, "1", d.String())
	assert.NoError(t, err)
}
