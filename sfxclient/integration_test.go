// +build integration

package sfxclient

import (
	"testing"
	"time"

	"github.com/signalfx/golib/distconf"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

var testEndpoint string
var testAuthToken string

func init() {
	C := distconf.FromLoaders([]distconf.BackingLoader{distconf.EnvLoader(), distconf.IniLoader("../.integration_test_config.ini")})
	testEndpoint = C.Str("client.endpoint", "https://ingest.signalfx.com/v2/datapoint").Get()
	if testEndpoint == "" {
		panic("Please set client.endpoint in env or .integration_test_config.ini")
	}
	testAuthToken = C.Str("auth.token", "").Get()
	if testAuthToken == "" {
		panic("Please set auth.token in env or .integration_test_config.ini")
	}
	log.WithField("testEndpoint", testEndpoint).WithField("testAuthToken", testAuthToken).Info("Init for sfxclient done")
}

func TestClientIT(t *testing.T) {
	ctx := context.Background()
	client := New(testAuthToken)
	client.Endpoint(testEndpoint)
	gauge1Val := int64(0)
	gauge1 := client.Gauge("TestClientIT", &IntAddr{&gauge1Val}).Dimensions(map[string]string{"metric": "1"})
	gauge2Val := float64(.5)
	gauge2 := client.Gauge("TestClientIT", FloatVal(func() float64 { return gauge2Val })).Dimensions(map[string]string{"metric": "2"})

	counter1Val := int64(2)
	counter1 := client.Gauge("TestClientIT", &IntAddr{&counter1Val}).Dimensions(map[string]string{"metric": "3"})

	ccounter1Val := int64(3)
	ccounter1 := client.Gauge("TestClientIT", &IntAddr{&ccounter1Val}).Dimensions(map[string]string{"metric": "3"})
	// For 3 sec, send a point every sec
	for i := 0; i < 3; i++ {
		assert.NoError(t, client.Report(ctx))
		gauge1Val++
		gauge2Val++
		counter1Val++
		ccounter1Val++
		time.Sleep(time.Second)
	}
	client.Remove(gauge1, gauge2, counter1, ccounter1)
}
