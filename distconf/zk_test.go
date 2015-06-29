package distconf

import (
	"errors"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/signalfx/golib/zkplus/zktest"
	"github.com/stretchr/testify/assert"
)

func TestZkConf(t *testing.T) {
	log.Info("TestZkConf")
	zkServer := zktest.New()
	z, err := Zk(ZkConnectorFunc(func() (ZkConn, <-chan zk.Event, error) {
		return zkServer.Connect()
	}))
	defer z.Close()
	assert.NoError(t, err)

	b, err := z.Get("TestZkConf")
	assert.NoError(t, err)
	assert.Nil(t, b)

	assert.NoError(t, z.Write("TestZkConf", nil))

	signalChan := make(chan string, 4)
	log.Info("Setting watches")
	z.(Dynamic).Watch("TestZkConf", backingCallbackFunction(func(S string) {
		log.Info("Watch fired!")
		assert.Equal(t, "TestZkConf", S)
		signalChan <- S
	}))

	// The write should work and I should get a single signal on the chan
	log.Info("Doing write 1")
	assert.NoError(t, z.Write("TestZkConf", []byte("newval")))
	log.Info("Write done")
	b, err = z.Get("TestZkConf")
	log.Info("Get done")
	assert.NoError(t, err)
	assert.Equal(t, []byte("newval"), b)
	log.Info("Blocking for values")
	res := <-signalChan
	assert.Equal(t, "TestZkConf", res)

	// Should send another signal
	log.Info("Doing write 2")
	assert.NoError(t, z.Write("TestZkConf", []byte("newval_v2")))
	res = <-signalChan
	assert.Equal(t, "TestZkConf", res)

	log.Info("Doing write 3")
	assert.NoError(t, z.Write("TestZkConf", nil))
	res = <-signalChan
	assert.Equal(t, "TestZkConf", res)
}

func TestCloseNormal(t *testing.T) {
	zkServer := zktest.New()
	zkServer.ForcedErrorCheck(func(s string) error {
		return errors.New("nope")
	})

	z, err := Zk(ZkConnectorFunc(func() (ZkConn, <-chan zk.Event, error) {
		return zkServer.Connect()
	}))
	assert.NoError(t, err)

	z.Close()

	// Should not deadlock
	<-z.(*zkConfig).shouldQuit
}

func TestErrorReregister(t *testing.T) {
	zkServer := zktest.New()
	zkServer.ChanTimeout = time.Millisecond

	z, err := Zk(ZkConnectorFunc(func() (ZkConn, <-chan zk.Event, error) {
		return zkServer.Connect()
	}))
	assert.NoError(t, err)
	defer z.Close()
	z.(Dynamic).Watch("hello", func(string) {

	})
	zkServer.ForcedErrorCheck(func(s string) error {
		return errors.New("nope")
	})
	z.(*zkConfig).setRefreshDelay(time.Millisecond)
	go func() {
		time.Sleep(time.Millisecond * 10)
		zkServer.ForcedErrorCheck(nil)
	}()
	z.(*zkConfig).refreshWatches()
}

func TestCloseQuitChan(t *testing.T) {
	zkServer := zktest.New()
	zkServer.ForcedErrorCheck(func(s string) error {
		return errors.New("nope")
	})

	z, err := Zk(ZkConnectorFunc(func() (ZkConn, <-chan zk.Event, error) {
		return zkServer.Connect()
	}))
	assert.NoError(t, err)

	// Should not deadlock
	close(z.(*zkConfig).shouldQuit)

	// Give drain() loop time to exit, for code coverage
	time.Sleep(time.Millisecond * 100)
}

func TestZkConfErrors(t *testing.T) {
	zkServer := zktest.New()
	zkServer.ForcedErrorCheck(func(s string) error {
		return errors.New("nope")
	})
	zkServer.ChanTimeout = time.Millisecond * 10

	z, err := Zk(ZkConnectorFunc(func() (ZkConn, <-chan zk.Event, error) {
		return zkServer.Connect()
	}))
	defer z.Close()
	assert.NoError(t, err)

	_, err = z.Get("TestZkConfErrors")
	assert.Error(t, err)

	assert.Error(t, z.(Dynamic).Watch("TestZkConfErrors", nil))
	assert.Error(t, z.Write("TestZkConfErrors", nil))
	assert.Error(t, z.(*zkConfig).reregisterWatch("TestZkConfErrors"))

	z.(*zkConfig).conn.Close()

	//	zkp.GlobalChan <- zk.Event{
	//		State: zk.StateDisconnected,
	//	}
	//	// Let the thread switch back to get code coverage
	time.Sleep(10 * time.Millisecond)
	//	zkp.Close()
}

func TestErrorLoader(t *testing.T) {
	_, err := Zk(ZkConnectorFunc(func() (ZkConn, <-chan zk.Event, error) {
		return nil, nil, errors.New("nope")
	}))
	assert.Error(t, err)
}
