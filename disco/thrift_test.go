package disco

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/zkplus"
	"github.com/signalfx/golib/zkplus/zktest"
	"github.com/stretchr/testify/assert"
)

func TestThrift(t *testing.T) {
	l, err := net.Listen("tcp", "localhost:0")
	assert.NoError(t, err)
	defer l.Close()
	go func() {
		c, err := l.Accept()
		assert.NoError(t, err)
		if err != nil {
			fmt.Printf("Err on accept\n")
			return
		}
		for {
			_, err := c.Write([]byte{0x53})
			if err != nil {
				break
			}
		}
	}()

	s2 := zktest.New()
	z, ch, _ := s2.Connect()
	zkConnFunc := ZkConnCreatorFunc(func() (ZkConn, <-chan zk.Event, error) {
		zkp, err := zkplus.NewBuilder().PathPrefix("/test").Connector(&zkplus.StaticConnector{C: z, Ch: ch}).Build()
		return zkp, zkp.EventChan(), err
	})
	d1, _ := New(zkConnFunc, "localhost")

	s, err := d1.Services("thriftservice")
	updated := make(chan struct{}, 5)
	s.Watch(func() {
		updated <- struct{}{}
	})
	assert.NoError(t, err)
	trans := NewThriftTransport(s, time.Second)
	defer trans.Close()
	trans.WrappedFactory = thrift.NewTTransportFactory()
	assert.NotNil(t, trans)

	assert.False(t, trans.IsOpen())

	assert.NoError(t, trans.Flush())
	assert.NoError(t, trans.Close())

	_, err = trans.Read([]byte{})
	assert.Error(t, err)

	_, err = trans.Write([]byte{})
	assert.Error(t, err)

	err = trans.Open()
	assert.Error(t, err)

	log.Infof("ad on %d\n", nettest.TCPPort(l))
	assert.NoError(t, d1.Advertise("thriftservice", "", nettest.TCPPort(l)))
	<-updated

	assert.NoError(t, trans.NextConnection())
	assert.NoError(t, trans.Open())
	b := []byte{0}
	_, err = trans.Read(b)
	fmt.Printf("%s\n", b)
	assert.NoError(t, err)
	assert.Equal(t, byte(0x53), b[0])

	_, err = trans.Write([]byte{0})
	assert.NoError(t, err)

	assert.NoError(t, trans.Flush())

	assert.NoError(t, trans.NextConnection())
}

var errNope = errors.New("nope")

type errorTransport struct{}

func (d *errorTransport) Flush() (err error) {
	return errNope
}

func (d *errorTransport) IsOpen() bool {
	return false
}

func (d *errorTransport) Close() error {
	return errNope
}

func (d *errorTransport) Read(b []byte) (n int, err error) {
	return 0, errNope
}

func (d *errorTransport) Write(b []byte) (n int, err error) {
	return 0, errNope
}

func (d *errorTransport) Open() error {
	return errNope
}

func TestCurrentTransportErrors(t *testing.T) {
	trans := &ThriftTransport{
		currentTransport: &errorTransport{},
	}
	assert.Error(t, trans.Close())
	trans.currentTransport = &errorTransport{}
	assert.Error(t, trans.Flush())

	trans.currentTransport = &errorTransport{}
	_, err := trans.Read([]byte{})
	assert.Error(t, err)

	trans.currentTransport = &errorTransport{}
	_, err = trans.Write([]byte{})
	assert.Error(t, err)
}

func TestNoGoodInstances(t *testing.T) {
	instances := []ServiceInstance{
		{
			Address: "bad.address.example.com",
			Port:    0,
		},
	}
	trans := &ThriftTransport{
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())),
		service:    &Service{},
	}
	trans.service.services.Store(instances)

	assert.Equal(t, ErrNoInstanceOpen, trans.NextConnection())
}
