package disco

import (
	"errors"
	"testing"

	"fmt"
	"strings"
	"testing/iotest"

	"bytes"

	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/signalfx/golib/zkplus"
	"github.com/signalfx/golib/zkplus/zktest"
	"github.com/stretchr/testify/require"
)

func TestUnableToConn(t *testing.T) {
	_, err := New(ZkConnCreatorFunc(func() (ZkConn, <-chan zk.Event, error) { return nil, nil, errors.New("bad") }), "")
	require.Error(t, err)
}

func TestBadRandomSource(t *testing.T) {
	r := iotest.TimeoutReader(strings.NewReader(""))
	r.Read([]byte{})
	r.Read([]byte{})
	r.Read([]byte{})
	_, err := NewRandSource(nil, "", r)
	require.Error(t, err)
}

func TestAdvertiseInZKErrs(t *testing.T) {
	s := zktest.New()
	z, ch, _ := s.Connect()
	b := zkplus.NewBuilder().PathPrefix("/test").Connector(&zkplus.StaticConnector{C: z, Ch: ch})
	z.Create("/test", []byte(""), 0, zk.WorldACL(zk.PermAll))
	d1, _ := New(BuilderConnector(b), "TestDupAdvertise")
	require.Nil(t, d1.Advertise("service1", "", uint16(1234)))
	e1 := errors.New("nope")

	z.ForcedErrorCheck(func(s string) error {
		if s == "delete" {
			return e1
		}
		return nil
	})
	require.Equal(t, e1, d1.advertiseInZK(true, "service1", ServiceInstance{}))

	z.ForcedErrorCheck(func(s string) error {
		if s == "exists" {
			return e1
		}
		return nil
	})
	require.Equal(t, e1, d1.advertiseInZK(false, "", ServiceInstance{}))
}

func TestDupAdvertise(t *testing.T) {
	s := zktest.New()
	z, ch, _ := s.Connect()
	testDupAdvertise(t, z, ch)

}

func testDupAdvertise(t *testing.T, z zktest.ZkConnSupported, ch <-chan zk.Event) {
	b := zkplus.NewBuilder().PathPrefix("/test").Connector(&zkplus.StaticConnector{C: z, Ch: ch})
	myID := "AAAAAAAAAAAAAAAA"
	guidStr := "41414141414141414141414141414141"
	d1, _ := NewRandSource(BuilderConnector(b), "TestDupAdvertise", bytes.NewBufferString(myID))
	defer d1.Close()
	z.Create("/test", []byte(""), 0, zk.WorldACL(zk.PermAll))
	z.Create("/test/t1", []byte(""), 0, zk.WorldACL(zk.PermAll))
	_, err := z.Create(fmt.Sprintf("/test/t1/%s", guidStr), []byte("nope"), 0, zk.WorldACL(zk.PermAll))
	require.Nil(t, err)
	require.Nil(t, d1.Advertise("t1", "", uint16(1234)))
	data, _, err := z.Get(fmt.Sprintf("/test/t1/%s", guidStr))
	require.Nil(t, err)
	require.NotEqual(t, string(data), "nope")
	close(d1.manualEvents)
	require.Equal(t, ErrDuplicateAdvertise, d1.Advertise("t1", "", uint16(1234)))
}

func TestJsonMarshalBadAdvertise(t *testing.T) {
	s := zktest.New()
	z, ch, _ := s.Connect()
	b := zkplus.NewBuilder().PathPrefix("/test").Connector(&zkplus.StaticConnector{C: z, Ch: ch})

	d1, _ := New(BuilderConnector(b), "TestAdvertise1")
	e := errors.New("nope")
	d1.jsonMarshal = func(v interface{}) ([]byte, error) {
		return nil, e
	}

	require.Equal(t, e, d1.Advertise("TestAdvertiseService", "", (uint16)(1234)))
}

func TestErrorNoRootCreate(t *testing.T) {
	s := zktest.New()
	z, ch, _ := s.Connect()
	zkConnFunc := ZkConnCreatorFunc(func() (ZkConn, <-chan zk.Event, error) {
		zkp, err := zkplus.NewBuilder().PathPrefix("/test").Connector(&zkplus.StaticConnector{C: z, Ch: ch}).Build()
		return zkp, zkp.EventChan(), err
	})

	c := 0
	z.ForcedErrorCheck(func(s string) error {
		c++
		if c < 2 {
			return nil
		}
		return zk.ErrNoNode
	})

	d1, _ := New(zkConnFunc, "TestAdvertise1")

	require.Equal(t, zk.ErrNoNode, d1.Advertise("TestAdvertiseService", "", (uint16)(1234)))
}

func TestBadRefresh(t *testing.T) {
	s2 := zktest.New()
	z, ch, _ := s2.Connect()

	badForce := errors.New("nope")
	c := 0
	z.ForcedErrorCheck(func(s string) error {
		if s == "childrenw" {
			c++
		}
		if c > 2 && s == "childrenw" {
			return badForce
		}
		return nil
	})

	zkConnFunc := ZkConnCreatorFunc(func() (ZkConn, <-chan zk.Event, error) {
		zkp, err := zkplus.NewBuilder().PathPrefix("/test").Connector(&zkplus.StaticConnector{C: z, Ch: ch}).Build()
		return zkp, zkp.EventChan(), err
	})

	d1, _ := New(zkConnFunc, "TestAdvertise1")

	require.NoError(t, d1.Advertise("TestAdvertiseService", "", (uint16)(1234)))
	s, err := d1.Services("TestAdvertiseService")
	require.NoError(t, err)

	d1.manualEvents <- zk.Event{
		Path: "/TestAdvertiseService",
	}
	time.Sleep(time.Millisecond)
	require.Equal(t, badForce, s.refresh(z))

	z.ForcedErrorCheck(nil)
	s2.ForcedErrorCheck(func(s string) error {
		return zk.ErrNoNode
	})
	d1.manualEvents <- zk.Event{
		Path: "/TestAdvertiseService",
	}
	require.NoError(t, s.refresh(z))
	require.Equal(t, 0, len(s.ServiceInstances()))

	z.ForcedErrorCheck(nil)
	s2.ForcedErrorCheck(func(s string) error {
		return badForce
	})

	// Verifying that .Services() doesn't cache a bad result
	delete(d1.watchedServices, "TestAdvertiseService2")
	s, err = d1.Services("TestAdvertiseService2")
	require.Error(t, err)
	_, exists := d1.watchedServices["TestAdvertiseService2"]
	require.False(t, exists)
}

func TestBadRefresh2(t *testing.T) {
	s2 := zktest.New()
	z, ch, _ := s2.Connect()
	badForce := errors.New("nope")
	c := 0
	z.ForcedErrorCheck(func(s string) error {
		if s == "getw" {
			c++
			if c > 1 {
				return badForce
			}
			return nil
		}
		return nil
	})

	zkp, err := zkplus.NewBuilder().PathPrefix("/test").Connector(&zkplus.StaticConnector{C: z, Ch: ch}).Build()
	zkConnFunc := ZkConnCreatorFunc(func() (ZkConn, <-chan zk.Event, error) {
		return zkp, zkp.EventChan(), err
	})

	d1, _ := New(zkConnFunc, "TestAdvertise1")

	require.NoError(t, d1.Advertise("TestAdvertiseService", "", (uint16)(1234)))
	s, err := d1.Services("TestAdvertiseService")
	require.NoError(t, err)

	d1.manualEvents <- zk.Event{
		Path: "/TestAdvertiseService",
	}
	require.Equal(t, badForce, s.refresh(zkp))
}

func TestInvalidServiceJson(t *testing.T) {
	s := zktest.New()
	z, ch, _ := s.Connect()
	var zkp *zkplus.ZkPlus
	zkConnFunc := ZkConnCreatorFunc(func() (ZkConn, <-chan zk.Event, error) {
		var err error
		zkp, err = zkplus.NewBuilder().PathPrefix("/test").Connector(&zkplus.StaticConnector{C: z, Ch: ch}).Build()
		return zkp, zkp.EventChan(), err
	})

	d1, _ := New(zkConnFunc, "TestInvalidServiceJson")
	// Give root paths time to create
	zkp.Exists("/")
	exists, _, err := z.Exists("/test")
	require.NoError(t, err)
	require.True(t, exists)
	_, err = z.Create("/test/badservice", []byte(""), 0, zk.WorldACL(zk.PermAll))
	require.NoError(t, err)
	_, err = z.Create("/test/badservice/badjson", []byte("badjson"), 0, zk.WorldACL(zk.PermAll))
	require.NoError(t, err)

	_, err = d1.Services("badservice")
	require.Error(t, err)

}

func TestAdvertise(t *testing.T) {
	s2 := zktest.New()
	z, ch, _ := s2.Connect()
	z2, ch2, _ := s2.Connect()
	zkConnFunc := ZkConnCreatorFunc(func() (ZkConn, <-chan zk.Event, error) {
		zkp, err := zkplus.NewBuilder().PathPrefix("/test").Connector(&zkplus.StaticConnector{C: z, Ch: ch}).Build()
		return zkp, zkp.EventChan(), err
	})
	zkConnFunc2 := ZkConnCreatorFunc(func() (ZkConn, <-chan zk.Event, error) {
		zkp, err := zkplus.NewBuilder().PathPrefix("/test").Connector(&zkplus.StaticConnector{C: z2, Ch: ch2}).Build()
		return zkp, zkp.EventChan(), err
	})
	testAdvertise(t, zkConnFunc, zkConnFunc2)
}

func testAdvertise(t *testing.T, zkConnFunc ZkConnCreatorFunc, zkConnFunc2 ZkConnCreatorFunc) {
	d1, err := New(zkConnFunc, "TestAdvertise1")

	require.NoError(t, err)
	require.NotNil(t, d1)
	defer d1.Close()

	fmt.Printf("Getting service\n")
	service, err := d1.Services("TestAdvertiseService")
	require.NoError(t, err)
	require.Equal(t, 0, len(service.ServiceInstances()))
	seen := make(chan struct{}, 5)
	service.Watch(ChangeWatch(func() {
		fmt.Printf("Event seen!\n")
		seen <- struct{}{}
	}))

	fmt.Printf("d1 Advertise!\n")
	require.NoError(t, d1.Advertise("TestAdvertiseService", "", (uint16)(1234)))
	require.Equal(t, "/TestAdvertiseService/"+d1.GUID(), d1.servicePath("TestAdvertiseService"))
	<-seen

	require.NoError(t, err)
	require.Equal(t, 1, len(service.ServiceInstances()))
	require.Equal(t, "TestAdvertise1:1234", service.ServiceInstances()[0].DialString())

	serviceRepeat, err := d1.Services("TestAdvertiseService")
	require.NoError(t, err)
	require.Exactly(t, serviceRepeat, service)

	d2, err := New(zkConnFunc2, "TestAdvertise2")
	require.NoError(t, err)
	require.NotNil(t, d2)
	defer d2.Close()

	fmt.Printf("About to advertise\n")
	require.NoError(t, d2.Advertise("TestAdvertiseService", "", (uint16)(1234)))
	fmt.Printf("Advertise done\n")

	<-seen

	require.Equal(t, 2, len(service.ServiceInstances()))
}

func TestServices(t *testing.T) {
	zkServer := zktest.New()
	z, ch, _ := zkServer.Connect()
	z2, _, _ := zkServer.Connect()
	testServices(t, z, ch, z2)
}

func testServices(t *testing.T, z1 zktest.ZkConnSupported, ch <-chan zk.Event, z2 zktest.ZkConnSupported) {
	zkConnFunc := ZkConnCreatorFunc(func() (ZkConn, <-chan zk.Event, error) {
		return z1, ch, nil
	})
	d1, err := New(zkConnFunc, "TestAdvertise1")
	require.NoError(t, err)

	fmt.Printf("Ensure delete\n")
	zktest.EnsureDelete(z2, "/not_here")
	defer func() {
		go zktest.EnsureDelete(z2, "/not_here")
	}()

	fmt.Printf("Services\n")
	s, err := d1.Services("not_here")
	require.NoError(t, err)
	require.Equal(t, 0, len(s.ServiceInstances()))

	onWatchChan := make(chan struct{})
	s.Watch(func() {
		log.Info("Event seen!")
		onWatchChan <- struct{}{}
	})

	fmt.Printf("Before create!\n")
	p1, err := z2.Create("/not_here", []byte(""), 0, zk.WorldACL(zk.PermAll))
	fmt.Printf("After create: %s\t%s\n", p1, err)
	require.NoError(t, err)
	fmt.Printf("After create!\n")

	_, err = z2.Create("/not_here/s1", []byte("{}"), 0, zk.WorldACL(zk.PermAll))
	require.NoError(t, err)
	<-onWatchChan

	require.Equal(t, 1, len(s.ServiceInstances()))
}
