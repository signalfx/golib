// +build integration

package disco

import (
	"testing"

	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/signalfx/golib/distconf"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/zkplus"
	"github.com/stretchr/testify/assert"
)

var zkTestHost string
var zkTestPrefix string
var zkTestService string

func init() {
	C := distconf.FromLoaders([]distconf.BackingLoader{distconf.EnvLoader(), distconf.IniLoader("../.integration_test_config.ini")})
	zkTestHost = C.Str("zk.test_host", "").Get()
	zkTestPrefix = C.Str("zk.testprefix", "").Get()
	zkTestService = C.Str("zk.test_service", "").Get()
	if zkTestHost == "" || zkTestPrefix == "" || zkTestService == "" {
		panic("Please set zk.test_host|zk.testprefix|zk.test_service in env or .integration_test_config.ini")
	}
}

func TestDupAdvertiseIT(t *testing.T) {
	z, ch, err := zk.Connect([]string{zkTestHost}, time.Second)
	assert.NoError(t, err)
	testDupAdvertise(t, z, ch)
}

func TestTestAdvertiseIT(t *testing.T) {
	z, ch, err := zk.Connect([]string{zkTestHost}, time.Second)
	assert.NoError(t, err)

	z2, ch2, _ := zk.Connect([]string{zkTestHost}, time.Second)

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

func TestDiscoConfIT(t *testing.T) {
	zkPlusRoot, err := zkplus.NewBuilder().DialZkConnector([]string{zkTestHost}, time.Second*30, nil).Build()
	assert.NoError(t, err)
	zkPlusChildBuilder := zkplus.NewBuilder().ZkPlus(zkPlusRoot).AppendPathPrefix(zkTestPrefix)
	zkPlusChild, err := zkPlusChildBuilder.Build()
	assert.NoError(t, err)

	// Clear out all the ephimeral nodes in our testing service directory
	children, _, _, err := zkPlusChild.ChildrenW("/TestDiscoConf")
	if err != nil {
		log.WithField("err", err).Info("Error getting children")
	} else {
		log.Info("Clearing children")
		for _, child := range children {
			log.WithField("Child", child).Info("Clearing out child")
			err := zkPlusChild.Delete("/TestDiscoConf/"+child, 0)
			if err != nil {
				log.WithField("err", err).Warn("Error clearing children!")
			}
		}
	}

	zkPlusRoot.Close()

	d1, err := New(ZkConnCreatorFunc(func() (ZkConn, <-chan zk.Event, error) {
		return zkPlusChildBuilder.BuildDirect()
	}), "t1")
	assert.NoError(t, err)
	defer d1.Close()

	s1, err := d1.Services("TestDiscoConf")
	x := make(chan struct{}, 3)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(s1.ServiceInstances()))

	s1.Watch(ChangeWatch(func() {
		x <- struct{}{}
	}))
	err = d1.Advertise("TestDiscoConf", "", uint16(1234))
	assert.NoError(t, err)
	<-x

	assert.Equal(t, 1, len(s1.ServiceInstances()))
}

func TestServicesIT(t *testing.T) {
	zkPlusRoot, err := zkplus.NewBuilder().PathPrefix("/test/TestServicesIT").DialZkConnector([]string{zkTestHost}, time.Second*30, nil).Build()
	assert.NoError(t, err)
	zkPlusRoot2, _ := zkplus.NewBuilder().PathPrefix("/test/TestServicesIT").DialZkConnector([]string{zkTestHost}, time.Second*30, nil).Build()
	testServices(t, zkPlusRoot, zkPlusRoot.EventChan(), zkPlusRoot2)
}

func TestZkDisconnectIT(t *testing.T) {
	log.Info("TestZkDisconnect")
	defer log.Info("Done for TestZkDisconnect")
	serviceName := "TestZkDisconnect"
	var dialer nettest.TrackingDialer
	defer dialer.Close()
	zkPlusBuilder := BuilderConnector(zkplus.NewBuilder().DialZkConnector([]string{zkTestHost}, time.Second*30, dialer.DialTimeout).AppendPathPrefix(zkTestPrefix))
	disco1, err := New(zkPlusBuilder, "addr1")
	assert.NoError(t, err)

	disco2, err := New(zkPlusBuilder, "addr2")
	assert.NoError(t, err)

	service, err := disco2.Services(serviceName)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(service.ServiceInstances()))
	foundOneInstance := make(chan struct{}, 1024)
	service.Watch(func() {
		instances := service.ServiceInstances()
		log.Infof("Found %d instances", len(instances))
		if len(instances) == 1 {
			foundOneInstance <- struct{}{}
		}
	})

	assert.NoError(t, disco1.Advertise(serviceName, struct{}{}, 1234))
	<-foundOneInstance

	assert.NoError(t, dialer.Close())
	time.Sleep(time.Second * 2)
	assert.Equal(t, 1, len(service.ServiceInstances()))
}
