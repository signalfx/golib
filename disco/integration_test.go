// +build integration

package disco

import (
	"testing"

	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/signalfx/golib/distconf"
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
	zkPlusRoot2, err := zkplus.NewBuilder().PathPrefix("/test/TestServicesIT").DialZkConnector([]string{zkTestHost}, time.Second*30, nil).Build()
	testServices(t, zkPlusRoot, zkPlusRoot.EventChan(), zkPlusRoot2)
}
