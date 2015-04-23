// +build integration

package distconf

import (
	"testing"

	"time"

	"fmt"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/signalfx/golib/zkplus"
	"github.com/stretchr/testify/assert"
)

var zkTestHost string
var zkTestPrefix string

func init() {
	C := FromLoaders([]BackingLoader{EnvLoader(), IniLoader("../.integration_test_config.ini")})
	zkTestHost = C.Str("zk.test_host", "").Get()
	zkTestPrefix = C.Str("zk.testprefix", "").Get()
	if zkTestHost == "" || zkTestPrefix == "" {
		panic("Please set zk.test_host|zk.test_host in env or .integration_test_config.ini")
	}
}

func TestZkConfIT(t *testing.T) {
	zkPlusBuilder := zkplus.NewBuilder().DialZkConnector([]string{zkTestHost}, time.Second*30, nil).AppendPathPrefix(zkTestPrefix).AppendPathPrefix("config")

	fmt.Printf("Zk()\n")
	z, err := Zk(ZkConnectorFunc(func() (ZkConn, <-chan zk.Event, error) {
		return zkPlusBuilder.BuildDirect()
	}))
	assert.NoError(t, err)
	defer z.Close()
	fmt.Printf("z.Write\n")
	assert.NoError(t, z.Write("TestZkConf", nil))
	config := FromLoaders([]BackingLoader{BackingLoaderFunc(func() (Reader, error) {
		return z, nil
	})})
	fmt.Printf("config.Str\n")
	s := config.Str("TestZkConf", "__DEFAULT__")
	assert.Equal(t, "__DEFAULT__", s.Get())

	fmt.Printf("z.Write\n")
	err = z.Write("TestZkConf", []byte("Hello world6"))
	fmt.Printf("Adding done\n")
	assert.NoError(t, err)
	time.Sleep(time.Second)
	assert.Equal(t, "Hello world6", s.Get())
}
