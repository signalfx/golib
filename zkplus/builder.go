package zkplus

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/signalfx/golib/zkplus/zktest"
)

// Builder helps users build a ZkPlus connection
type Builder struct {
	pathPrefix  string
	zkConnector ZkConnector
}

// NewBuilder creates a new builder for making ZkPlus connections
func NewBuilder() *Builder {
	return &Builder{
		pathPrefix: "",
	}
}

// DialZkConnector sets how zk network connections are created
func (b *Builder) DialZkConnector(servers []string, sessionTimeout time.Duration, dialer zk.Dialer) *Builder {
	b.zkConnector = ZkConnectorFunc(func() (zktest.ZkConnSupported, <-chan zk.Event, error) {
		if dialer != nil {
			return zk.Connect(servers, sessionTimeout, zk.WithDialer(dialer))
		}
		return zk.Connect(servers, sessionTimeout)
	})
	return b
}

// Connector sets what we use to create zk connections
func (b *Builder) Connector(zkConnector ZkConnector) *Builder {
	b.zkConnector = zkConnector
	return b
}

// PathPrefix is the prefix any zk operations get
func (b *Builder) PathPrefix(pathPrefix string) *Builder {
	b.pathPrefix = pathPrefix
	return b
}

// ZkPlus copies the config from another connection
func (b *Builder) ZkPlus(zkPlus *ZkPlus) *Builder {
	b.pathPrefix = zkPlus.pathPrefix
	b.zkConnector = zkPlus.zkConnector
	return b
}

// AppendPathPrefix to the existing path.  Can be chained to create /a/b/c directories
func (b *Builder) AppendPathPrefix(childPath string) *Builder {
	b.pathPrefix = fmt.Sprintf("%s/%s", b.pathPrefix, childPath)
	return b
}

// BuildDirect is a helper that looks like the regular zk create function
func (b *Builder) BuildDirect() (*ZkPlus, <-chan zk.Event, error) {
	z, err := b.Build()
	if err != nil {
		return nil, nil, err
	}
	return z, z.EventChan(), nil
}

// Build a ZkPlus connection, returning an error if the path doesn't make sense
func (b *Builder) Build() (*ZkPlus, error) {
	prefix := b.pathPrefix
	if len(prefix) == 0 {
		prefix = ""
	} else if prefix[0] != '/' {
		return nil, errInvalidPathPrefix
	} else if prefix[len(prefix)-1] == '/' {
		return nil, errInvalidPathSuffix
	}
	// Prefix is of the form /..... (then no ending /) or empty string
	log.WithField("prefix", prefix).Info("New with prefix")

	ret := &ZkPlus{
		pathPrefix:  prefix,
		zkConnector: b.zkConnector,
		exposedChan: make(chan zk.Event),
		shouldQuit:  make(chan chan struct{}),
		askForConn:  make(chan chan zktest.ZkConnSupported),
	}
	go ret.eventLoop()
	return ret, nil
}
