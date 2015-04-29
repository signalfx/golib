package distconf

import (
	"errors"
	"fmt"

	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
)

// ZkConn does zookeeper connections
type ZkConn interface {
	Exists(path string) (bool, *zk.Stat, error)
	Get(path string) ([]byte, *zk.Stat, error)
	Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error)
	Set(path string, data []byte, version int32) (*zk.Stat, error)
	ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error)
	ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error)
	Delete(path string, version int32) error
	Close()
}

// ZkConnector creates zk connections for distconf
type ZkConnector interface {
	Connect() (ZkConn, <-chan zk.Event, error)
}

// ZkConnectorFunc wraps a dumb function to help you get a ZkConnector
type ZkConnectorFunc func() (ZkConn, <-chan zk.Event, error)

// Connect to Zk by calling itself()
func (z ZkConnectorFunc) Connect() (ZkConn, <-chan zk.Event, error) {
	return z()
}

type zkConfig struct {
	conn       ZkConn
	eventChan  <-chan zk.Event
	shouldQuit chan struct{}
	servers    []string

	callbackLock sync.Mutex
	callbacks    map[string][]backingCallbackFunction
}

func (back *zkConfig) configPath(key string) string {
	return fmt.Sprintf("%s", key)
}

// Get returns the config value from zookeeper
func (back *zkConfig) Get(key string) ([]byte, error) {
	bytes, _, err := back.conn.Get(back.configPath(key))
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, nil
		}
		return nil, err
	}
	return bytes, nil
}

func (back *zkConfig) Write(key string, value []byte) error {
	log.WithField("key", key).Info("Write")
	path := back.configPath(key)
	exists, stat, err := back.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exists {
		if value == nil {
			return nil
		}
		_, err := back.conn.Create(path, value, 0, zk.WorldACL(zk.PermAll))
		return err
	}
	if value == nil {
		err = back.conn.Delete(path, stat.Version)
	} else {
		stat, err = back.conn.Set(path, value, stat.Version)
	}

	return err
}

func (back *zkConfig) Watch(key string, callback backingCallbackFunction) error {
	log.WithField("key", key).Debug("Watch")
	path := back.configPath(key)
	_, _, _, err := back.conn.ExistsW(path)
	if err != nil {
		return err
	}
	// use the connection's global event chan for callbacks
	back.callbackLock.Lock()
	defer back.callbackLock.Unlock()
	back.callbacks[path] = append(back.callbacks[path], callback)

	return nil
}

func (back *zkConfig) Close() {
	close(back.shouldQuit)
	back.conn.Close()
}

var errInvalidPathPrefix = errors.New("invalid prefix path: Must being with /")
var errInvalidPathSuffix = errors.New("invalid prefix path: Must not end with /")

func (back *zkConfig) drainEventChan() {
	defer log.Info("Quitting ZK distconf event loop")
	for {
		log.Info("Blocking with event")
		select {
		case e := <-back.eventChan:
			log.WithField("event", e).Info("Event seen")
			if e.State == zk.StateDisconnected {
				log.WithField("event", e).Info("Disconnected from zookeeper.  Unable to read more updates.")
				return
			}
			if len(e.Path) > 0 && e.Path[0] == '/' {
				e.Path = e.Path[1:]
			}
			{
				back.callbackLock.Lock()
				log.WithField("event", e).WithField("len()", len(back.callbacks)).Info("Change state")
				for _, c := range back.callbacks[e.Path] {
					c(e.Path)
				}
				back.callbackLock.Unlock()
			}
			log.WithField("path", e.Path).Info("reregistering watch")

			// Note: return value currently ignored.  Not sure what to do about it
			back.reregisterWatch(e.Path)
			log.WithField("path", e.Path).Info("reregistering watch finished")
		case <-back.shouldQuit:
			return
		}
	}
}

func (back *zkConfig) reregisterWatch(path string) error {
	// Reregister watch
	_, _, _, err := back.conn.ExistsW(path)
	if err != nil {
		log.WithField("err", err).Info("Unable to reregister watch")
		return err
	}
	return nil
}

// Zk creates a zookeeper readable backing
func Zk(zkConnector ZkConnector) (ReaderWriter, error) {
	ret := &zkConfig{
		shouldQuit: make(chan struct{}),
		callbacks:  make(map[string][]backingCallbackFunction),
	}
	var err error
	ret.conn, ret.eventChan, err = zkConnector.Connect()
	if err != nil {
		return nil, err
	}
	go ret.drainEventChan()
	return ret, nil
}
