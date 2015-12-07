package distconf

import (
	"fmt"

	"sync"

	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/signalfx/golib/errors"
)

// ZkConn does zookeeper connections
type ZkConn interface {
	Exists(path string) (bool, *zk.Stat, error)
	Get(path string) ([]byte, *zk.Stat, error)
	GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error)
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

type callbackMap struct {
	mu        sync.Mutex
	callbacks map[string][]backingCallbackFunction
}

func (c *callbackMap) add(key string, val backingCallbackFunction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callbacks[key] = append(c.callbacks[key], val)
}

func (c *callbackMap) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.callbacks)
}

func (c *callbackMap) get(key string) []backingCallbackFunction {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, exists := c.callbacks[key]
	if exists {
		return r
	}
	return []backingCallbackFunction{}
}

func (c *callbackMap) copy() map[string][]backingCallbackFunction {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := make(map[string][]backingCallbackFunction, len(c.callbacks))
	for k, v := range c.callbacks {
		ret[k] = []backingCallbackFunction{}
		for _, c := range v {
			ret[k] = append(ret[k], c)
		}
	}
	return ret
}

type atomicDuration struct {
	mu                sync.Mutex
	refreshRetryDelay time.Duration
}

func (a *atomicDuration) set(t time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refreshRetryDelay = t
}

func (a *atomicDuration) get() time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.refreshRetryDelay
}

type zkConfig struct {
	conn       ZkConn
	eventChan  <-chan zk.Event
	shouldQuit chan struct{}

	callbacks    callbackMap
	refreshDelay atomicDuration
}

func (back *zkConfig) configPath(key string) string {
	return fmt.Sprintf("%s", key)
}

// Get returns the config value from zookeeper
func (back *zkConfig) Get(key string) ([]byte, error) {
	pathToFetch := back.configPath(key)
	bytes, _, _, err := back.conn.GetW(pathToFetch)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, nil
		}
		return nil, errors.Annotatef(err, "cannot load zk node %s", pathToFetch)
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
		return errors.Annotatef(err, "cannot create path %s", path)
	}
	if value == nil {
		err = back.conn.Delete(path, stat.Version)
	} else {
		stat, err = back.conn.Set(path, value, stat.Version)
	}

	return errors.Annotatef(err, "cannot change path %s", path)
}

func (back *zkConfig) Watch(key string, callback backingCallbackFunction) error {
	log.WithField("key", key).Debug("Watch")
	path := back.configPath(key)
	_, _, _, err := back.conn.ExistsW(path)
	if err != nil {
		return errors.Annotatef(err, "cannot watch path %s", path)
	}
	back.callbacks.add(path, callback)

	return nil
}

func (back *zkConfig) Close() {
	close(back.shouldQuit)
	back.conn.Close()
}

func (back *zkConfig) logInfoState(e zk.Event) bool {
	if e.State == zk.StateDisconnected {
		log.WithField("event", e).Info("Disconnected from zookeeper.  Will attempt to remake connection.")
		return true
	}
	if e.State == zk.StateConnecting {
		log.WithField("event", e).Info("Server is now attempting to reconnect.")
		return true
	}
	return false
}

func (back *zkConfig) drainEventChan() {
	defer log.Info("Quitting ZK distconf event loop")
	for {
		log.Info("Blocking with event")
		select {
		case e := <-back.eventChan:
			log.WithField("event", e).Info("Event seen")
			back.logInfoState(e)
			if e.State == zk.StateHasSession {
				log.WithField("event", e).Info("Server now has a session.")
				back.refreshWatches()
				continue
			}
			if e.Path == "" {
				continue
			}
			if len(e.Path) > 0 && e.Path[0] == '/' {
				e.Path = e.Path[1:]
			}
			{
				log.WithField("event", e).WithField("len()", back.callbacks.len()).Info("Change state")
				for _, c := range back.callbacks.get(e.Path) {
					c(e.Path)
				}
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

func (back *zkConfig) refreshWatches() {
	for path, callbacks := range back.callbacks.copy() {
		for _, c := range callbacks {
			c(path)
		}
		for {
			err := back.reregisterWatch(path)
			if err == nil {
				break
			}
			log.Warnf("Error reregistering watch: %s  Will sleep and try again", err)
			time.Sleep(back.refreshDelay.get())
		}
	}
}

func (back *zkConfig) setRefreshDelay(refreshDelay time.Duration) {
	back.refreshDelay.set(refreshDelay)
}

func (back *zkConfig) reregisterWatch(path string) error {
	log.Infof("Reregistering watch for %s", path)
	_, _, _, err := back.conn.ExistsW(path)
	if err != nil {
		log.WithField("err", err).Info("Unable to reregister watch")
		return errors.Annotatef(err, "unable to reregister watch for node %s", path)
	}
	return nil
}

// Zk creates a zookeeper readable backing
func Zk(zkConnector ZkConnector) (ReaderWriter, error) {
	ret := &zkConfig{
		shouldQuit: make(chan struct{}),
		callbacks: callbackMap{
			callbacks: make(map[string][]backingCallbackFunction),
		},
		refreshDelay: atomicDuration{
			refreshRetryDelay: time.Millisecond * 500,
		},
	}
	var err error
	ret.conn, ret.eventChan, err = zkConnector.Connect()
	if err != nil {
		return nil, errors.Annotate(err, "cannot create zk connection")
	}
	go ret.drainEventChan()
	return ret, nil
}
