package zkplus

import (
	goerrors "errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/signalfx/golib/v3/errors"
	"github.com/signalfx/golib/v3/log"
	"github.com/signalfx/golib/v3/logkey"
	"github.com/signalfx/golib/v3/zkplus/zktest"
)

const (
	// DEBUG all the annoying zk message
	DEBUG = iota
	// NORMAL enough to be useful (he says)
	NORMAL
)

// LogLevel sets the log level of this module
// I'm sorry to have to put this here but i just can't deal with rewriting the entire
// log library or swapping it out for zap at the moment. sorry.
var LogLevel = NORMAL

// SetLogLevel sets the log level of the library.
func SetLogLevel(ll int) {
	logLevelRWLock.Lock()
	defer logLevelRWLock.Unlock()
	LogLevel = ll
}

var logLevelRWLock = sync.RWMutex{}

func normalit(logger log.Logger, keyvals ...interface{}) {
	logLevelRWLock.RLock()
	defer logLevelRWLock.RUnlock()
	if LogLevel <= NORMAL {
		logger.Log(keyvals...)
	}
}

func debugit(logger log.Logger, keyvals ...interface{}) {
	logLevelRWLock.RLock()
	defer logLevelRWLock.RUnlock()
	if LogLevel <= DEBUG {
		logger.Log(keyvals...)
	}
}

// ZkConnector tells ZkPlus how to create a zk connection
type ZkConnector interface {
	Conn() (zktest.ZkConnSupported, <-chan zk.Event, error)
}

// ZkConnectorFunc is a helper to wrap a simple function for making Zk connections
type ZkConnectorFunc func() (zktest.ZkConnSupported, <-chan zk.Event, error)

// Conn to a Zk Connection, calling itself to create the connection
func (f ZkConnectorFunc) Conn() (zktest.ZkConnSupported, <-chan zk.Event, error) {
	return f()
}

// A StaticConnector will always return the same connection and chan for every connection request.
// Usually only needed for testing
type StaticConnector struct {
	C  zktest.ZkConnSupported
	Ch <-chan zk.Event
}

// Conn will just return the constructed connection and event channel
func (s *StaticConnector) Conn() (zktest.ZkConnSupported, <-chan zk.Event, error) {
	return s.C, s.Ch, nil
}

// ZkPlus wraps a zookeeper connection to provide namespacing, auto reconnects, and server list changing
type ZkPlus struct {
	pathPrefix  string
	zkConnector ZkConnector
	logger      log.Logger
	createRoot  bool

	connectedConn zktest.ZkConnSupported
	connectedChan <-chan zk.Event
	shouldQuit    chan chan struct{}
	askForConn    chan chan zktest.ZkConnSupported

	exposedChan chan zk.Event
}

var (
	errInvalidPathPrefix = errors.New("invalid prefix path: Must being with /")
	errInvalidPathSuffix = errors.New("invalid prefix path: Must not end with /")
)

// EventChan that will see zookeeper events whose path is changed to this zk connection's
// namespace
func (z *ZkPlus) EventChan() <-chan zk.Event {
	return z.exposedChan
}

func whenI(cond bool, in <-chan zk.Event) <-chan zk.Event {
	if cond {
		return in
	}
	return nil
}

func whenO(cond bool, out chan zk.Event) chan zk.Event {
	if cond {
		return out
	}
	return nil
}

func whenAsk(cond bool, out chan chan zktest.ZkConnSupported) chan chan zktest.ZkConnSupported {
	if cond {
		return out
	}
	return nil
}

func whenTimer(cond bool, duration time.Duration) <-chan time.Time {
	if cond {
		return time.After(duration)
	}
	return nil
}

func (z *ZkPlus) ensureRootPath(conn zktest.ZkConnSupported) error {
	parts := strings.Split(z.pathPrefix, "/")
	totalPath := ""
	for _, p := range parts {
		if p == "" {
			continue
		}
		totalPath = totalPath + "/" + p

		exists, _, err := conn.Exists(totalPath)
		if err != nil {
			return errors.Annotatef(err, "cannot verify that node %q exists", totalPath)
		}

		if !exists {
			if z.createRoot {
				_, err := conn.Create(totalPath, []byte(""), 0, zk.WorldACL(zk.PermAll))
				// There could be a race where the root is created in the
				// meantime since the Exists check above, so ignore ErrNodeExists.
				if err != nil && !goerrors.Is(err, zk.ErrNodeExists) {
					return errors.Annotatef(err, "cannot create path %s", totalPath)
				}
			} else {
				return fmt.Errorf("root node %q does not exist", totalPath)
			}
		}
	}
	return nil
}

func (z *ZkPlus) eventLoop() {
	var haveEventToSend bool
	var eventToSend zk.Event
	delayForNewConn := time.Millisecond * 0
	for {
		select {
		case eventToSend = <-whenI(!haveEventToSend && z.connectedChan != nil, z.connectedChan):
			normalit(z.logger, logkey.ZkEvent, eventToSend, logkey.ZkPrefix, z.pathPrefix, log.Msg, "ZK node modification event")
			if strings.HasPrefix(eventToSend.Path, z.pathPrefix) {
				eventToSend.Path = eventToSend.Path[len(z.pathPrefix):]
				if eventToSend.Path == "" {
					eventToSend.Path = "/"
				}
			}
			haveEventToSend = true
			delayForNewConn = time.Millisecond * 0
		case whenO(haveEventToSend, z.exposedChan) <- eventToSend:
			haveEventToSend = false
		case c := <-whenAsk(z.connectedConn != nil, z.askForConn):
			c <- z.connectedConn
		case <-whenTimer(z.connectedConn == nil, delayForNewConn):
			delayForNewConn = time.Millisecond * 100
			z.setupConn()
		case c := <-z.shouldQuit:
			z.onQuit(c)
			return
		}
	}
}

func (z *ZkPlus) onQuit(c chan struct{}) {
	c <- struct{}{}
	if z.connectedConn != nil {
		z.connectedConn.Close()
		z.connectedConn = nil
	}
	normalit(z.logger, "Close on event loop")
}

func (z *ZkPlus) setupConn() {
	c, e, err := z.zkConnector.Conn()
	if err != nil {
		return
	}
	z.connectedConn = c
	z.connectedChan = e
	if err := z.ensureRootPath(c); err != nil {
		normalit(z.logger, "err", err, "Unable to ensure root path")
		z.connectedConn.Close()
		z.connectedConn = nil
		z.connectedChan = nil
	}
}

// Close this zk connection, blocking till the eventLoop() is finished.
func (z *ZkPlus) Close() {
	c := make(chan struct{})
	z.shouldQuit <- c
	<-c
	close(z.shouldQuit)
	close(z.exposedChan)
}

func (z *ZkPlus) realPath(path string) string {
	if len(path) == 0 || path[0] != '/' {
		path = "/" + path
	}
	finalPath := fmt.Sprintf("%s%s", z.pathPrefix, path)
	if finalPath != "/" && finalPath[len(finalPath)-1] == '/' {
		finalPath = finalPath[0 : len(finalPath)-1]
	}
	return finalPath
}

func (z *ZkPlus) blockOnConn() zktest.ZkConnSupported {
	c := make(chan zktest.ZkConnSupported)
	z.askForConn <- c
	r := <-c
	return r
}

// Exists returns true if the path exists
func (z *ZkPlus) Exists(path string) (bool, *zk.Stat, error) {
	debugit(z.forPath(path), logkey.ZkMethod, "Exists")
	return z.blockOnConn().Exists(z.realPath(path))
}

// ExistsW is like Exists but also sets a watch.  Note: We DO NOT change paths on the returned
// channel nor do we reconnect it.  Use the global channel instead
func (z *ZkPlus) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	debugit(z.forPath(path), logkey.ZkMethod, "ExistsW")
	return z.blockOnConn().ExistsW(z.realPath(path))
}

// Get the bytes of a zk path
func (z *ZkPlus) Get(path string) ([]byte, *zk.Stat, error) {
	debugit(z.forPath(path), logkey.ZkMethod, "Get")
	return z.blockOnConn().Get(z.realPath(path))
}

// GetW is like Get, but also sets a watch
func (z *ZkPlus) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	debugit(z.forPath(path), logkey.ZkMethod, "GetW")
	return z.blockOnConn().GetW(z.realPath(path))
}

// Children gets children of a path
func (z *ZkPlus) Children(path string) ([]string, *zk.Stat, error) {
	debugit(z.forPath(path), logkey.ZkMethod, "Children")
	return z.blockOnConn().Children(z.realPath(path))
}

// ChildrenW is like children but also sets a watch
func (z *ZkPlus) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	debugit(z.forPath(path), logkey.ZkMethod, "ChildrenW")
	return z.blockOnConn().ChildrenW(z.realPath(path))
}

// Delete a Zk node
func (z *ZkPlus) Delete(path string, version int32) error {
	normalit(z.forPath(path), logkey.ZkMethod, "Delete")
	return z.blockOnConn().Delete(z.realPath(path), version)
}

// Create a Zk node
func (z *ZkPlus) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	normalit(z.forPath(path), logkey.ZkMethod, "Create")
	p, err := z.blockOnConn().Create(z.realPath(path), data, flags, acl)
	if strings.HasPrefix(p, z.pathPrefix) && z.pathPrefix != "" {
		p = p[len(z.pathPrefix)+1:]
	}
	return p, errors.Annotatef(err, "cannot create zk path %s", path)
}

// Set the data of a zk node
func (z *ZkPlus) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	normalit(z.forPath(path), logkey.ZkMethod, "Set")
	return z.blockOnConn().Set(z.realPath(path), data, version)
}

func (z *ZkPlus) forPath(path string) log.Logger {
	return log.NewContext(z.logger).With(logkey.ZkPath, z.realPath(path))
}
