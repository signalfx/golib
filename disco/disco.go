package disco

import (
	"crypto/rand"
	"io"

	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"strings"
	"sync"

	"bytes"
	"encoding/binary"
	"sort"

	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/signalfx/golib/logherd"
	"github.com/signalfx/golib/zkplus"
)

var log *logrus.Logger

func init() {
	log = logherd.New()
}

// ServiceInstance defines a remote service and is similar to
// https://curator.apache.org/apidocs/org/apache/curator/x/discovery/ServiceInstanceBuilder.html
type ServiceInstance struct {
	ID                  string      `json:"id"`
	Name                string      `json:"name"`
	Payload             interface{} `json:"payload,omitempty"`
	Address             string      `json:"address"`
	Port                uint16      `json:"port"`
	RegistrationTimeUTC int64       `json:"registrationTimeUTC"`
	SslPort             *uint16     `json:"sslPort"`
	ServiceType         string      `json:"serviceType"`
	URISpec             *string     `json:"uriSpec"`
}

func (s *ServiceInstance) uniqueHash() []byte {
	buf := new(bytes.Buffer)
	buf.Write([]byte(s.ID))
	binary.Write(buf, binary.BigEndian, s.RegistrationTimeUTC)
	return buf.Bytes()
}

// DialString is a string that net.Dial() can accept that will connect to this service's Port
func (s *ServiceInstance) DialString() string {
	return fmt.Sprintf("%s:%d", s.Address, s.Port)
}

// ChangeWatch is a callback you can register on a service that is executed whenever the service's
// instances change
type ChangeWatch func()

// Service is a set of ServiceInstance that describe a discovered service
type Service struct {
	services atomic.Value // []ServiceInstance
	name     string

	watchLock sync.Mutex
	watches   []ChangeWatch
}

// ZkConn does zookeeper connections
type ZkConn interface {
	GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error)
	Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error)
	ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error)
	ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error)
	Close()
}

// ZkConnCreator creates Zk connections for disco to use.
type ZkConnCreator interface {
	Connect() (ZkConn, <-chan zk.Event, error)
}

// ZkConnCreatorFunc gives you a ZkConnCreator out of a function
type ZkConnCreatorFunc func() (ZkConn, <-chan zk.Event, error)

// Connect to a zookeeper endpoint
func (z ZkConnCreatorFunc) Connect() (ZkConn, <-chan zk.Event, error) {
	return z()
}

// Disco is a service discovery framework orchestrated via zookeeper
type Disco struct {
	zkConnCreator ZkConnCreator

	zkConn               ZkConn
	eventChan            <-chan zk.Event
	GUIDbytes            [16]byte
	publishAddress       string
	myAdvertisedServices map[string]struct{}
	shouldQuit           chan struct{}
	eventLoopDone        chan struct{}

	watchedMutex    sync.Mutex
	watchedServices map[string]*Service

	manualEvents chan zk.Event

	jsonMarshal func(v interface{}) ([]byte, error)
}

// BuilderConnector satisfies the disco zk connect interface for a zkplus.Builder
func BuilderConnector(b *zkplus.Builder) ZkConnCreator {
	return ZkConnCreatorFunc(func() (ZkConn, <-chan zk.Event, error) {
		return b.BuildDirect()
	})
}

// New creates a disco discovery/publishing service
func New(zkConnCreator ZkConnCreator, publishAddress string) (*Disco, error) {
	return NewRandSource(zkConnCreator, publishAddress, rand.Reader)
}

// NewRandSource creates a disco discovery/publishing service using r as the GUID enthropy
func NewRandSource(zkConnCreator ZkConnCreator, publishAddress string, r io.Reader) (*Disco, error) {
	var GUID [16]byte
	_, err := io.ReadFull(r, GUID[:16])
	if err != nil {
		return nil, err
	}

	d := &Disco{
		zkConnCreator:        zkConnCreator,
		myAdvertisedServices: make(map[string]struct{}),
		GUIDbytes:            GUID,
		publishAddress:       publishAddress,
		shouldQuit:           make(chan struct{}),
		eventLoopDone:        make(chan struct{}),
		watchedServices:      make(map[string]*Service),
		manualEvents:         make(chan zk.Event),
	}
	d.zkConn, d.eventChan, err = zkConnCreator.Connect()
	if err != nil {
		return nil, err
	}
	go d.eventLoop()
	return d, nil
}

func isServiceModificationEvent(eventType zk.EventType) bool {
	return eventType == zk.EventNodeDataChanged || eventType == zk.EventNodeDeleted || eventType == zk.EventNodeCreated || eventType == zk.EventNodeChildrenChanged
}

func (d *Disco) eventLoop() {
	defer func() {
		close(d.eventLoopDone)
	}()
	for {
		select {
		case <-d.shouldQuit:
			return
		case e := <-d.manualEvents:
			d.processZkEvent(&e)
		case e := <-d.eventChan:
			d.processZkEvent(&e)
		}
	}
}

var errServiceDoesNotExist = errors.New("could not find service to refresh")

func (d *Disco) processZkEvent(e *zk.Event) {
	log.WithField("event", e).Info("Disco event seen")
	serviceName := ""
	if isServiceModificationEvent(e.Type) {
		// serviceName is in () /(___)/___
		serviceName = e.Path
		if serviceName[0] == '/' {
			serviceName = serviceName[1:]
		}
		parts := strings.SplitN(serviceName, "/", 2)
		serviceName = parts[0]
	}
	if serviceName != "" {
		log.WithField("service", serviceName).Info("Refresh on service")
		service, err := func() (*Service, error) {
			d.watchedMutex.Lock()
			defer d.watchedMutex.Unlock()
			s, exist := d.watchedServices[serviceName]
			if exist {
				return s, nil
			}
			return nil, errServiceDoesNotExist
		}()
		if err != nil {
			log.WithField("event", e).WithField("parent", serviceName).WithField("err", err).Warn("Unable to find parent")
		} else {
			log.WithField("service", service).Info("refreshing")
			service.refresh(d.zkConn)
		}
	}
}

// Close any open disco connections making this disco unreliable for future updates
func (d *Disco) Close() {
	close(d.shouldQuit)
	<-d.eventLoopDone
	d.zkConn.Close()
}

func (d *Disco) servicePath(serviceName string) string {
	return fmt.Sprintf("/%s/%s", serviceName, d.GUID())
}

// GUID that this disco advertises itself as
func (d *Disco) GUID() string {
	return fmt.Sprintf("%x", d.GUIDbytes)
}

func (d *Disco) myServiceData(serviceName string, payload interface{}, port uint16) ServiceInstance {
	return ServiceInstance{
		ID:                  d.GUID(),
		Name:                serviceName,
		Payload:             payload,
		Address:             d.publishAddress,
		Port:                port,
		RegistrationTimeUTC: time.Now().UnixNano() / int64(time.Millisecond),
		SslPort:             nil,
		ServiceType:         "DYNAMIC",
		URISpec:             nil,
	}
}

// Advertise yourself as hosting a service
func (d *Disco) Advertise(serviceName string, payload interface{}, port uint16) error {
	log.WithField("name", serviceName).Info("Advertising myself on a service")
	service := d.myServiceData(serviceName, payload, port)
	jsonMarshal := json.Marshal
	if d.jsonMarshal != nil {
		jsonMarshal = d.jsonMarshal
	}
	bytes, err := jsonMarshal(service)
	if err != nil {
		return err
	}
	d.myAdvertisedServices[serviceName] = struct{}{}
	_, err = d.zkConn.Create(d.servicePath(serviceName), bytes, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNoNode {
		// Need to create service root node
		_, err = d.zkConn.Create(fmt.Sprintf("/%s", serviceName), []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
		// Retry create now that we have parent node
		_, err = d.zkConn.Create(d.servicePath(serviceName), bytes, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	}

	d.manualEvents <- zk.Event{
		Type: zk.EventNodeChildrenChanged,
		Path: serviceName,
	}

	return err
}

// Services advertising for serviceName
func (d *Disco) Services(serviceName string) (*Service, error) {
	d.watchedMutex.Lock()
	defer d.watchedMutex.Unlock()
	s, exist := d.watchedServices[serviceName]
	if exist {
		return s, nil
	}
	ret := &Service{
		name: serviceName,
	}
	ret.services.Store([]ServiceInstance{})
	refreshRes := ret.refresh(d.zkConn)
	if refreshRes == nil {
		d.watchedServices[serviceName] = ret
		return ret, nil
	}
	return nil, refreshRes
}

// ServiceInstances that represent instances of this service in your system
func (s *Service) ServiceInstances() []ServiceInstance {
	return s.services.Load().([]ServiceInstance)
}

// Watch for changes to the members of this service
func (s *Service) Watch(watch ChangeWatch) {
	s.watchLock.Lock()
	defer s.watchLock.Unlock()
	s.watches = append(s.watches, watch)
}

func (s *Service) String() string {
	s.watchLock.Lock()
	defer s.watchLock.Unlock()
	return fmt.Sprintf("name=%s|len(watch)=%d|instances=%v", s.name, len(s.watches), s.services.Load())
}

func (s *Service) byteHashes() string {
	all := []string{}
	for _, i := range s.ServiceInstances() {
		all = append(all, string(i.uniqueHash()))
	}
	slice := sort.StringSlice(all)
	slice.Sort()
	r := ""
	for _, s := range all {
		r += s
	}
	return r
}

func childrenServices(serviceName string, children []string, zkConn ZkConn) ([]ServiceInstance, error) {
	log.WithField("serviceName", serviceName).Info("Getting services")
	ret := make([]ServiceInstance, len(children))

	var err error
	var wg sync.WaitGroup
	for index, child := range children {
		wg.Add(1)
		go func(child string, instanceAddr *ServiceInstance) {
			defer wg.Done()
			var bytes []byte
			var err2 error
			bytes, _, _, err2 = zkConn.GetW(fmt.Sprintf("/%s/%s", serviceName, child))
			if err2 != nil {
				err = err2
				return
			}
			err2 = json.Unmarshal(bytes, instanceAddr)
			if err2 != nil {
				err = err2
				return
			}
		}(child, &ret[index]) // <--- Important b/c inside range
	}
	wg.Wait()
	return ret, err
}

func (s *Service) refresh(zkConn ZkConn) error {
	log.WithField("service", s.name).Info("refresh called")
	oldHash := s.byteHashes()
	children, _, _, err := zkConn.ChildrenW(fmt.Sprintf("/%s", s.name))
	if err != nil && err != zk.ErrNoNode {
		log.Warn("Error?")
		return err
	}

	if err == zk.ErrNoNode {
		exists, _, _, err := zkConn.ExistsW(fmt.Sprintf("/%s", s.name))
		if exists || err != nil {
			log.WithField("err", err).Warn("Unable to register exists watch!")
		}
		s.services.Store(make([]ServiceInstance, 0))
	} else {
		services, err := childrenServices(s.name, children, zkConn)
		if err != nil {
			return err
		}
		s.services.Store(services)
	}
	s.watchLock.Lock()
	defer s.watchLock.Unlock()
	newHash := s.byteHashes()
	if oldHash != newHash {
		log.Info("Calling watches")
		for _, w := range s.watches {
			w()
		}
	}
	return nil
}
