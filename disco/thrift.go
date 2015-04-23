package disco

import (
	"math/rand"
	"net"

	"time"

	"errors"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/signalfx/golib/logherd"
)

// ThriftTransport can be used as the transport layer for thrift, connecting to services discovered
// by disco
type ThriftTransport struct {
	currentTransport thrift.TTransport
	WrappedFactory   thrift.TTransportFactory
	service          *Service
	randSource       *rand.Rand
	Dialer           net.Dialer
}

var _ thrift.TTransport = &ThriftTransport{}

// NewThriftTransport creates a new ThriftTransport.  The default transport factory is TFramed
func NewThriftTransport(service *Service, timeout time.Duration) *ThriftTransport {
	return &ThriftTransport{
		WrappedFactory: thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()),
		service:        service,
		randSource:     rand.New(rand.NewSource(time.Now().UnixNano())),
		Dialer: net.Dialer{
			Timeout: timeout,
		},
	}
}

// Flush the underline transport
func (d *ThriftTransport) Flush() (err error) {
	if d.currentTransport == nil {
		return nil
	}
	err = d.currentTransport.Flush()
	if err != nil {
		d.currentTransport.Close()
		d.currentTransport = nil
	}
	return err
}

// IsOpen will return true if there is a connected underline transport and it is open
func (d *ThriftTransport) IsOpen() bool {
	return d.currentTransport != nil && d.currentTransport.IsOpen()
}

// Close and nil the underline transport
func (d *ThriftTransport) Close() error {
	if d.currentTransport == nil {
		return nil
	}
	ret := d.currentTransport.Close()
	d.currentTransport = nil
	return ret
}

// Read bytes from underline transport if it is not nil.  Exact definition defined in TTransport
func (d *ThriftTransport) Read(b []byte) (n int, err error) {
	if d.currentTransport == nil {
		return 0, thrift.NewTTransportException(thrift.NOT_OPEN, "")
	}
	n, err = d.currentTransport.Read(b)
	if err != nil {
		d.Close()
	}
	return
}

// Write bytes to underline transport if it is not nil.  Exact definition defined in TTransport
func (d *ThriftTransport) Write(b []byte) (n int, err error) {
	if d.currentTransport == nil {
		return 0, thrift.NewTTransportException(thrift.NOT_OPEN, "")
	}
	n, err = d.currentTransport.Write(b)
	if err != nil {
		d.Close()
	}
	return
}

// ErrNoInstance is returned by NextConnection if the service has no instances
var ErrNoInstance = errors.New("no thrift instances in disco")

// ErrNoInstanceOpen is returned by NextConnection if it cannot connect to any service ports
var ErrNoInstanceOpen = errors.New("no thrift instances is open")

// NextConnection will connect this transport to another random disco service, or return an error
// if no disco service can be dialed
func (d *ThriftTransport) NextConnection() error {
	if d.currentTransport != nil && d.currentTransport.IsOpen() {
		d.currentTransport.Close()
	}
	instances := d.service.ServiceInstances()
	if len(instances) == 0 {
		return ErrNoInstance
	}
	logherd.Debug(log, "instances", instances, "Getting next connection")
	startIndex := d.randSource.Intn(len(instances))
	for i := 0; i < len(instances); i++ {
		instance := &instances[(startIndex+i)%len(instances)]
		logherd.Debug(log, "instance", instance, "Looking at instance")
		conn, err := d.Dialer.Dial("tcp", instance.DialString())
		if err != nil {
			log.WithField("err", err).Info("Unable to dial instance")
			continue
		}
		d.currentTransport = d.WrappedFactory.GetTransport(thrift.NewTSocketFromConnTimeout(conn, d.Dialer.Timeout))
		return nil
	}
	d.currentTransport = nil
	return ErrNoInstanceOpen
}

// Open a connection if one does not exist, otherwise do nothing.
func (d *ThriftTransport) Open() error {
	if d.currentTransport == nil || !d.currentTransport.IsOpen() {
		return d.NextConnection()
	}
	return nil
}
