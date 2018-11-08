/*
Copyright 2011 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"

	"encoding/binary"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	headerSize = 24

	// magic
	reqMagic = byte(0x80)
	resMagic = byte(0x81)

	// ops
	opGet     = byte(0x00)
	opSet     = byte(0x01)
	opAdd     = byte(0x02)
	opReplace = byte(0x03)

	// statuses
	statusSuccess        = uint16(0x00)
	statusKeyEnoent      = uint16(0x01)
	statusKeyExists      = uint16(0x02)
	statusE2Big          = uint16(0x03)
	statusEinval         = uint16(0x04)
	statusNotStored      = uint16(0x05)
	statusDeltaBadVal    = uint16(0x06)
	statusNotMyVBucket   = uint16(0x07)
	statusUnknownCommand = uint16(0x81)
	statusEnomem         = uint16(0x82)
	statusTmpFail        = uint16(0x86)
)

var statusNames map[uint16]string

func init() {
	statusNames = make(map[uint16]string)
	statusNames[statusSuccess] = "SUCCESS"
	statusNames[statusKeyEnoent] = "KEY_ENOENT"
	statusNames[statusKeyExists] = "KEY_EEXISTS"
	statusNames[statusE2Big] = "E2BIG"
	statusNames[statusEinval] = "EINVAL"
	statusNames[statusNotStored] = "NOT_STORED"
	statusNames[statusDeltaBadVal] = "DELTA_BADVAL"
	statusNames[statusNotMyVBucket] = "NOT_MY_VBUCKET"
	statusNames[statusUnknownCommand] = "UNKNOWN_COMMAND"
	statusNames[statusEnomem] = "ENOMEM"
	statusNames[statusTmpFail] = "TMPFAIL"
}

// Similar to:
// https://godoc.org/google.golang.org/appengine/memcache

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrServerError means that a server error occurred.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long.
	// If ASCII cannot contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")

	// ErrWrongMagic is returned if the server returns the incorrect magic
	ErrWrongMagic = errors.New("memcache: wrong magic")

	// ErrWrongOp is returned if the server returns a response for the incorrect operation
	ErrWrongOp = errors.New("memcache: wrong op")

	// ErrMissingCas is returned if a cas is required, but isn't supplied
	ErrMissingCas = errors.New("memcache: response should contain casid")

	// ErrExtrasPresent is returned if extras are present when they are required not to be
	ErrExtrasPresent = errors.New("memcache: extras present")

	// ErrKeyPresent is returned if key is present when it should't be
	ErrKeyPresent = errors.New("memcache: key present")

	// ErrValuePresent is returned if value is present when it should't be
	ErrValuePresent = errors.New("memcache: value present")

	// ErrUnsupported is returned if a method is called with a binary client that hasn't been implemented yet
	ErrUnsupported = errors.New("memcache: the binary version of this method hasn't been implemented yet")
)

type errBadStatus struct {
	op uint16
}

func (e *errBadStatus) Error() string {
	return "Bad status in response: " + statusNames[e.op]
}

const (
	// DefaultTimeout is the default socket read/write timeout.
	DefaultTimeout = 100 * time.Millisecond

	// DefaultMaxIdleConns is the default maximum number of idle connections
	// kept for any single address.
	DefaultMaxIdleConns = 2
)

type doer func(*conn, *Item) (*Item, error)

const buffered = 8 // arbitrary buffered channel size, for readability

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	switch err {
	case ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrMalformedKey:
		return true
	}
	return false
}

func (c *Client) legalKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	if !c.Binary {
		for i := 0; i < len(key); i++ {
			if key[i] <= ' ' || key[i] == 0x7f {
				return false
			}
		}
	}
	return true
}

var (
	crlf            = []byte("\r\n")
	space           = []byte(" ")
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultOk        = []byte("OK\r\n")
	resultTouched   = []byte("TOUCHED\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
)

// New returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func New(server ...string) *Client {
	ss := new(ServerList)
	err := ss.SetServers(server...)
	if err != nil {
		panic("Unable to set servers: " + err.Error())
	}
	return NewFromSelector(ss)
}

// NewFromSelector returns a new Client using the provided ServerSelector.
func NewFromSelector(ss ServerSelector) *Client {
	return &Client{selector: ss}
}

// Client is a memcache client.
// It is safe for unlocked use by multiple concurrent goroutines.
type Client struct {
	// Timeout specifies the socket read/write timeout.
	// If zero, DefaultTimeout is used.
	Timeout time.Duration
	// determines which protocol is used, binary or plaintext
	Binary bool

	// MaxIdleConns specifies the maximum number of idle connections that will
	// be maintained per address. If less than one, DefaultMaxIdleConns will be
	// used.
	//
	// Consider your expected traffic rates and latency carefully. This should
	// be set to a number higher than your peak parallel requests.
	MaxIdleConns int

	selector ServerSelector

	lk       sync.Mutex
	freeconn map[string][]*conn
}

// TODO implement the rest as we add ops
func (i *Item) validateResponse(op byte) error {
	switch op {
	case opSet:
		fallthrough
	case opAdd:
		fallthrough
	case opReplace:
		// MUST have CAS
		if i.casid == uint64(0) {
			return ErrMissingCas
		}
		// MUST not have extras
		if i.extras != nil || len(i.extras) > 0 {
			return ErrExtrasPresent
		}
		// MUST not have key
		if len(i.Key) > 0 {
			return ErrKeyPresent
		}
		// MUST not have key
		if i.Value != nil || len(i.Value) > 0 {
			return ErrValuePresent
		}
	}
	return nil
}

// Item is an item to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string

	// Value is the Item's value.
	Value []byte

	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32

	// Compare and swap ID.
	casid uint64

	// opaque
	opaque uint32

	// extras
	extras []byte
}

// conn is a connection to a server.
type conn struct {
	nc   net.Conn
	rw   *bufio.ReadWriter
	addr net.Addr
	c    *Client
}

// release returns this connection back to the client's free pool
func (cn *conn) release() {
	cn.c.putFreeConn(cn.addr, cn)
}

func (cn *conn) extendDeadline() error {
	return cn.nc.SetDeadline(time.Now().Add(cn.c.netTimeout()))
}

// condRelease releases this connection if the error pointed to by err
// is nil (not an error) or is only a protocol level error (e.g. a
// cache miss).  The purpose is to not recycle TCP connections that
// are bad.
func (cn *conn) condRelease(err *error) {
	if *err == nil || resumableError(*err) {
		cn.release()
	} else {
		_ = cn.nc.Close()
	}
}

func (c *Client) putFreeConn(addr fmt.Stringer, cn *conn) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		c.freeconn = make(map[string][]*conn)
	}
	freelist := c.freeconn[addr.String()]
	if len(freelist) >= c.maxIdleConns() {
		cn.nc.Close()
		return
	}
	c.freeconn[addr.String()] = append(freelist, cn)
}

func (c *Client) getFreeConn(addr fmt.Stringer) (cn *conn, ok bool) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		return nil, false
	}
	freelist, ok := c.freeconn[addr.String()]
	if !ok || len(freelist) == 0 {
		return nil, false
	}
	cn = freelist[len(freelist)-1]
	c.freeconn[addr.String()] = freelist[:len(freelist)-1]
	return cn, true
}

func (c *Client) netTimeout() time.Duration {
	if c.Timeout != 0 {
		return c.Timeout
	}
	return DefaultTimeout
}

func (c *Client) maxIdleConns() int {
	if c.MaxIdleConns > 0 {
		return c.MaxIdleConns
	}
	return DefaultMaxIdleConns
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "memcache: connect timeout to " + cte.Addr.String()
}

func (c *Client) dial(addr net.Addr) (net.Conn, error) {
	nc, err := net.DialTimeout(addr.Network(), addr.String(), c.netTimeout())
	if err == nil {
		return nc, nil
	}

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return nil, &ConnectTimeoutError{addr}
	}

	return nil, err
}

func (c *Client) getConn(addr net.Addr) (*conn, error) {
	cn, ok := c.getFreeConn(addr)
	if ok {
		err := cn.extendDeadline()
		if err != nil {
			return nil, err
		}
		return cn, nil
	}
	nc, err := c.dial(addr)
	if err != nil {
		return nil, err
	}
	cn = &conn{
		nc:   nc,
		addr: addr,
		c:    c,
		rw:   bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
	}
	err = cn.extendDeadline()
	if err != nil {
		return nil, err
	}
	return cn, nil
}

func (c *Client) noItemOnItem(item *Item, fn doer) error {
	_, err := c.onItem(item, fn)
	return err
}

func (c *Client) onItem(item *Item, fn doer) (*Item, error) {
	addr, err := c.selector.PickServer(item.Key)
	if err != nil {
		return nil, err
	}
	cn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	defer cn.condRelease(&err)
	item, err = fn(cn, item)
	return item, err
}

// FlushAll flushes each selector
func (c *Client) FlushAll() error {
	return c.selector.Each(c.flushAllFromAddr)
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *Client) Get(key string) (item *Item, err error) {
	if c.Binary {
		return c.onItem(&Item{Key: key}, c.get)
	}
	err = c.withKeyAddr(key, func(addr net.Addr) error {
		return c.getFromAddr(addr, []string{key}, func(it *Item) {
			item = it
		})
	})
	if err == nil && item == nil {
		err = ErrCacheMiss
	}
	return
}

// only callable as binary
func (c *Client) get(cn *conn, item *Item) (*Item, error) {
	if !c.Binary {
		panic("Only binary mode allowewd here!")
	}
	return c.binaryPopulate(cn.nc, opGet, item)
}

// Touch updates the expiry for the given key. The seconds parameter is either
// a Unix timestamp or, if seconds is less than 1 month, the number of seconds
// into the future at which time the item will expire. Zero means the item has
// no expiration time. ErrCacheMiss is returned if the key is not in the cache.
// The key must be at most 250 bytes in length.
func (c *Client) Touch(key string, seconds int32) (err error) {
	return c.withKeyAddr(key, func(addr net.Addr) error {
		return c.touchFromAddr(addr, []string{key}, seconds)
	})
}

func (c *Client) withKeyAddr(key string, fn func(net.Addr) error) (err error) {
	if !c.legalKey(key) {
		return ErrMalformedKey
	}
	addr, err := c.selector.PickServer(key)
	if err != nil {
		return err
	}
	return fn(addr)
}

func (c *Client) withAddrRw(addr net.Addr, fn func(*bufio.ReadWriter) error) (err error) {
	cn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)
	err = fn(cn.rw)
	return err
}

func (c *Client) withKeyRw(key string, fn func(*bufio.ReadWriter) error) error {
	if c.Binary {
		return ErrUnsupported
	}
	return c.withKeyAddr(key, func(addr net.Addr) error {
		return c.withAddrRw(addr, fn)
	})
}

func (c *Client) getFromAddr(addr net.Addr, keys []string, cb func(*Item)) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "gets %s\r\n", strings.Join(keys, " ")); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}
		if err := parseGetResponse(rw.Reader, cb); err != nil {
			return err
		}
		return nil
	})
}

// flushAllFromAddr send the flush_all command to the given addr
func (c *Client) flushAllFromAddr(addr net.Addr) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "flush_all\r\n"); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}
		line, err := rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultOk):
			break
		default:
			return fmt.Errorf("memcache: unexpected response line from flush_all: %q", string(line))
		}
		return nil
	})
}

func (c *Client) touchFromAddr(addr net.Addr, keys []string, expiration int32) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		for _, key := range keys {
			if _, err := fmt.Fprintf(rw, "touch %s %d\r\n", key, expiration); err != nil {
				return err
			}
			if err := rw.Flush(); err != nil {
				return err
			}
			line, err := rw.ReadSlice('\n')
			if err != nil {
				return err
			}
			switch {
			case bytes.Equal(line, resultTouched):
				break
			case bytes.Equal(line, resultNotFound):
				return ErrCacheMiss
			default:
				return fmt.Errorf("memcache: unexpected response line from touch: %q", string(line))
			}
		}
		return nil
	})
}

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (c *Client) GetMulti(keys []string) (map[string]*Item, error) {
	if c.Binary {
		return nil, ErrUnsupported
	}
	var lk sync.Mutex
	m := make(map[string]*Item)
	addItemToMap := func(it *Item) {
		lk.Lock()
		defer lk.Unlock()
		m[it.Key] = it
	}

	keyMap := make(map[net.Addr][]string)
	for _, key := range keys {
		if !c.legalKey(key) {
			return nil, ErrMalformedKey
		}
		addr, err := c.selector.PickServer(key)
		if err != nil {
			return nil, err
		}
		keyMap[addr] = append(keyMap[addr], key)
	}

	ch := make(chan error, buffered)
	for addr, keys := range keyMap {
		go func(addr net.Addr, keys []string) {
			ch <- c.getFromAddr(addr, keys, addItemToMap)
		}(addr, keys)
	}

	var err error
	for range keyMap {
		if ge := <-ch; ge != nil {
			err = ge
		}
	}
	return m, err
}

// parseGetResponse reads a GET response from r and calls cb for each
// read and allocated Item
func parseGetResponse(r *bufio.Reader, cb func(*Item)) error {
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		if bytes.Equal(line, resultEnd) {
			return nil
		}
		it := new(Item)
		size, err := scanGetResponseLine(line, it)
		if err != nil {
			return err
		}
		it.Value = make([]byte, size+2)
		_, err = io.ReadFull(r, it.Value)
		if err != nil {
			it.Value = nil
			return err
		}
		if !bytes.HasSuffix(it.Value, crlf) {
			it.Value = nil
			return fmt.Errorf("memcache: corrupt get result read")
		}
		it.Value = it.Value[:size]
		cb(it)
	}
}

// scanGetResponseLine populates it and returns the declared size of the item.
// It does not read the bytes of the item.
func scanGetResponseLine(line []byte, it *Item) (size int, err error) {
	pattern := "VALUE %s %d %d %d\r\n"
	dest := []interface{}{&it.Key, &it.Flags, &size, &it.casid}
	if bytes.Count(line, space) == 3 {
		pattern = "VALUE %s %d %d\r\n"
		dest = dest[:3]
	}
	n, err := fmt.Sscanf(string(line), pattern, dest...)
	if err != nil || n != len(dest) {
		return -1, fmt.Errorf("memcache: unexpected line in get response: %q", line)
	}
	return size, nil
}

// Set writes the given item, unconditionally.
func (c *Client) Set(item *Item) error {
	return c.noItemOnItem(item, c.set)
}

func (c *Client) set(cn *conn, item *Item) (*Item, error) {
	if c.Binary {
		return c.binaryPopulate(cn.nc, opSet, item)
	}
	return nil, c.populateOne(cn.rw, "set", item)
}

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (c *Client) Add(item *Item) error {
	return c.noItemOnItem(item, c.add)
}

func (c *Client) add(cn *conn, item *Item) (*Item, error) {
	if c.Binary {
		return nil, ErrUnsupported
	}
	return nil, c.populateOne(cn.rw, "add", item)
}

// Replace writes the given item, but only if the server *does*
// already hold data for this key
func (c *Client) Replace(item *Item) error {
	return c.noItemOnItem(item, c.replace)
}

func (c *Client) replace(cn *conn, item *Item) (*Item, error) {
	if c.Binary {
		return nil, ErrUnsupported
	}
	return nil, c.populateOne(cn.rw, "replace", item)
}

// CompareAndSwap writes the given item that was previously returned
// by Get, if the value was neither modified or evicted between the
// Get and the CompareAndSwap calls. The item's Key should not change
// between calls but all other item fields may differ. ErrCASConflict
// is returned if the value was modified in between the
// calls. ErrNotStored is returned if the value was evicted in between
// the calls.
func (c *Client) CompareAndSwap(item *Item) error {
	return c.noItemOnItem(item, c.cas)
}

func (c *Client) cas(cn *conn, item *Item) (*Item, error) {
	if c.Binary {
		return nil, ErrUnsupported
	}
	return nil, c.populateOne(cn.rw, "cas", item)
}

// TODO finish more than SET and GET
// TODO maybe use an arena for the body buff
func binaryRequest(b *bytes.Buffer, op byte, item *Item) (*bytes.Buffer, error) {
	b.Reset()
	var extraLength byte
	switch op {
	case opSet:
		extraLength = 8
	case opGet:
		extraLength = 0
	default:
		panic("unsupported operation")
	}
	errs := make([]error, 0)
	totalBody := uint32(int(extraLength) + len(item.Key) + len(item.Value))
	body := bytes.NewBuffer(make([]byte, 0, totalBody))
	f := func(x interface{}) {
		err := binary.Write(b, binary.BigEndian, x)
		if err != nil {
			errs = append(errs, err)
		}
	}
	g := func(x interface{}) {
		err := binary.Write(body, binary.BigEndian, x)
		if err != nil {
			errs = append(errs, err)
		}
	}

	f(reqMagic) // magic
	f(op)
	f(uint16(len(item.Key))) // key length
	f(extraLength)
	f(byte(0x00)) // data type
	f(uint16(0))  // status or vbucket
	f(totalBody)  // total body
	f(uint32(0))  // opaque
	f(uint64(0))  // CAS
	// extras
	switch op {
	case opSet:
		g(item.Flags)
		g(item.Expiration)
	case opGet:
		break
	default:
		panic("unsupported operation")
	}
	g([]byte(item.Key))
	g(item.Value)
	if b.Len() != int(headerSize) || body.Len() != int(totalBody) {
		panic(fmt.Sprintf("wrong size, internal error, this is a bug, expected header %d got %d; expected body %d got %d", headerSize, b.Len(), totalBody, body.Len()))
	}
	if len(errs) > 0 {
		return nil, errs[0] // return first
	}
	return body, nil
}

func (c *Client) binaryPopulate(conn io.ReadWriter, op byte, item *Item) (*Item, error) {
	if !c.legalKey(item.Key) {
		return nil, ErrMalformedKey
	}
	b := make([]byte, headerSize)
	headerBuff := bytes.NewBuffer(b)
	body, err := binaryRequest(headerBuff, op, item)
	if err != nil {
		return nil, err
	}
	_, err = conn.Write(headerBuff.Bytes())
	if err != nil {
		return nil, err
	}
	if body.Len() > 0 {
		_, err = conn.Write(body.Bytes())
		if err != nil {
			return nil, err
		}
	}
	return binaryResponse(b, conn, op)
}

// TODO maybe use an arena for the body buff
func binaryResponse(headerBuff []byte, conn io.Reader, op byte) (*Item, error) {
	_, err := io.ReadFull(conn, headerBuff)
	if err != nil {
		return nil, err
	}

	magic := headerBuff[0]
	if magic != resMagic {
		return nil, ErrWrongMagic
	}
	opCode := headerBuff[1]
	if opCode != op {
		return nil, ErrWrongOp
	}
	keyLen := int(binary.BigEndian.Uint16(headerBuff[2:4]))
	extraLen := int(headerBuff[4])

	status := binary.BigEndian.Uint16(headerBuff[6:8])
	if status != statusSuccess {
		return nil, &errBadStatus{op: status}
	}

	opaque := binary.BigEndian.Uint32(headerBuff[12:16])
	cas := binary.BigEndian.Uint64(headerBuff[16:24])

	bodyLen := int(binary.BigEndian.Uint32(headerBuff[8:12])) - (keyLen + extraLen)

	buf := make([]byte, keyLen+extraLen+bodyLen)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return nil, err
	}

	responseItem := &Item{casid: cas, opaque: opaque}
	if extraLen > 0 {
		responseItem.extras = buf[0:extraLen]
	}
	if keyLen > 0 {
		responseItem.Key = string(buf[extraLen : keyLen+extraLen])
	}
	if keyLen+extraLen > 0 {
		responseItem.Value = buf[keyLen+extraLen:]
	}

	err = responseItem.validateResponse(op)
	if err != nil {
		return nil, err
	}
	return responseItem, nil
}

func (c *Client) writeItem(rw *bufio.ReadWriter, verb string, item *Item) error {
	var err error
	if verb == "cas" {
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d %d\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value), item.casid)
	} else {
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value))
	}
	if err != nil {
		return err
	}
	if _, err = rw.Write(item.Value); err != nil {
		return err
	}
	if _, err = rw.Write(crlf); err != nil {
		return err
	}
	if err = rw.Flush(); err != nil {
		return err
	}
	return nil
}

func (c *Client) populateOne(rw *bufio.ReadWriter, verb string, item *Item) error {
	if !c.legalKey(item.Key) {
		return ErrMalformedKey
	}
	err := c.writeItem(rw, verb, item)
	line, err := rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultStored):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrNotStored
	case bytes.Equal(line, resultExists):
		return ErrCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line from %q: %q", verb, string(line))
}

func writeReadLine(rw *bufio.ReadWriter, format string, args ...interface{}) ([]byte, error) {
	_, err := fmt.Fprintf(rw, format, args...)
	if err != nil {
		return nil, err
	}
	if err = rw.Flush(); err != nil {
		return nil, err
	}
	return rw.ReadSlice('\n')
}

func writeExpectf(rw *bufio.ReadWriter, expect []byte, format string, args ...interface{}) error {
	line, err := writeReadLine(rw, format, args...)
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultOK):
		return nil
	case bytes.Equal(line, expect):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrNotStored
	case bytes.Equal(line, resultExists):
		return ErrCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line: %q", string(line))
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *Client) Delete(key string) error {
	return c.withKeyRw(key, func(rw *bufio.ReadWriter) error {
		return writeExpectf(rw, resultDeleted, "delete %s\r\n", key)
	})
}

// DeleteAll deletes all items in the cache.
func (c *Client) DeleteAll() error {
	return c.withKeyRw("", func(rw *bufio.ReadWriter) error {
		return writeExpectf(rw, resultDeleted, "flush_all\r\n")
	})
}

// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
func (c *Client) Increment(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("incr", key, delta)
}

// Decrement atomically decrements key by delta. The return value is
// the new value after being decremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On underflow, the new value is capped at zero and does not wrap
// around.
func (c *Client) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("decr", key, delta)
}

func (c *Client) incrDecr(verb, key string, delta uint64) (uint64, error) {
	var val uint64
	err := c.withKeyRw(key, func(rw *bufio.ReadWriter) error {
		line, err := writeReadLine(rw, "%s %s %d\r\n", verb, key, delta)
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultNotFound):
			return ErrCacheMiss
		case bytes.HasPrefix(line, resultClientErrorPrefix):
			errMsg := line[len(resultClientErrorPrefix) : len(line)-2]
			return errors.New("memcache: client error: " + string(errMsg))
		}
		val, err = strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
		return err
	})
	return val, err
}
