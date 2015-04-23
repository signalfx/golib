package sfxclient

import (
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/signalfx/golib/datapoint"
)

// A Value is the abstract type for all types that can export a signalfx value
type Value interface {
	GetValue() (datapoint.Value, error)
}

// StringValue extracts a datapoint from a stringer variable (or an expvar.Var)
type StringValue struct {
	fmt.Stringer // Same as expvar.Var
}

// Val is the smplest exported value helper, calling String() on s and attempting to parse
// the result into an integer or float.  It may be more efficient to use IntAddr or FloatVal
// instead
func Val(s fmt.Stringer) *StringValue {
	return &StringValue{
		s,
	}
}

var errUnableToParseExpvarValue = errors.New("unable to parse expvar value")

// GetValue attempts to convert the the stringer value into a datapoint value for export
func (e *StringValue) GetValue() (datapoint.Value, error) {
	kv := e.String()
	i, err := strconv.ParseInt(kv, 10, 64)
	if err == nil {
		return datapoint.NewIntValue(i), nil
	}
	f, err := strconv.ParseFloat(kv, 64)
	if err == nil {
		return datapoint.NewFloatValue(f), nil
	}
	return nil, errUnableToParseExpvarValue
}

// IntAddr attempts to be an integer value by using atomic.LoadInt64 on the stored integer
type IntAddr struct {
	*int64
}

// Int wraps a pointer to an int64 for client requests.
func Int(i *int64) IntAddr {
	return IntAddr{i}
}

// GetValue will return a datpoint value calling atomic.LoadInt64 on the address
func (e IntAddr) GetValue() (datapoint.Value, error) {
	return datapoint.NewIntValue(atomic.LoadInt64(e.int64)), nil
}

// UIntAddr attempts to be an integer value by using atomic.LoadInt64 on the stored integer
type UIntAddr struct {
	*uint64
}

// UInt wraps a *uint64 for client data
func UInt(i *uint64) UIntAddr {
	return UIntAddr{i}
}

// GetValue will return a datpoint value calling atomic.LoadUint64 on the address.  If it overflows
// int64, it will return a datapoint.Value in the negative range.
func (e UIntAddr) GetValue() (datapoint.Value, error) {
	return datapoint.NewIntValue(int64(atomic.LoadUint64(e.uint64))), nil
}

// IntVal loads an integer from a callback
type IntVal func() int64

// GetValue creates a new datapoint value of type Float from the callback.
func (e IntVal) GetValue() (datapoint.Value, error) {
	return datapoint.NewIntValue(e()), nil
}

// FloatVal gets its datapoint value from a callback
type FloatVal func() float64

// GetValue creates a new datapoint value of type Float from the callback.
func (e FloatVal) GetValue() (datapoint.Value, error) {
	return datapoint.NewFloatValue(e()), nil
}
