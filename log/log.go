package log

import (
	"errors"
)

// Logger is the minimal interface for logging methods
type Logger interface {
	Log(keyvals ...interface{})
}

// Disableable is an optional interface that a logger can implement to signal it is disabled and should not
// be sent log messages for optimization reasons
type Disableable interface {
	Disabled() bool
}

// ErrorHandler can handle errors that various loggers may return
type ErrorHandler interface {
	ErrorLogger(error) Logger
}

// ErrorHandlingLogger wraps Logger and ErrorHandler
type ErrorHandlingLogger interface {
	Logger
	ErrorHandler
}

// ErrorHandlingDisableableLogger wraps Logger and ErrorHandler and Disabled
type ErrorHandlingDisableableLogger interface {
	Logger
	ErrorHandler
	Disableable
}

// ErrorHandlerFunc converts a function into a ErrorHandler
type ErrorHandlerFunc func(error) Logger

// ErrorLogger calls the wrapped function
func (f ErrorHandlerFunc) ErrorLogger(e error) Logger {
	return f(e)
}

// ErrMissingValue is the value used for the "value" in odd numbered log statements
var ErrMissingValue = errors.New("(MISSING)")

// Context allows users to create a logger that appends key/values to log statements
type Context struct {
	Logger  Logger
	KeyVals []interface{}
}

// NewContext creates a context out of a logger
func NewContext(logger Logger) *Context {
	if c, ok := logger.(*Context); ok {
		return c
	}
	ctx := &Context{
		Logger: logger,
	}
	return ctx
}

// IsDisabled returns true if the wrapped logger implements
func IsDisabled(l Logger) bool {
	if disable, ok := l.(Disableable); ok && disable.Disabled() {
		return true
	}
	return false
}

func addArrays(a, b []interface{}) []interface{} {
	if len(b) == 0 && len(a) == 0 {
		return []interface{}{}
	}
	n := len(a) + len(b)
	if len(a)%2 != 0 {
		n++
	}
	if len(b)%2 != 0 {
		n++
	}
	ret := make([]interface{}, 0, n)
	ret = append(ret, a...)
	if len(a)%2 != 0 {
		ret = append(ret, ErrMissingValue)
	}
	ret = append(ret, b...)
	if len(b)%2 != 0 {
		ret = append(ret, ErrMissingValue)
	}
	return ret
}

// Log calls Log() on the wrapped logger appending the Context's values
func (l *Context) Log(keyvals ...interface{}) {
	// Note: The assumption here is that copyIfDynamic is "slow" since dynamic values
	//       could be slow to calculate.  So to optimize the case of "logs are turned off"
	//       we enable the ability to early return if the logger is off.
	if IsDisabled(l.Logger) {
		return
	}
	l.Logger.Log(copyIfDynamic(addArrays(l.KeyVals, keyvals))...)
}

// With returns a new context that adds key/values to log statements
func (l *Context) With(keyvals ...interface{}) *Context {
	if len(keyvals) == 0 {
		return l
	}
	return &Context{
		Logger:  l.Logger,
		KeyVals: addArrays(l.KeyVals, keyvals),
	}
}

// WithPrefix is like With but adds keyvalus to the beginning of the eventual log statement
func (l *Context) WithPrefix(keyvals ...interface{}) *Context {
	if len(keyvals) == 0 {
		return l
	}
	return &Context{
		Logger:  l.Logger,
		KeyVals: addArrays(keyvals, l.KeyVals),
	}
}

// LoggerFunc converts a function into a Logger
type LoggerFunc func(...interface{})

// Log calls the wrapped function
func (f LoggerFunc) Log(keyvals ...interface{}) {
	f(keyvals...)
}
