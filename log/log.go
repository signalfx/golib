package log

import (
	"errors"
)

type Logger interface {
	Log(keyvals ...interface{})
}

type Disablable interface {
	Disabled() bool
}

type ErrorHandler interface {
	ErrorLogger(error) Logger
}

type ErrorHandlingLogger interface {
	Logger
	ErrorHandler
}

type ErrorHandlingDisablableLogger interface {
	Logger
	ErrorHandler
	Disablable
}

type ErrorHandlerFunc func(error) Logger

var ErrMissingValue = errors.New("(MISSING)")

func (f ErrorHandlerFunc) ErrorLogger(e error) Logger {
	return f(e)
}

type Context struct {
	Logger    Logger
	KeyVals   []interface{}
}

var DefaultKeyvalsSize = 16

func NewContext(logger Logger) *Context {
	if c, ok := logger.(*Context); ok {
		return c
	}
	ctx := &Context{
		Logger: logger,
	}
	return ctx
}

func isDisabled(l Logger) bool {
	if disable, ok := l.(Disablable); ok && disable.Disabled() {
		return true
	}
	return false
}

func addArrays(a, b []interface{}) []interface{} {
	if len(b) == 0 && len(a) == 0 {
		return []interface{}{}
	}
	n := len(a) + len(b)
	if len(a) % 2 != 0 {
		n++
	}
	if len(b) % 2 != 0 {
		n++
	}
	ret := make([]interface{}, 0, n)
	ret = append(ret, a...)
	if len(a) % 2 != 0 {
		ret = append(ret, ErrMissingValue)
	}
	ret = append(ret, b...)
	if len(b) % 2 != 0 {
		ret = append(ret, ErrMissingValue)
	}
	return ret
}

func (l *Context) Log(keyvals ...interface{}) {
	// Note: The assumption here is that copyIfDynamic is "slow" since dynamic values
	//       could be slow to calculate.  So to optimize the case of "logs are turned off"
	//       we enable the ability to early return if the logger is off.
	if isDisabled(l.Logger) {
		return
	}
	l.Logger.Log(copyIfDynamic(addArrays(l.KeyVals, keyvals))...)
}

func (l *Context) With(keyvals ...interface{}) *Context {
	if len(keyvals) == 0 {
		return l
	}
	return &Context {
		Logger: l.Logger,
		KeyVals:   addArrays(l.KeyVals, keyvals),
	}
}

func (l *Context) WithPrefix(keyvals ...interface{}) *Context {
	if len(keyvals) == 0 {
		return l
	}
	return &Context{
		Logger:    l.Logger,
		KeyVals:   addArrays(keyvals, l.KeyVals),
	}
}

type LoggerFunc func(...interface{})

func (f LoggerFunc) Log(keyvals ...interface{}) {
	f(keyvals...)
}