package log

import "sync/atomic"

type Counter struct {
	Count int64
}

var _ Logger = &Counter{}

func (c *Counter) Log(keyvals ...interface{}) {
	atomic.AddInt64(&c.Count, 1)
}

func (c *Counter) ErrorLogger(error) Logger {
	return c
}