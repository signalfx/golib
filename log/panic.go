package log

import "log"

type panicLogger struct{}

// Panic is a logger that always panics
var Panic ErrorHandlingLogger = &panicLogger{}

// Log calls the stdlib log with panic for keyvals
func (n *panicLogger) Log(keyvals ...interface{}) {
	log.Panic(keyvals...)
}

// ErrorLogger returns the wrapped logger
func (n *panicLogger) ErrorLogger(error) Logger {
	return n
}
