package log
import "log"

type panicLogger struct {}

var Panic ErrorHandlingLogger = &panicLogger{}

func (n *panicLogger) Log(keyvals ...interface{}) {
	log.Panic(keyvals...)
}

func (n *panicLogger) ErrorLogger(error) Logger {
	return n
}