package log

type nop struct{}

var Discard ErrorHandlingDisablableLogger = nop{}

func (n nop) Log(keyvals ...interface{}) {
}

func (n nop) ErrorLogger(error) Logger {
	return n
}

func (n nop) Disabled() bool {
	return true
}