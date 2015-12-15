package log

type MultiLogger []Logger

var _ Logger = MultiLogger(nil)

func (c MultiLogger) Log(keyvals ...interface{}) {
	for _, l := range c {
		l.Log(keyvals...)
	}
}

func (c MultiLogger) Disabled() bool {
	for _, l := range c {
		if disablable, ok := l.(Disablable); !ok || !disablable.Disabled() {
			return false
		}
	}
	return true
}