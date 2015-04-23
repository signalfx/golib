package logherd

import (
	"path/filepath"
	"runtime"

	"io"

	"sync"

	"github.com/Sirupsen/logrus"
)

//  A group of walrus is a herd .... we manage a group of logrus .........
//  get it?
//  the joke?
//  because a bunch of walrus .... logrus.....
//  got it now?
type herdManager struct {
	loggers map[loggerKey]*logInstance
	lock    sync.Mutex
}

type loggerKey struct {
	filename string
}

type logInstance struct {
	innerLogger *logrus.Logger
}

// NoopWriter makes all Write() functions return as fully written without actually doing anything
type NoopWriter struct{}

func (d *NoopWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

var _ io.Writer = &NoopWriter{}

type wrappedWritter struct {
	io.Writer
	setBackTo io.Writer
}

var _ io.Writer = &wrappedWritter{}

// LogrusDimensionAdder is a hook that adds the dimension Key->Value to the event's data.
type LogrusDimensionAdder struct {
	Key, Value string
	FireLevels []logrus.Level
}

// Fire adds [Key] = [Value] to the event
func (l *LogrusDimensionAdder) Fire(e *logrus.Entry) error {
	if _, exists := e.Data[l.Key]; !exists {
		e.Data[l.Key] = l.Value
	}
	return nil
}

var allLogrusLevels = []logrus.Level{
	logrus.PanicLevel,
	logrus.FatalLevel,
	logrus.ErrorLevel,
	logrus.WarnLevel,
	logrus.InfoLevel,
	logrus.DebugLevel,
}

// Levels returns the field FireLevels
func (l *LogrusDimensionAdder) Levels() []logrus.Level {
	return l.FireLevels
}

var _ logrus.Hook = &LogrusDimensionAdder{}

var instance = &herdManager{
	loggers: make(map[loggerKey]*logInstance),
}

// SetLevel will set the level of the default logrus logger and all loggers registered to sfxlog
func SetLevel(level logrus.Level) logrus.Level {
	instance.lock.Lock()
	defer instance.lock.Unlock()
	ret := logrus.GetLevel()
	logrus.SetLevel(level)
	for _, v := range instance.loggers {
		v.innerLogger.Level = level
	}
	return ret
}

// SetOutput sets the output writer of all loggers and return the previous output.  You can stub
// the output during a function with something like
// func TestBob() {
//   defer sfxlog.SetOutput(sfxlog.SetOutput(&sfxlog.NoopWriter{}))
// }
func SetOutput(out io.Writer) io.Writer {
	// When used as the example above, on the second call you want to wait as long as possible
	// before setting the writer back, so Gosched
	runtime.Gosched()
	instance.lock.Lock()
	defer instance.lock.Unlock()
	logrus.SetOutput(out)
	ret := logrus.StandardLogger().Out
	for _, v := range instance.loggers {
		v.innerLogger.Out = out
	}
	return ret
}

// SilenceOutput for all loggers while restoring each when done.  Intended to be used like:
// func Bob() {
//    defer sfxlog.SilenceOutput()()
//    ....
// }
func SilenceOutput() func() {
	instance.lock.Lock()
	defer instance.lock.Unlock()

	silence := &NoopWriter{}
	logrus.SetOutput(&wrappedWritter{Writer: silence, setBackTo: logrus.StandardLogger().Out})
	for _, v := range instance.loggers {
		v.innerLogger.Out = &wrappedWritter{Writer: silence, setBackTo: v.innerLogger.Out}
	}
	ret := func() {
		// It's usually best to yield here so we can end any Close() goroutines and silence their
		// logging too
		runtime.Gosched()
		if item, ok := logrus.StandardLogger().Out.(*wrappedWritter); ok {
			logrus.SetOutput(item.setBackTo)
		}
		for _, v := range instance.loggers {
			if item, ok := v.innerLogger.Out.(*wrappedWritter); ok {
				v.innerLogger.Out = item.setBackTo
			}
		}
	}
	return ret
}

// SetFormatter setes the output formatter for all loggers
func SetFormatter(formatter logrus.Formatter) logrus.Formatter {
	instance.lock.Lock()
	defer instance.lock.Unlock()
	ret := logrus.StandardLogger().Formatter
	logrus.SetFormatter(formatter)
	for _, v := range instance.loggers {
		v.innerLogger.Formatter = formatter
	}
	return ret
}

var runtimeCaller = runtime.Caller

// New returns a logger that will add a param called "file" that is the filename of the caller
func New() *logrus.Logger {
	instance.lock.Lock()
	defer instance.lock.Unlock()
	_, file, _, ok := runtimeCaller(1)
	if !ok {
		file = "unknown"
	}
	key := loggerKey{
		filename: filepath.Base(file),
	}
	if current, exists := instance.loggers[key]; exists {
		logrus.WithField("file", file).Warn("Ambiguous logger")
		return current.innerLogger
	}

	ret := logrus.New()
	ret.Level = logrus.GetLevel()
	inst := &logInstance{
		innerLogger: ret,
	}
	instance.loggers[key] = inst
	ret.Out = logrus.StandardLogger().Out
	ret.Formatter = logrus.StandardLogger().Formatter
	ret.Hooks.Add(&LogrusDimensionAdder{
		Key:        "file",
		Value:      key.filename,
		FireLevels: allLogrusLevels,
	})
	return ret
}

// Debug to logger a key/value pair and message.  Intended to save the mem alloc that WithField creates
func Debug(l *logrus.Logger, key string, val interface{}, msg string) {
	if l.Level >= logrus.DebugLevel {
		if key != "" {
			l.WithField(key, val).Debug(msg)
		} else {
			l.Debug(msg)
		}
	}
}

// Debug2 to logger 2 key/value pairs and message.  Intended to save the mem alloc that WithField creates
func Debug2(l *logrus.Logger, key string, val interface{}, key2 string, val2 interface{}, msg string) {
	if l.Level >= logrus.DebugLevel {
		if key != "" {
			l.WithField(key, val).WithField(key2, val2).Debug(msg)
		} else {
			l.Debug(msg)
		}
	}
}

// Debug3 to logger 3 key/value pairs and message.  Intended to save the mem alloc that WithField creates
func Debug3(l *logrus.Logger, key string, val interface{}, key2 string, val2 interface{}, key3 string, val3 interface{}, msg string) {
	if l.Level >= logrus.DebugLevel {
		if key != "" {
			l.WithField(key, val).WithField(key2, val2).WithField(key3, val3).Debug(msg)
		} else {
			l.Debug(msg)
		}
	}
}

// Debug4 to logger 4 key/value pairs and message.  Intended to save the mem alloc that WithField creates
func Debug4(l *logrus.Logger, key string, val interface{}, key2 string, val2 interface{}, key3 string, val3 interface{}, key4 string, val4 interface{}, msg string) {
	if l.Level >= logrus.DebugLevel {
		if key != "" {
			l.WithField(key, val).WithField(key2, val2).WithField(key3, val3).WithField(key4, val4).Debug(msg)
		} else {
			l.Debug(msg)
		}
	}
}

// Debugf to logger a logrus.Fields and message.  Intended to save the mem alloc that WithField creates
func Debugf(l *logrus.Logger, fields logrus.Fields, msg string) {
	if l.Level >= logrus.DebugLevel {
		l.WithFields(fields).Debug(msg)
	}
}
