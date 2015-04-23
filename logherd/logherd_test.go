package logherd

import (
	"runtime"
	"testing"

	"os"

	"bytes"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestLogger(t *testing.T) {
	x := New()
	x.Printf("hello")
}

func TestSetLevel(t *testing.T) {
	l := logrus.StandardLogger().Level
	defer SetLevel(l)
	SetLevel(logrus.DebugLevel)
	assert.Equal(t, logrus.DebugLevel, logrus.StandardLogger().Level)

	o := logrus.StandardLogger().Out
	defer SetOutput(o)
	SetOutput(os.Stderr)
	assert.Equal(t, os.Stderr, logrus.StandardLogger().Out)

	f := logrus.StandardLogger().Formatter
	defer SetFormatter(f)
	SetFormatter(nil)
	assert.Equal(t, nil, logrus.StandardLogger().Formatter)
}

func TestSilenceOutput(t *testing.T) {
	x := New()
	buf := new(bytes.Buffer)
	x.Out = buf
	x.Info("Hello")
	assert.True(t, buf.Len() > 0)
	buf.Reset()
	defer SilenceOutput()()
	x.Info("hello")
	assert.Equal(t, 0, buf.Len())
}

func TestMultiple(t *testing.T) {
	x := New()
	x.Level = logrus.WarnLevel
	x = New()
	assert.Equal(t, logrus.WarnLevel, x.Level)
}

func TestBadRuntimeCaller(t *testing.T) {
	runtimeCaller = func(level int) (pc uintptr, file string, line int, ok bool) {
		return
	}
	defer func() {
		runtimeCaller = runtime.Caller
	}()
	x := New()
	x.Level = logrus.WarnLevel
	x = New()
	assert.Equal(t, logrus.WarnLevel, x.Level)
}

type badHook struct {
	count int
}

func (b *badHook) Fire(e *logrus.Entry) error {
	b.count++
	return nil
}

func (b *badHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.DebugLevel}
}

func TestDebugLevelOff(t *testing.T) {
	x := New()
	b := &badHook{}
	x.Hooks.Add(b)
	Debug(x, "k", "v", "")
	Debug2(x, "k", "v", "", "", "")
	Debug3(x, "k", "v", "", "", "", "", "")
	Debug4(x, "k", "v", "", "", "", "", "", "", "")
	Debugf(x, nil, "")
	assert.Equal(t, 0, b.count)
}

func TestDebugLevelOn(t *testing.T) {
	x := New()
	x.Level = logrus.DebugLevel
	b := &badHook{}
	x.Hooks.Add(b)
	Debug(x, "k", "v", "")
	Debug2(x, "k", "v", "", "", "")
	Debug3(x, "k", "v", "", "", "", "", "")
	Debug4(x, "k", "v", "", "", "", "", "", "", "")
	Debugf(x, nil, "")

	Debug(x, "", "v", "")
	Debug2(x, "", "v", "", "", "")
	Debug3(x, "", "v", "", "", "", "", "")
	Debug4(x, "", "v", "", "", "", "", "", "", "")
	assert.Equal(t, 9, b.count)
}
