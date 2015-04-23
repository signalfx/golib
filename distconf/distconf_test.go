package distconf

import (
	"errors"
	"testing"

	"time"

	"github.com/stretchr/testify/assert"
)

type allErrorBacking struct {
}

var errNope = errors.New("nope")

func (m *allErrorBacking) Get(key string) ([]byte, error) {
	return nil, errNope
}

func (m *allErrorBacking) Write(key string, value []byte) error {
	return errNope
}

func (m *allErrorBacking) Watch(key string, callback backingCallbackFunction) error {
	return errNope
}

func (m *allErrorBacking) Close() {
}

type allErrorconfigVariable struct {
}

func (a *allErrorconfigVariable) Update(newValue []byte) error {
	return errNope
}

func TestDistconf(t *testing.T) {
	memConf := Mem()
	conf := &Config{
		registeredVars: make(map[string]configVariable),
		readers:        []Reader{memConf},
	}
	defer conf.Close()
	iVal := conf.Int("testval", 1)
	assert.Equal(t, int64(1), iVal.Get())
	totalWatches := 0
	iVal.Watch(IntWatch(func(str *Int, oldValue int64) {
		totalWatches++
	}))

	memConf.Write("testval", []byte("2"))
	assert.Equal(t, int64(2), iVal.Get())

	sVal := conf.Str("testval_s", "default")
	assert.Equal(t, "default", sVal.Get())
	sVal.Watch(StrWatch(func(str *Str, oldValue string) {
		totalWatches++
	}))

	memConf.Write("testval_s", []byte("newval"))
	assert.Equal(t, "newval", sVal.Get())

	var nilInt *Int
	assert.Equal(t, nilInt, conf.Int("testval_s", 0))

	var nilStr *Str
	assert.Equal(t, nilStr, conf.Str("testval", ""))

	assert.NotPanics(t, func() {
		(&noopCloser{}).Close()
	})

	memConf.Write("testval", []byte("invalidint"))
	assert.Equal(t, int64(1), iVal.Get())

	assert.Equal(t, 3, totalWatches)

	assert.Nil(t, conf.Duration("testval_s", time.Second))
	memConf.Write("testval_t", []byte("3ms"))
	timeVal := conf.Duration("testval_t", time.Second)
	assert.Equal(t, time.Millisecond*3, timeVal.Get())

	timeVal.Watch(DurationWatch(func(*Duration, time.Duration) {
		totalWatches++
	}))
	memConf.Write("testval_t", []byte("10ms"))
	assert.Equal(t, time.Millisecond*10, timeVal.Get())
	assert.Equal(t, 4, totalWatches)

	memConf.Write("testval_t", []byte("abcd"))
	assert.Equal(t, time.Second, timeVal.Get())
	assert.Equal(t, 5, totalWatches)

	memConf.Write("testval_t", nil)
	assert.Equal(t, time.Second, timeVal.Get())
	assert.Equal(t, 5, totalWatches)
}

func TestDistconfErrorBackings(t *testing.T) {
	conf := &Config{
		registeredVars: make(map[string]configVariable),
		readers:        []Reader{&allErrorBacking{}},
	}

	iVal := conf.Int("testval", 1)
	assert.Equal(t, int64(1), iVal.Get())

	assert.NotPanics(t, func() {
		conf.onBackingChange("not_in_map")
	})

	assert.NotPanics(t, func() {
		conf.refresh("testval2", &allErrorconfigVariable{})
	})

}
