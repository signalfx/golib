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
func makeConf() (ReaderWriter, *Config) {
	memConf := Mem()
	conf := &Config{
		registeredVars: make(map[string]configVariable),
		readers:        []Reader{memConf},
	}
	return memConf, conf
}

func TestDistconfInt(t *testing.T) {
	memConf, conf := makeConf()
	defer conf.Close()

	// default
	val := conf.Int("testval", 1)
	assert.Equal(t, int64(1), val.Get())
	totalWatches := 0
	val.Watch(IntWatch(func(str *Int, oldValue int64) {
		totalWatches++
	}))

	// update valid
	memConf.Write("testval", []byte("2"))
	assert.Equal(t, int64(2), val.Get())

	// check already registered
	conf.Str("testval_other", "moo")
	var nilInt *Int
	assert.Equal(t, nilInt, conf.Int("testval_other", 0))

	// update to invalid
	memConf.Write("testval", []byte("invalidint"))
	assert.Equal(t, int64(2), val.Get())

	// update to nil
	memConf.Write("testval", nil)
	assert.Equal(t, int64(1), val.Get())

	// check callback
	assert.Equal(t, 2, totalWatches)
}

func TestDistconfFloat(t *testing.T) {
	memConf, conf := makeConf()
	defer conf.Close()

	// default
	val := conf.Float("testval", 3.14)
	assert.Equal(t, float64(3.14), val.Get())
	totalWatches := 0
	val.Watch(FloatWatch(func(float *Float, oldValue float64) {
		totalWatches++
	}))

	// update to valid
	memConf.Write("testval", []byte("4.771"))
	assert.Equal(t, float64(4.771), val.Get())

	// check already registered
	conf.Str("testval_other", "moo")
	var nilFloat *Float
	assert.Equal(t, nilFloat, conf.Float("testval_other", 0.0))

	// update to invalid
	memConf.Write("testval", []byte("invalidfloat"))
	assert.Equal(t, float64(4.771), val.Get())

	// update to nil
	memConf.Write("testval", nil)
	assert.Equal(t, float64(3.14), val.Get())

	// check callback
	assert.Equal(t, 2, totalWatches)
}

func TestDistconfStr(t *testing.T) {
	memConf, conf := makeConf()
	defer conf.Close()

	// default
	val := conf.Str("testval", "default")
	assert.Equal(t, "default", val.Get())
	totalWatches := 0
	val.Watch(StrWatch(func(str *Str, oldValue string) {
		totalWatches++
	}))

	// update to valid
	memConf.Write("testval", []byte("newval"))
	assert.Equal(t, "newval", val.Get())

	// check already registered
	conf.Int("testval_other", 0)
	var nilStr *Str
	assert.Equal(t, nilStr, conf.Str("testval_other", ""))

	// update to nil
	memConf.Write("testval", nil)
	assert.Equal(t, "default", val.Get())

	// check callback
	assert.Equal(t, 2, totalWatches)

}

func TestDistconfDuration(t *testing.T) {
	memConf, conf := makeConf()
	defer conf.Close()

	//default

	val := conf.Duration("testval", time.Second)
	assert.Equal(t, time.Second, val.Get())
	totalWatches := 0
	val.Watch(DurationWatch(func(*Duration, time.Duration) {
		totalWatches++
	}))

	// update valid
	memConf.Write("testval", []byte("10ms"))
	assert.Equal(t, time.Millisecond*10, val.Get())

	// check already registered
	conf.Str("testval_other", "moo")
	var nilDuration *Duration
	assert.Equal(t, nilDuration, conf.Duration("testval_other", 0))

	// update to invalid
	memConf.Write("testval", []byte("abcd"))
	assert.Equal(t, time.Second, val.Get())

	// update to nil
	memConf.Write("testval", nil)
	assert.Equal(t, time.Second, val.Get())

	assert.Equal(t, 2, totalWatches)
}

func TestDistconfBool(t *testing.T) {
	memConf, conf := makeConf()
	defer conf.Close()

	//default

	val := conf.Bool("testval", false)
	assert.False(t, val.Get())
	totalWatches := 0
	val.Watch(BoolWatch(func(*Bool, bool) {
		totalWatches++
	}))

	// update valid
	memConf.Write("testval", []byte("true"))
	assert.True(t, val.Get())

	// update valid
	memConf.Write("testval", []byte("FALSE"))
	assert.False(t, val.Get())

	// check already registered
	conf.Str("testval_other", "moo")
	var nilBool *Bool
	assert.Equal(t, nilBool, conf.Bool("testval_other", true))

	// update to invalid
	memConf.Write("testval", []byte("__"))
	assert.False(t, val.Get())

	// update to nil
	memConf.Write("testval", nil)
	assert.False(t, val.Get())

	assert.Equal(t, 2, totalWatches)
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
