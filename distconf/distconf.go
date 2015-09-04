package distconf

import (
	"sync"

	"time"

	"math"

	"expvar"

	log "github.com/Sirupsen/logrus"
)

// Config gets configuration data from the first backing that has it
type Config struct {
	readers []Reader

	varsMutex      sync.Mutex
	registeredVars map[string]configVariable
}

type configVariable interface {
	Update(newValue []byte) error
	// Get but on an interface return.  Oh how I miss you templates.
	GenericGet() interface{}
}

type noopCloser struct {
}

func (n *noopCloser) Close() {
}

// Var returns an expvar variable that shows all the current configuration variables and their
// current value
func (c *Config) Var() expvar.Var {
	return expvar.Func(func() interface{} {
		c.varsMutex.Lock()
		defer c.varsMutex.Unlock()

		m := make(map[string]interface{})
		for name, v := range c.registeredVars {
			m[name] = v.GenericGet()
		}
		return m
	})
}

// Int object that can be referenced to get integer values from a backing config
func (c *Config) Int(key string, defaultVal int64) *Int {
	s := &intConf{
		defaultVal: defaultVal,
		Int: Int{
			currentVal: defaultVal,
		},
	}
	// Note: in race conditions 's' may not be the thing actually returned
	ret, okCast := c.register(key, s).(*intConf)
	if !okCast {
		log.WithField("key", key).Error("Registering key with multiple types!  FIX ME!!!!")
		return nil
	}
	return &ret.Int
}

// Float object that can be referenced to get float values from a backing config
func (c *Config) Float(key string, defaultVal float64) *Float {
	s := &floatConf{
		defaultVal: defaultVal,
		Float: Float{
			currentVal: math.Float64bits(defaultVal),
		},
	}
	// Note: in race conditions 's' may not be the thing actually returned
	ret, okCast := c.register(key, s).(*floatConf)
	if !okCast {
		log.WithField("key", key).Error("Registering key with multiple types!  FIX ME!!!!")
		return nil
	}
	return &ret.Float
}

// Str object that can be referenced to get string values from a backing config
func (c *Config) Str(key string, defaultVal string) *Str {
	s := &strConf{
		defaultVal: defaultVal,
	}
	s.currentVal.Store(defaultVal)
	// Note: in race conditions 's' may not be the thing actually returned
	ret, okCast := c.register(key, s).(*strConf)
	if !okCast {
		log.WithField("key", key).Error("Registering key with multiple types!  FIX ME!!!!")
		return nil
	}
	return &ret.Str
}

// Bool object that can be referenced to get boolean values from a backing config
func (c *Config) Bool(key string, defaultVal bool) *Bool {
	var defautlAsInt int32
	if defaultVal {
		defautlAsInt = 1
	} else {
		defautlAsInt = 0
	}

	s := &boolConf{
		defaultVal: defautlAsInt,
		Bool: Bool{
			currentVal: defautlAsInt,
		},
	}
	// Note: in race conditions 's' may not be the thing actually returned
	ret, okCast := c.register(key, s).(*boolConf)
	if !okCast {
		log.WithField("key", key).Error("Registering key with multiple types!  FIX ME!!!!")
		return nil
	}
	return &ret.Bool
}

// Duration returns a duration object that calls ParseDuration() on the given key
func (c *Config) Duration(key string, defaultVal time.Duration) *Duration {
	s := &durationConf{
		defaultVal: defaultVal,
		Duration: Duration{
			currentVal: defaultVal.Nanoseconds(),
		},
	}
	// Note: in race conditions 's' may not be the thing actually returned
	ret, okCast := c.register(key, s).(*durationConf)
	if !okCast {
		log.WithField("key", key).Error("Registering key with multiple types!  FIX ME!!!!")
		return nil
	}
	return &ret.Duration
}

// Close this config framework's readers.  Config variable results are undefined after this call.
func (c *Config) Close() {
	c.varsMutex.Lock()
	defer c.varsMutex.Unlock()
	for _, backing := range c.readers {
		backing.Close()
	}
}

func (c *Config) register(key string, configVariable configVariable) configVariable {
	c.varsMutex.Lock()
	defer c.varsMutex.Unlock()
	existing, exists := c.registeredVars[key]
	if exists {
		// Don't log if everything else is the same?
		log.WithField("key", key).Warn("Possible race registering key")
		c.refresh(key, existing)
		return existing
	}
	dynamicOnPath := c.refresh(key, configVariable)
	if dynamicOnPath {
		c.watch(key, configVariable)
	}
	c.registeredVars[key] = configVariable
	return configVariable
}

func (c *Config) refresh(key string, configVar configVariable) bool {
	dynamicReadersOnPath := false
	for _, backing := range c.readers {
		if !dynamicReadersOnPath {
			_, ok := backing.(Dynamic)
			if ok {
				dynamicReadersOnPath = true
			}
		}

		v, e := backing.Get(key)
		if e != nil {
			log.WithField("err", e).Warn("Unable to read from backing")
			continue
		}
		if v != nil {
			e = configVar.Update(v)
			if e != nil {
				log.WithField("err", e).Error("Invalid config bytes for " + key)
			}
			return dynamicReadersOnPath
		}
	}

	e := configVar.Update(nil)
	if e != nil {
		log.WithField("err", e).Warn("Unable to set bytes to nil/clear")
	}

	// If this is false, then the variable is fixed and can never change
	return dynamicReadersOnPath
}

func (c *Config) watch(key string, configVar configVariable) {
	for _, backing := range c.readers {
		d, ok := backing.(Dynamic)
		if ok {
			err := d.Watch(key, c.onBackingChange)
			if err != nil {
				log.WithField("err", err).WithField("key", key).Warn("Unable to watch for config var")
			}
		}
	}
}

func (c *Config) onBackingChange(key string) {
	c.varsMutex.Lock()
	m, exists := c.registeredVars[key]
	c.varsMutex.Unlock()
	if !exists {
		log.WithField("key", key).Warn("Backing callback on variable that doesn't exist")
		return
	}
	c.refresh(key, m)
}

// Reader can get a []byte value for a config key
type Reader interface {
	Get(key string) ([]byte, error)
	Close()
}

// Writer can modify Config properties
type Writer interface {
	Write(key string, value []byte) error
}

type backingCallbackFunction func(string)

// A Dynamic config can change what it thinks a value is over time.
type Dynamic interface {
	Watch(key string, callback backingCallbackFunction) error
}

// A ReaderWriter can both read and write configuration information
type ReaderWriter interface {
	Reader
	Writer
}
