package distconf

import (
	"encoding/json"
	"expvar"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/signalfx/golib/v3/log"
	"github.com/signalfx/golib/v3/logkey"
)

// DefaultLogger is used by package structs that don't have a default logger set.
var DefaultLogger = log.Logger(log.DefaultLogger.CreateChild())

// Distconf gets configuration data from the first backing that has it
type Distconf struct {
	Logger  log.Logger
	readers []Reader

	varsMutex      sync.Mutex
	infoMutex      sync.RWMutex
	registeredVars map[string]*registeredVariableTracker
	distInfos      map[string]DistInfo
	callerFunc     func(int) (uintptr, string, int, bool)
}

type registeredVariableTracker struct {
	distvar        configVariable
	hasInitialized sync.Once
}

// New creates a Distconf from a list of backing readers
func New(readers []Reader) *Distconf {
	return &Distconf{
		Logger:         DefaultLogger,
		readers:        readers,
		registeredVars: make(map[string]*registeredVariableTracker),
		distInfos:      make(map[string]DistInfo),
	}
}

type configVariable interface {
	Update(newValue []byte) error
	// Get but on an interface return.  Oh how I miss you templates.
	GenericGet() interface{}
	GenericGetDefault() interface{}
	Type() DistType
}

type noopCloser struct{}

func (n *noopCloser) Close() {
	//nolint
	return
}

// DistType is used to type each of the DistInfos
type DistType int

const (
	// StrType is type Str
	StrType DistType = iota
	// BoolType is type Bool
	BoolType
	// FloatType is type Float
	FloatType
	// DurationType is type Duration
	DurationType
	// IntType is type Int
	IntType
)

// DistInfo is useful to unmarshal/marshal the Info expvar
type DistInfo struct {
	File         string      `json:"file"`
	Line         int         `json:"line"`
	DefaultValue interface{} `json:"default_value"`
	DistType     DistType    `json:"dist_type"`
}

func (c *Distconf) grabInfo(key string) {
	if c.callerFunc == nil {
		c.callerFunc = runtime.Caller
	}
	_, file, line, ok := c.callerFunc(2)
	if !ok {
		c.Logger.Log(logkey.DistconfKey, key, "unable to find call for distconf")
	}
	info := DistInfo{
		File: file,
		Line: line,
	}
	c.infoMutex.Lock()
	defer c.infoMutex.Unlock()
	c.distInfos[key] = info
}

// Var returns an expvar variable that shows all the current configuration variables and their
// current value
func (c *Distconf) Var() expvar.Var {
	return expvar.Func(func() interface{} {
		c.varsMutex.Lock()
		defer c.varsMutex.Unlock()

		m := make(map[string]interface{}, len(c.registeredVars))
		for name, v := range c.registeredVars {
			m[name] = v.distvar.GenericGet()
		}
		return m
	})
}

// Info returns an expvar variable that shows the information for all configuration variables.
// Information consist of file, line, default value and type of variable.
func (c *Distconf) Info() expvar.Var {
	return expvar.Func(func() interface{} {
		c.infoMutex.RLock()
		defer c.infoMutex.RUnlock()

		m := make(map[string]DistInfo, len(c.distInfos))
		for k, i := range c.distInfos {
			v, ok := c.registeredVars[k]
			if ok {
				v := DistInfo{
					File:         i.File,
					Line:         i.Line,
					DefaultValue: v.distvar.GenericGetDefault(),
					DistType:     v.distvar.Type(),
				}
				m[k] = v
			}
		}
		return m
	})
}

// Int object that can be referenced to get integer values from a backing config
func (c *Distconf) Int(key string, defaultVal int64) *Int {
	c.grabInfo(key)
	s := &intConf{
		defaultVal: defaultVal,
		Int: Int{
			currentVal: defaultVal,
		},
	}
	// Note: in race conditions 's' may not be the thing actually returned
	ret, okCast := c.createOrGet(key, s).(*intConf)
	if !okCast {
		c.Logger.Log(logkey.DistconfKey, key, "Registering key with multiple types!  FIX ME!!!!")
		return nil
	}
	return &ret.Int
}

// Float object that can be referenced to get float values from a backing config
func (c *Distconf) Float(key string, defaultVal float64) *Float {
	c.grabInfo(key)
	s := &floatConf{
		defaultVal: defaultVal,
		Float: Float{
			currentVal: math.Float64bits(defaultVal),
		},
	}
	// Note: in race conditions 's' may not be the thing actually returned
	ret, okCast := c.createOrGet(key, s).(*floatConf)
	if !okCast {
		c.Logger.Log(logkey.DistconfKey, key, "Registering key with multiple types!  FIX ME!!!!")
		return nil
	}
	return &ret.Float
}

// Str object that can be referenced to get string values from a backing config
func (c *Distconf) Str(key string, defaultVal string) *Str {
	c.grabInfo(key)
	s := &strConf{
		defaultVal: defaultVal,
	}
	s.currentVal.Store(defaultVal)
	// Note: in race conditions 's' may not be the thing actually returned
	ret, okCast := c.createOrGet(key, s).(*strConf)
	if !okCast {
		c.Logger.Log(logkey.DistconfKey, key, "Registering key with multiple types!  FIX ME!!!!")
		return nil
	}
	return &ret.Str
}

// Bool object that can be referenced to get boolean values from a backing config
func (c *Distconf) Bool(key string, defaultVal bool) *Bool {
	c.grabInfo(key)
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
	ret, okCast := c.createOrGet(key, s).(*boolConf)
	if !okCast {
		c.Logger.Log(logkey.DistconfKey, key, "Registering key with multiple types!  FIX ME!!!!")
		return nil
	}
	return &ret.Bool
}

// Duration returns a duration object that calls ParseDuration() on the given key
func (c *Distconf) Duration(key string, defaultVal time.Duration) *Duration {
	c.grabInfo(key)
	s := &durationConf{
		defaultVal: defaultVal,
		Duration: Duration{
			currentVal: defaultVal.Nanoseconds(),
		},
		logger: log.NewContext(c.Logger).With(logkey.DistconfKey, key),
	}
	// Note: in race conditions 's' may not be the thing actually returned
	ret, okCast := c.createOrGet(key, s).(*durationConf)
	if !okCast {
		c.Logger.Log(logkey.DistconfKey, key, "Registering key with multiple types!  FIX ME!!!!")
		return nil
	}
	return &ret.Duration
}

// Get retrieves the raw bytes for a given key from the first backing reader that has it.
// This is useful for accessing complex types like arrays and objects that cannot be
// represented by the typed methods (Int, Str, Bool, etc.).
// For YAML arrays/objects, the returned bytes will be JSON-encoded.
func (c *Distconf) Get(key string) ([]byte, error) {
	for _, reader := range c.readers {
		val, err := reader.Get(key)
		if err != nil {
			return nil, err
		}
		if val != nil {
			return val, nil
		}
	}
	return nil, nil
}

// GetStringSlice retrieves a string slice for the given key.
// The value can be either a YAML array or a JSON array string.
// Returns the default value if the key is not found or if parsing fails.
func (c *Distconf) GetStringSlice(key string, defaultVal []string) []string {
	bytes, err := c.Get(key)
	if err != nil || bytes == nil {
		return defaultVal
	}

	var result []string
	if err := json.Unmarshal(bytes, &result); err != nil {
		c.Logger.Log(logkey.DistconfKey, key, log.Err, err, "failed to unmarshal string slice")
		return defaultVal
	}
	return result
}

// GetIntSlice retrieves an int slice for the given key.
// The value can be either a YAML array or a JSON array string.
// Returns the default value if the key is not found or if parsing fails.
func (c *Distconf) GetIntSlice(key string, defaultVal []int) []int {
	bytes, err := c.Get(key)
	if err != nil || bytes == nil {
		return defaultVal
	}

	var result []int
	if err := json.Unmarshal(bytes, &result); err != nil {
		c.Logger.Log(logkey.DistconfKey, key, log.Err, err, "failed to unmarshal int slice")
		return defaultVal
	}
	return result
}

// GetStringMap retrieves a map[string]string for the given key.
// The value can be either a YAML map or a JSON object string.
// Returns the default value if the key is not found or if parsing fails.
func (c *Distconf) GetStringMap(key string, defaultVal map[string]string) map[string]string {
	bytes, err := c.Get(key)
	if err != nil || bytes == nil {
		return defaultVal
	}

	var result map[string]string
	if err := json.Unmarshal(bytes, &result); err != nil {
		c.Logger.Log(logkey.DistconfKey, key, log.Err, err, "failed to unmarshal string map")
		return defaultVal
	}
	return result
}

// GetStringBoolMap retrieves a map[string]bool for the given key.
// This is useful for org overrides like: '{"org1": true, "org2": false}'
// The value can be either a YAML map or a JSON object string.
// Returns the default value if the key is not found or if parsing fails.
func (c *Distconf) GetStringBoolMap(key string, defaultVal map[string]bool) map[string]bool {
	bytes, err := c.Get(key)
	if err != nil || bytes == nil {
		return defaultVal
	}

	var result map[string]bool
	if err := json.Unmarshal(bytes, &result); err != nil {
		c.Logger.Log(logkey.DistconfKey, key, log.Err, err, "failed to unmarshal string bool map")
		return defaultVal
	}
	return result
}

// GetStringIntMap retrieves a map[string]int for the given key.
// This is useful for org overrides with integer values like sample rates:
// '{"org1": 100, "org2": 50}'
// The value can be either a YAML map or a JSON object string.
// Returns the default value if the key is not found or if parsing fails.
func (c *Distconf) GetStringIntMap(key string, defaultVal map[string]int) map[string]int {
	bytes, err := c.Get(key)
	if err != nil || bytes == nil {
		return defaultVal
	}

	var result map[string]int
	if err := json.Unmarshal(bytes, &result); err != nil {
		c.Logger.Log(logkey.DistconfKey, key, log.Err, err, "failed to unmarshal string int map")
		return defaultVal
	}
	return result
}

// GetStringSliceMap retrieves a map[string][]string for the given key.
// This is useful for org overrides with string array values like blocked span tags:
// '{"ORG_ID": ["TAG1", "TAG2"]}'
// The value can be either a YAML map or a JSON object string.
// Returns the default value if the key is not found or if parsing fails.
func (c *Distconf) GetStringSliceMap(key string, defaultVal map[string][]string) map[string][]string {
	bytes, err := c.Get(key)
	if err != nil || bytes == nil {
		return defaultVal
	}

	var result map[string][]string
	if err := json.Unmarshal(bytes, &result); err != nil {
		c.Logger.Log(logkey.DistconfKey, key, log.Err, err, "failed to unmarshal string slice map")
		return defaultVal
	}
	return result
}

// GetNestedStringSliceMap retrieves a map[string]map[string][]string for the given key.
// This is useful for complex org overrides like blocked process tags:
// '{"G7qxWWeAAAU": {"sf_environment": ["psr-ai-lab0"]}}'
// The value can be either a YAML map or a JSON object string.
// Returns the default value if the key is not found or if parsing fails.
func (c *Distconf) GetNestedStringSliceMap(key string, defaultVal map[string]map[string][]string) map[string]map[string][]string {
	bytes, err := c.Get(key)
	if err != nil || bytes == nil {
		return defaultVal
	}

	var result map[string]map[string][]string
	if err := json.Unmarshal(bytes, &result); err != nil {
		c.Logger.Log(logkey.DistconfKey, key, log.Err, err, "failed to unmarshal nested string slice map")
		return defaultVal
	}
	return result
}

// GetJSON unmarshals the config value into the provided target interface.
// The value can be either a YAML object/array or a JSON string.
// Returns an error if the key is not found or if unmarshalling fails.
// Example usage:
//
//	var config MyConfigStruct
//	err := conf.GetJSON("my.config.key", &config)
func (c *Distconf) GetJSON(key string, target interface{}) error {
	bytes, err := c.Get(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return fmt.Errorf("key %s not found", key)
	}
	return json.Unmarshal(bytes, target)
}

// Close this config framework's readers.  Config variable results are undefined after this call.
func (c *Distconf) Close() {
	c.varsMutex.Lock()
	defer c.varsMutex.Unlock()
	for _, backing := range c.readers {
		backing.Close()
	}
}

func (c *Distconf) refresh(key string, configVar configVariable) bool {
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
			c.Logger.Log(logkey.DistconfKey, key, log.Err, e, "Unable to read from backing")
			continue
		}
		if v != nil {
			e = configVar.Update(v)
			if e != nil {
				c.Logger.Log(logkey.DistconfKey, key, log.Err, e, "Invalid config bytes")
			}
			return dynamicReadersOnPath
		}
	}

	if e := configVar.Update(nil); e != nil {
		c.Logger.Log(log.Err, e, "Unable to set bytes to nil/clear")
	}

	// If this is false, then the variable is fixed and can never change
	return dynamicReadersOnPath
}

func (c *Distconf) watch(key string, _ configVariable) {
	for _, backing := range c.readers {
		d, ok := backing.(Dynamic)
		if ok {
			err := d.Watch(key, c.onBackingChange)
			if err != nil {
				c.Logger.Log(logkey.DistconfKey, key, log.Err, err, "Unable to watch for config var")
			}
		}
	}
}

func (c *Distconf) createOrGet(key string, defaultVar configVariable) configVariable {
	c.varsMutex.Lock()
	rv, exists := c.registeredVars[key]
	if !exists {
		rv = &registeredVariableTracker{
			distvar: defaultVar,
		}
		c.registeredVars[key] = rv
	}
	c.varsMutex.Unlock()

	rv.hasInitialized.Do(func() {
		dynamicOnPath := c.refresh(key, rv.distvar)
		if dynamicOnPath {
			c.watch(key, rv.distvar)
		}
	})
	return rv.distvar
}

func (c *Distconf) onBackingChange(key string) {
	c.varsMutex.Lock()
	m, exists := c.registeredVars[key]
	c.varsMutex.Unlock()
	if !exists {
		c.Logger.Log(logkey.DistconfKey, key, "Backing callback on variable that doesn't exist")
		return
	}
	c.refresh(key, m.distvar)
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
