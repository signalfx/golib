package distconf

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/signalfx/golib/v3/log"
	"github.com/stretchr/testify/assert"
)

func TestInvalidYamlConfigPath(t *testing.T) {
	_, err := Yaml("/asdfdsaf/asdf/dsfad/sdfsa/fdsa/fasd/dsfa/sdfa/")
	assert.Error(t, err)
}

func TestYamlConf(t *testing.T) {
	// Create a temporary YAML file
	file, err := ioutil.TempFile("", "TestYamlConf")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(`val1: abc`), 0))

	y, err := YamlLoader(file.Name()).Get()
	assert.NoError(t, err)

	b, err := y.Get("some_non_existent_key")
	assert.NoError(t, err)
	assert.Nil(t, b)

	v, err := y.Get("val1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("abc"), v)
}

func TestYamlCallbackMapAdd(t *testing.T) {
	m := yamlCallbackMap{
		callbacks: make(map[string][]backingCallbackFunction),
	}

	callCount := 0
	callback := func(key string) {
		callCount++
	}

	// Test adding a callback
	m.add("key1", callback)
	assert.Equal(t, 1, len(m.callbacks["key1"]))

	// Test adding multiple callbacks to the same key
	m.add("key1", callback)
	assert.Equal(t, 2, len(m.callbacks["key1"]))

	// Test adding callback to a different key
	m.add("key2", callback)
	assert.Equal(t, 1, len(m.callbacks["key2"]))
	assert.Equal(t, 2, len(m.callbacks["key1"]))
}

func TestYamlCallbackMapCopy(t *testing.T) {
	m := yamlCallbackMap{
		callbacks: make(map[string][]backingCallbackFunction),
	}

	callCount := 0
	callback := func(key string) {
		callCount++
	}

	m.add("key1", callback)
	m.add("key1", callback)
	m.add("key2", callback)

	// Test copy returns correct data
	copied := m.copy()
	assert.Equal(t, 2, len(copied["key1"]))
	assert.Equal(t, 1, len(copied["key2"]))

	// Test that copy is independent (modifying original doesn't affect copy)
	m.add("key1", callback)
	assert.Equal(t, 3, len(m.callbacks["key1"]))
	assert.Equal(t, 2, len(copied["key1"])) // Copy should still have 2
}

func TestYamlCallbackMapCopyEmpty(t *testing.T) {
	m := yamlCallbackMap{
		callbacks: make(map[string][]backingCallbackFunction),
	}

	copied := m.copy()
	assert.NotNil(t, copied)
	assert.Equal(t, 0, len(copied))
}

func TestYamlCallbackMapConcurrency(t *testing.T) {
	m := yamlCallbackMap{
		callbacks: make(map[string][]backingCallbackFunction),
	}

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrently add callbacks
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.add("key1", func(key string) {})
		}()
	}

	// Concurrently copy while adding
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.copy()
		}()
	}

	wg.Wait()

	// Verify all callbacks were added
	assert.Equal(t, numGoroutines, len(m.callbacks["key1"]))
}

func TestYamlImplementsDynamic(t *testing.T) {
	// Create a temporary YAML file
	file, err := ioutil.TempFile("", "TestYamlImplementsDynamic")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(`val1: abc`), 0))

	y, err := Yaml(file.Name())
	assert.NoError(t, err)

	// Verify that yamlFileDisco implements the Dynamic interface
	dynamicReader, ok := y.(Dynamic)
	assert.True(t, ok, "yamlFileDisco should implement Dynamic interface")
	assert.NotNil(t, dynamicReader)
}

func TestYamlWatch(t *testing.T) {
	// Create a temporary YAML file
	file, err := ioutil.TempFile("", "TestYamlWatch")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(`val1: abc`), 0))

	y, err := Yaml(file.Name())
	assert.NoError(t, err)

	dynamicReader := y.(Dynamic)

	var callCount int32
	callback := func(key string) {
		atomic.AddInt32(&callCount, 1)
	}

	// Test Watch returns no error
	err = dynamicReader.Watch("val1", callback)
	assert.NoError(t, err)

	// Verify callback was registered by checking internal state
	yamlDisco := y.(*yamlFileDisco)
	callbacks := yamlDisco.callbacks.copy()
	assert.Equal(t, 1, len(callbacks["val1"]))
}

func TestYamlWatchMultipleCallbacks(t *testing.T) {
	// Create a temporary YAML file
	file, err := ioutil.TempFile("", "TestYamlWatchMultiple")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(`val1: abc
val2: def`), 0))

	y, err := Yaml(file.Name())
	assert.NoError(t, err)

	dynamicReader := y.(Dynamic)

	callback1 := func(key string) {}
	callback2 := func(key string) {}

	// Register multiple callbacks for same key
	err = dynamicReader.Watch("val1", callback1)
	assert.NoError(t, err)
	err = dynamicReader.Watch("val1", callback2)
	assert.NoError(t, err)

	// Register callback for different key
	err = dynamicReader.Watch("val2", callback1)
	assert.NoError(t, err)

	// Verify callbacks were registered
	yamlDisco := y.(*yamlFileDisco)
	callbacks := yamlDisco.callbacks.copy()
	assert.Equal(t, 2, len(callbacks["val1"]))
	assert.Equal(t, 1, len(callbacks["val2"]))
}

func TestYamlFileChangeTriggersCallbacks(t *testing.T) {
	// Create a temporary YAML file with proper extension
	tmpDir, err := ioutil.TempDir("", "yamltest")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.RemoveAll(tmpDir))
	}()

	filename := tmpDir + "/config.yaml"
	assert.NoError(t, ioutil.WriteFile(filename, []byte(`val1: abc`), 0o600))

	y, err := Yaml(filename)
	assert.NoError(t, err)

	dynamicReader := y.(Dynamic)

	var callbackInvoked int32
	var receivedKey string
	var mu sync.Mutex

	callback := func(key string) {
		mu.Lock()
		receivedKey = key
		mu.Unlock()
		atomic.AddInt32(&callbackInvoked, 1)
	}

	err = dynamicReader.Watch("val1", callback)
	assert.NoError(t, err)

	// Give file watcher time to initialize
	time.Sleep(200 * time.Millisecond)

	// Modify the file to trigger the callback - use truncate and write to ensure change is detected
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC, 0o600)
	assert.NoError(t, err)
	_, err = f.WriteString(`val1: xyz`)
	assert.NoError(t, err)
	assert.NoError(t, f.Sync())
	assert.NoError(t, f.Close())

	// Wait for file watcher to detect change and trigger callback
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&callbackInvoked) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify callback was invoked
	assert.Greater(t, atomic.LoadInt32(&callbackInvoked), int32(0), "Callback should have been invoked on file change")

	mu.Lock()
	assert.Equal(t, "val1", receivedKey, "Callback should receive the correct key")
	mu.Unlock()

	// Verify the new value is readable
	val, err := y.Get("val1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("xyz"), val)
}

func TestYamlDistconfIntegration(t *testing.T) {
	// Create a temporary YAML file with proper extension
	tmpDir, err := ioutil.TempDir("", "yamltest")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.RemoveAll(tmpDir))
	}()

	filename := tmpDir + "/config.yaml"
	assert.NoError(t, ioutil.WriteFile(filename, []byte(`testkey: initialvalue`), 0o600))

	// Create distconf with YAML backing
	backs := []BackingLoader{YamlLoader(filename)}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Get a string config value
	strVal := conf.Str("testkey", "default")
	assert.Equal(t, "initialvalue", strVal.Get())

	// Give file watcher time to initialize
	time.Sleep(200 * time.Millisecond)

	// Modify the file - use truncate and write to ensure change is detected
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC, 0o600)
	assert.NoError(t, err)
	_, err = f.WriteString(`testkey: newvalue`)
	assert.NoError(t, err)
	assert.NoError(t, f.Sync())
	assert.NoError(t, f.Close())

	// Wait for file watcher to detect change and update the value
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strVal.Get() == "newvalue" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify the value was updated
	assert.Equal(t, "newvalue", strVal.Get(), "distconf should reflect the updated YAML value")
}

func TestYamlArrays(t *testing.T) {
	// Create a temporary YAML file with an array
	file, err := ioutil.TempFile("", "TestYamlArrays")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	yamlContent := `
items:
  - item1
  - item2
  - item3
numbers:
  - 1
  - 2
  - 3
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	y, err := YamlLoader(file.Name()).Get()
	assert.NoError(t, err)

	// Test string array
	v, err := y.Get("items")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	var items []string
	err = json.Unmarshal(v, &items)
	assert.NoError(t, err)
	assert.Equal(t, []string{"item1", "item2", "item3"}, items)

	// Test integer array
	v, err = y.Get("numbers")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	var numbers []int
	err = json.Unmarshal(v, &numbers)
	assert.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3}, numbers)
}

func TestYamlObjects(t *testing.T) {
	// Create a temporary YAML file with nested objects
	file, err := ioutil.TempFile("", "TestYamlObjects")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	yamlContent := `
database:
  host: localhost
  port: 5432
  name: mydb
server:
  address: 0.0.0.0
  ports:
    - 8080
    - 8443
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	y, err := YamlLoader(file.Name()).Get()
	assert.NoError(t, err)

	// Test nested object
	v, err := y.Get("database")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	var db map[string]interface{}
	err = json.Unmarshal(v, &db)
	assert.NoError(t, err)
	assert.Equal(t, "localhost", db["host"])
	assert.Equal(t, float64(5432), db["port"]) // JSON numbers are float64
	assert.Equal(t, "mydb", db["name"])

	// Test object with nested array
	v, err = y.Get("server")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	var server map[string]interface{}
	err = json.Unmarshal(v, &server)
	assert.NoError(t, err)
	assert.Equal(t, "0.0.0.0", server["address"])

	ports := server["ports"].([]interface{})
	assert.Equal(t, float64(8080), ports[0])
	assert.Equal(t, float64(8443), ports[1])
}

func TestYamlEmptyString(t *testing.T) {
	// Create a temporary YAML file with an empty string value
	file, err := ioutil.TempFile("", "TestYamlEmptyString")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	yamlContent := `
empty_value: ""
non_empty: "hello"
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	y, err := YamlLoader(file.Name()).Get()
	assert.NoError(t, err)

	// Empty string should return nil
	v, err := y.Get("empty_value")
	assert.NoError(t, err)
	assert.Nil(t, v)

	// Non-empty string should return the value
	v, err = y.Get("non_empty")
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), v)
}

func TestDistconfGetArraysAndObjects(t *testing.T) {
	// Create a temporary YAML file with arrays and objects
	file, err := ioutil.TempFile("", "TestDistconfGetArraysAndObjects")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	yamlContent := `
string_val: hello
items:
  - item1
  - item2
  - item3
database:
  host: localhost
  port: 5432
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	// Use FromLoaders to create Distconf (the typical usage pattern)
	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Test that typed methods still work
	strVal := conf.Str("string_val", "default")
	assert.Equal(t, "hello", strVal.Get())

	// Test Get() for string value
	v, err := conf.Get("string_val")
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), v)

	// Test Get() for array
	v, err = conf.Get("items")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	var items []string
	err = json.Unmarshal(v, &items)
	assert.NoError(t, err)
	assert.Equal(t, []string{"item1", "item2", "item3"}, items)

	// Test Get() for object
	v, err = conf.Get("database")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	var db map[string]interface{}
	err = json.Unmarshal(v, &db)
	assert.NoError(t, err)
	assert.Equal(t, "localhost", db["host"])
	assert.Equal(t, float64(5432), db["port"])

	// Test Get() for non-existent key
	v, err = conf.Get("non_existent_key")
	assert.NoError(t, err)
	assert.Nil(t, v)
}

func TestYamlStringContainingJSON(t *testing.T) {
	// Test case: YAML value is a string that contains JSON
	// This is a common pattern where users store JSON as a string in YAML config
	file, err := ioutil.TempFile("", "TestYamlStringContainingJSON")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())

	// This mimics the user's use case:
	// sf.trace-ingest.filebus.enabled_org_overrides: "{\"BqDQY5OAAAA\": true, ...}"
	yamlContent := `
simple_string: hello
json_object_string: '{"BqDQY5OAAAA": true, "G7qxWWeAAAU": true, "EauzO4EAIAA": true}'
json_array_string: '["item1", "item2", "item3"]'
nested:
  config:
    enabled_org_overrides: '{"org1": true, "org2": false, "org3": true}'
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	// Use FromLoaders to create Distconf (typical usage pattern)
	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Test 1: Simple string still works
	v, err := conf.Get("simple_string")
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), v)

	// Test 2: JSON object stored as string - should return raw string
	v, err = conf.Get("json_object_string")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	// The value should be the raw JSON string, which users can unmarshal themselves
	rawString := string(v)
	assert.Equal(t, `{"BqDQY5OAAAA": true, "G7qxWWeAAAU": true, "EauzO4EAIAA": true}`, rawString)

	// User can manually unmarshal this JSON string
	var orgOverrides map[string]bool
	err = json.Unmarshal(v, &orgOverrides)
	assert.NoError(t, err)
	assert.True(t, orgOverrides["BqDQY5OAAAA"])
	assert.True(t, orgOverrides["G7qxWWeAAAU"])
	assert.True(t, orgOverrides["EauzO4EAIAA"])

	// Test 3: JSON array stored as string - should return raw string
	v, err = conf.Get("json_array_string")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	rawString = string(v)
	assert.Equal(t, `["item1", "item2", "item3"]`, rawString)

	// User can manually unmarshal this JSON array
	var items []string
	err = json.Unmarshal(v, &items)
	assert.NoError(t, err)
	assert.Equal(t, []string{"item1", "item2", "item3"}, items)

	// Test 4: Nested config with JSON string value
	v, err = conf.Get("nested.config.enabled_org_overrides")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	rawString = string(v)
	assert.Equal(t, `{"org1": true, "org2": false, "org3": true}`, rawString)

	// User can manually unmarshal
	var nestedOverrides map[string]bool
	err = json.Unmarshal(v, &nestedOverrides)
	assert.NoError(t, err)
	assert.True(t, nestedOverrides["org1"])
	assert.False(t, nestedOverrides["org2"])
	assert.True(t, nestedOverrides["org3"])

	// Test 5: Also works with Str() method for backward compatibility
	strVal := conf.Str("json_object_string", "")
	rawString = strVal.Get()
	assert.Equal(t, `{"BqDQY5OAAAA": true, "G7qxWWeAAAU": true, "EauzO4EAIAA": true}`, rawString)
}

func TestDistconfConvenienceMethods(t *testing.T) {
	// Test the new convenience methods that handle unmarshalling automatically
	file, err := ioutil.TempFile("", "TestDistconfConvenienceMethods")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())

	yamlContent := `
# YAML native arrays
string_items:
  - item1
  - item2
  - item3
int_items:
  - 1
  - 2
  - 3

# YAML native maps
string_map:
  key1: value1
  key2: value2

# JSON strings (user's use case for org overrides)
org_overrides_json: '{"BqDQY5OAAAA": true, "G7qxWWeAAAU": true, "EauzO4EAIAA": false}'
items_json: '["a", "b", "c"]'

# JSON string for sample rate org overrides (map[string]int)
sample_rate_overrides_json: '{"BqDQY5OAAAA": 100, "G7qxWWeAAAU": 50, "EauzO4EAIAA": 25}'

# JSON string for blocked span tags (map[string][]string)
blocked_span_tags_json: '{"ORG_ID1": ["TAG1", "TAG2"], "ORG_ID2": ["TAG3"]}'

# JSON string for blocked process tags (map[string]map[string][]string)
blocked_process_tags_json: '{"G7qxWWeAAAU": {"sf_environment": ["psr-ai-lab0"]}, "BqDQY5OAAAA": {"sf_service": ["svc1", "svc2"], "sf_environment": ["prod"]}}'

# Nested config
sf:
  trace-ingest:
    filebus:
      enabled_org_overrides: '{"org1": true, "org2": false}'
      ai_spans_sample_rate_org_overrides: '{"BqDQY5OAAAA": 100, "G7qxWWeAAAU": 100, "EauzO4EAIAA": 100, "G_yXEGYAIAI": 100}'
    spanbus:
      blocked_process_tags_org_overrides: '{"G7qxWWeAAAU": {"sf_environment": ["psr-ai-lab0"]}}'
      blocked_span_tags: '{"ORG_ID": ["TAG1", "TAG2"]}'
  trace_ingest:
    spanbus:
      blocked_process_tags: '{"ORG_ID": {"TAG_KEY": ["TAG_VALUE1", "TAG_VALUE2"]}}'

# For GetJSON with struct
server_config: '{"host": "localhost", "port": 8080, "enabled": true}'
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Test GetStringSlice with YAML array
	items := conf.GetStringSlice("string_items", nil)
	assert.Equal(t, []string{"item1", "item2", "item3"}, items)

	// Test GetStringSlice with JSON string
	itemsFromJSON := conf.GetStringSlice("items_json", nil)
	assert.Equal(t, []string{"a", "b", "c"}, itemsFromJSON)

	// Test GetStringSlice with non-existent key (returns default)
	defaultItems := conf.GetStringSlice("non_existent", []string{"default"})
	assert.Equal(t, []string{"default"}, defaultItems)

	// Test GetIntSlice with YAML array
	nums := conf.GetIntSlice("int_items", nil)
	assert.Equal(t, []int{1, 2, 3}, nums)

	// Test GetIntSlice with non-existent key (returns default)
	defaultNums := conf.GetIntSlice("non_existent", []int{0})
	assert.Equal(t, []int{0}, defaultNums)

	// Test GetStringMap with YAML map
	strMap := conf.GetStringMap("string_map", nil)
	assert.Equal(t, "value1", strMap["key1"])
	assert.Equal(t, "value2", strMap["key2"])

	// Test GetStringBoolMap with JSON string (the main user use case!)
	orgOverrides := conf.GetStringBoolMap("org_overrides_json", nil)
	assert.True(t, orgOverrides["BqDQY5OAAAA"])
	assert.True(t, orgOverrides["G7qxWWeAAAU"])
	assert.False(t, orgOverrides["EauzO4EAIAA"])

	// Test GetStringBoolMap with nested key
	nestedOverrides := conf.GetStringBoolMap("sf.trace-ingest.filebus.enabled_org_overrides", nil)
	assert.True(t, nestedOverrides["org1"])
	assert.False(t, nestedOverrides["org2"])

	// Test GetStringBoolMap with non-existent key (returns default)
	defaultMap := conf.GetStringBoolMap("non_existent", map[string]bool{"default": true})
	assert.True(t, defaultMap["default"])

	// Test GetStringIntMap with JSON string (sample rate use case!)
	sampleRates := conf.GetStringIntMap("sample_rate_overrides_json", nil)
	assert.Equal(t, 100, sampleRates["BqDQY5OAAAA"])
	assert.Equal(t, 50, sampleRates["G7qxWWeAAAU"])
	assert.Equal(t, 25, sampleRates["EauzO4EAIAA"])

	// Test GetStringIntMap with nested key (the exact user use case!)
	aiSampleRates := conf.GetStringIntMap("sf.trace-ingest.filebus.ai_spans_sample_rate_org_overrides", nil)
	assert.Equal(t, 100, aiSampleRates["BqDQY5OAAAA"])
	assert.Equal(t, 100, aiSampleRates["G7qxWWeAAAU"])
	assert.Equal(t, 100, aiSampleRates["EauzO4EAIAA"])
	assert.Equal(t, 100, aiSampleRates["G_yXEGYAIAI"])

	// Test GetStringIntMap with non-existent key (returns default)
	defaultIntMap := conf.GetStringIntMap("non_existent", map[string]int{"default": 42})
	assert.Equal(t, 42, defaultIntMap["default"])

	// Test GetStringSliceMap with JSON string (blocked span tags use case!)
	blockedSpanTags := conf.GetStringSliceMap("blocked_span_tags_json", nil)
	assert.NotNil(t, blockedSpanTags)
	assert.Equal(t, []string{"TAG1", "TAG2"}, blockedSpanTags["ORG_ID1"])
	assert.Equal(t, []string{"TAG3"}, blockedSpanTags["ORG_ID2"])

	// Test GetStringSliceMap with nested key (the exact user use case!)
	nestedBlockedSpanTags := conf.GetStringSliceMap("sf.trace-ingest.spanbus.blocked_span_tags", nil)
	assert.NotNil(t, nestedBlockedSpanTags)
	assert.Equal(t, []string{"TAG1", "TAG2"}, nestedBlockedSpanTags["ORG_ID"])

	// Test GetStringSliceMap with non-existent key (returns default)
	defaultSliceMap := conf.GetStringSliceMap("non_existent", map[string][]string{"org1": {"tag1"}})
	assert.Equal(t, []string{"tag1"}, defaultSliceMap["org1"])

	// Test GetNestedStringSliceMap with JSON string (blocked process tags use case!)
	blockedTags := conf.GetNestedStringSliceMap("blocked_process_tags_json", nil)
	assert.NotNil(t, blockedTags)
	assert.Equal(t, []string{"psr-ai-lab0"}, blockedTags["G7qxWWeAAAU"]["sf_environment"])
	assert.Equal(t, []string{"svc1", "svc2"}, blockedTags["BqDQY5OAAAA"]["sf_service"])
	assert.Equal(t, []string{"prod"}, blockedTags["BqDQY5OAAAA"]["sf_environment"])

	// Test GetNestedStringSliceMap with nested key (the exact user use case!)
	nestedBlockedTags := conf.GetNestedStringSliceMap("sf.trace-ingest.spanbus.blocked_process_tags_org_overrides", nil)
	assert.NotNil(t, nestedBlockedTags)
	assert.Equal(t, []string{"psr-ai-lab0"}, nestedBlockedTags["G7qxWWeAAAU"]["sf_environment"])

	// Test GetNestedStringSliceMap with blocked_process_tags format: {"ORG_ID": {"TAG_KEY": ["TAG_VALUE1", "TAG_VALUE2"]}}
	blockedProcessTags := conf.GetNestedStringSliceMap("sf.trace_ingest.spanbus.blocked_process_tags", nil)
	assert.NotNil(t, blockedProcessTags)
	assert.Equal(t, []string{"TAG_VALUE1", "TAG_VALUE2"}, blockedProcessTags["ORG_ID"]["TAG_KEY"])

	// Test GetNestedStringSliceMap with non-existent key (returns default)
	defaultNestedMap := conf.GetNestedStringSliceMap("non_existent", map[string]map[string][]string{"org1": {"tag1": {"val1"}}})
	assert.Equal(t, []string{"val1"}, defaultNestedMap["org1"]["tag1"])

	// Test GetJSON with a custom struct
	type ServerConfig struct {
		Host    string `json:"host"`
		Port    int    `json:"port"`
		Enabled bool   `json:"enabled"`
	}
	var serverCfg ServerConfig
	err = conf.GetJSON("server_config", &serverCfg)
	assert.NoError(t, err)
	assert.Equal(t, "localhost", serverCfg.Host)
	assert.Equal(t, 8080, serverCfg.Port)
	assert.True(t, serverCfg.Enabled)

	// Test GetJSON with non-existent key (returns error)
	var emptyCfg ServerConfig
	err = conf.GetJSON("non_existent_key", &emptyCfg)
	assert.Error(t, err)
}

func TestGetStringSliceEdgeCases(t *testing.T) {
	file, err := ioutil.TempFile("", "TestGetStringSliceEdgeCases")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())

	yamlContent := `
empty_array: '[]'
single_item: '["only_one"]'
with_spaces: '["item with spaces", "another item"]'
invalid_json: 'not valid json'
wrong_type: '{"key": "value"}'
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Test empty array
	empty := conf.GetStringSlice("empty_array", []string{"default"})
	assert.Equal(t, []string{}, empty)

	// Test single item array
	single := conf.GetStringSlice("single_item", nil)
	assert.Equal(t, []string{"only_one"}, single)

	// Test array with spaces in values
	withSpaces := conf.GetStringSlice("with_spaces", nil)
	assert.Equal(t, []string{"item with spaces", "another item"}, withSpaces)

	// Test invalid JSON returns default
	invalid := conf.GetStringSlice("invalid_json", []string{"fallback"})
	assert.Equal(t, []string{"fallback"}, invalid)

	// Test wrong type (object instead of array) returns default
	wrongType := conf.GetStringSlice("wrong_type", []string{"fallback"})
	assert.Equal(t, []string{"fallback"}, wrongType)

	// Test nil default
	nilDefault := conf.GetStringSlice("non_existent", nil)
	assert.Nil(t, nilDefault)
}

func TestGetIntSliceEdgeCases(t *testing.T) {
	file, err := ioutil.TempFile("", "TestGetIntSliceEdgeCases")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())

	yamlContent := `
empty_array: '[]'
single_item: '[42]'
negative_numbers: '[-1, -100, 0, 100]'
invalid_json: 'not valid json'
wrong_type: '["string", "values"]'
floats_truncated: '[1.9, 2.1, 3.5]'
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Test empty array
	empty := conf.GetIntSlice("empty_array", []int{999})
	assert.Equal(t, []int{}, empty)

	// Test single item
	single := conf.GetIntSlice("single_item", nil)
	assert.Equal(t, []int{42}, single)

	// Test negative numbers
	negative := conf.GetIntSlice("negative_numbers", nil)
	assert.Equal(t, []int{-1, -100, 0, 100}, negative)

	// Test invalid JSON returns default
	invalid := conf.GetIntSlice("invalid_json", []int{-1})
	assert.Equal(t, []int{-1}, invalid)

	// Test wrong type returns default
	wrongType := conf.GetIntSlice("wrong_type", []int{-1})
	assert.Equal(t, []int{-1}, wrongType)

	// Test nil default
	nilDefault := conf.GetIntSlice("non_existent", nil)
	assert.Nil(t, nilDefault)
}

func TestGetStringMapEdgeCases(t *testing.T) {
	file, err := ioutil.TempFile("", "TestGetStringMapEdgeCases")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())

	yamlContent := `
empty_map: '{}'
single_entry: '{"key": "value"}'
special_chars: '{"key-with-dash": "value_with_underscore", "key.with.dot": "value/with/slash"}'
invalid_json: 'not valid json'
wrong_type: '["array", "not", "map"]'
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Test empty map
	empty := conf.GetStringMap("empty_map", map[string]string{"default": "value"})
	assert.Equal(t, map[string]string{}, empty)

	// Test single entry
	single := conf.GetStringMap("single_entry", nil)
	assert.Equal(t, "value", single["key"])

	// Test special characters in keys and values
	special := conf.GetStringMap("special_chars", nil)
	assert.Equal(t, "value_with_underscore", special["key-with-dash"])
	assert.Equal(t, "value/with/slash", special["key.with.dot"])

	// Test invalid JSON returns default
	invalid := conf.GetStringMap("invalid_json", map[string]string{"fallback": "value"})
	assert.Equal(t, "value", invalid["fallback"])

	// Test wrong type returns default
	wrongType := conf.GetStringMap("wrong_type", map[string]string{"fallback": "value"})
	assert.Equal(t, "value", wrongType["fallback"])

	// Test nil default
	nilDefault := conf.GetStringMap("non_existent", nil)
	assert.Nil(t, nilDefault)
}

func TestGetStringBoolMapEdgeCases(t *testing.T) {
	file, err := ioutil.TempFile("", "TestGetStringBoolMapEdgeCases")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())

	yamlContent := `
empty_map: '{}'
all_true: '{"org1": true, "org2": true}'
all_false: '{"org1": false, "org2": false}'
mixed: '{"enabled": true, "disabled": false}'
invalid_json: 'not valid json'
wrong_type: '{"key": "string_not_bool"}'
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Test empty map
	empty := conf.GetStringBoolMap("empty_map", map[string]bool{"default": true})
	assert.Equal(t, map[string]bool{}, empty)

	// Test all true
	allTrue := conf.GetStringBoolMap("all_true", nil)
	assert.True(t, allTrue["org1"])
	assert.True(t, allTrue["org2"])

	// Test all false
	allFalse := conf.GetStringBoolMap("all_false", nil)
	assert.False(t, allFalse["org1"])
	assert.False(t, allFalse["org2"])

	// Test mixed values
	mixed := conf.GetStringBoolMap("mixed", nil)
	assert.True(t, mixed["enabled"])
	assert.False(t, mixed["disabled"])

	// Test invalid JSON returns default
	invalid := conf.GetStringBoolMap("invalid_json", map[string]bool{"fallback": true})
	assert.True(t, invalid["fallback"])

	// Test wrong type returns default
	wrongType := conf.GetStringBoolMap("wrong_type", map[string]bool{"fallback": false})
	assert.False(t, wrongType["fallback"])

	// Test nil default
	nilDefault := conf.GetStringBoolMap("non_existent", nil)
	assert.Nil(t, nilDefault)
}

func TestGetStringIntMapEdgeCases(t *testing.T) {
	file, err := ioutil.TempFile("", "TestGetStringIntMapEdgeCases")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())

	yamlContent := `
empty_map: '{}'
zero_values: '{"org1": 0, "org2": 0}'
negative_values: '{"org1": -100, "org2": -1}'
large_values: '{"org1": 1000000, "org2": 999999}'
invalid_json: 'not valid json'
wrong_type: '{"key": "string_not_int"}'
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Test empty map
	empty := conf.GetStringIntMap("empty_map", map[string]int{"default": 42})
	assert.Equal(t, map[string]int{}, empty)

	// Test zero values
	zeros := conf.GetStringIntMap("zero_values", nil)
	assert.Equal(t, 0, zeros["org1"])
	assert.Equal(t, 0, zeros["org2"])

	// Test negative values
	negative := conf.GetStringIntMap("negative_values", nil)
	assert.Equal(t, -100, negative["org1"])
	assert.Equal(t, -1, negative["org2"])

	// Test large values
	large := conf.GetStringIntMap("large_values", nil)
	assert.Equal(t, 1000000, large["org1"])
	assert.Equal(t, 999999, large["org2"])

	// Test invalid JSON returns default
	invalid := conf.GetStringIntMap("invalid_json", map[string]int{"fallback": -1})
	assert.Equal(t, -1, invalid["fallback"])

	// Test wrong type returns default
	wrongType := conf.GetStringIntMap("wrong_type", map[string]int{"fallback": -1})
	assert.Equal(t, -1, wrongType["fallback"])

	// Test nil default
	nilDefault := conf.GetStringIntMap("non_existent", nil)
	assert.Nil(t, nilDefault)
}

func TestGetStringSliceMapEdgeCases(t *testing.T) {
	file, err := ioutil.TempFile("", "TestGetStringSliceMapEdgeCases")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())

	yamlContent := `
empty_map: '{}'
single_org: '{"ORG1": ["tag1"]}'
multiple_tags: '{"ORG1": ["tag1", "tag2", "tag3"]}'
multiple_orgs: '{"ORG1": ["tag1"], "ORG2": ["tag2", "tag3"]}'
empty_tags: '{"ORG1": []}'
special_chars: '{"ORG-WITH-DASH": ["tag_with_underscore", "tag.with.dot"]}'
invalid_json: 'not valid json'
wrong_type: '["array", "not", "map"]'
wrong_value_type: '{"ORG1": "string_not_array"}'
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Test empty map
	empty := conf.GetStringSliceMap("empty_map", map[string][]string{"default": {"val"}})
	assert.Equal(t, map[string][]string{}, empty)

	// Test single org with single tag
	singleOrg := conf.GetStringSliceMap("single_org", nil)
	assert.Equal(t, []string{"tag1"}, singleOrg["ORG1"])

	// Test single org with multiple tags
	multipleTags := conf.GetStringSliceMap("multiple_tags", nil)
	assert.Equal(t, []string{"tag1", "tag2", "tag3"}, multipleTags["ORG1"])

	// Test multiple orgs
	multipleOrgs := conf.GetStringSliceMap("multiple_orgs", nil)
	assert.Equal(t, []string{"tag1"}, multipleOrgs["ORG1"])
	assert.Equal(t, []string{"tag2", "tag3"}, multipleOrgs["ORG2"])

	// Test empty tags array
	emptyTags := conf.GetStringSliceMap("empty_tags", nil)
	assert.Equal(t, []string{}, emptyTags["ORG1"])

	// Test special characters in keys and values
	special := conf.GetStringSliceMap("special_chars", nil)
	assert.Equal(t, []string{"tag_with_underscore", "tag.with.dot"}, special["ORG-WITH-DASH"])

	// Test invalid JSON returns default
	invalid := conf.GetStringSliceMap("invalid_json", map[string][]string{"fallback": {"val"}})
	assert.Equal(t, []string{"val"}, invalid["fallback"])

	// Test wrong type returns default
	wrongType := conf.GetStringSliceMap("wrong_type", map[string][]string{"fallback": {"val"}})
	assert.Equal(t, []string{"val"}, wrongType["fallback"])

	// Test wrong value type returns default
	wrongValueType := conf.GetStringSliceMap("wrong_value_type", map[string][]string{"fallback": {"val"}})
	assert.Equal(t, []string{"val"}, wrongValueType["fallback"])

	// Test nil default
	nilDefault := conf.GetStringSliceMap("non_existent", nil)
	assert.Nil(t, nilDefault)
}

func TestGetNestedStringSliceMapEdgeCases(t *testing.T) {
	file, err := ioutil.TempFile("", "TestGetNestedStringSliceMapEdgeCases")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())

	yamlContent := `
empty_map: '{}'
empty_nested: '{"org1": {}}'
empty_array: '{"org1": {"tag1": []}}'
multiple_orgs: '{"org1": {"tag1": ["val1"]}, "org2": {"tag2": ["val2", "val3"]}}'
multiple_tags: '{"org1": {"tag1": ["a", "b"], "tag2": ["c", "d", "e"]}}'
invalid_json: 'not valid json'
wrong_type: '["array", "not", "map"]'
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Test empty map
	empty := conf.GetNestedStringSliceMap("empty_map", map[string]map[string][]string{"default": {"tag": {"val"}}})
	assert.Equal(t, map[string]map[string][]string{}, empty)

	// Test empty nested map
	emptyNested := conf.GetNestedStringSliceMap("empty_nested", nil)
	assert.NotNil(t, emptyNested["org1"])
	assert.Equal(t, map[string][]string{}, emptyNested["org1"])

	// Test empty array value
	emptyArray := conf.GetNestedStringSliceMap("empty_array", nil)
	assert.Equal(t, []string{}, emptyArray["org1"]["tag1"])

	// Test multiple orgs
	multiOrgs := conf.GetNestedStringSliceMap("multiple_orgs", nil)
	assert.Equal(t, []string{"val1"}, multiOrgs["org1"]["tag1"])
	assert.Equal(t, []string{"val2", "val3"}, multiOrgs["org2"]["tag2"])

	// Test multiple tags per org
	multiTags := conf.GetNestedStringSliceMap("multiple_tags", nil)
	assert.Equal(t, []string{"a", "b"}, multiTags["org1"]["tag1"])
	assert.Equal(t, []string{"c", "d", "e"}, multiTags["org1"]["tag2"])

	// Test invalid JSON returns default
	invalid := conf.GetNestedStringSliceMap("invalid_json", map[string]map[string][]string{"fallback": {"tag": {"val"}}})
	assert.Equal(t, []string{"val"}, invalid["fallback"]["tag"])

	// Test wrong type returns default
	wrongType := conf.GetNestedStringSliceMap("wrong_type", map[string]map[string][]string{"fallback": {"tag": {"val"}}})
	assert.Equal(t, []string{"val"}, wrongType["fallback"]["tag"])

	// Test nil default
	nilDefault := conf.GetNestedStringSliceMap("non_existent", nil)
	assert.Nil(t, nilDefault)
}

func TestGetJSONEdgeCases(t *testing.T) {
	file, err := ioutil.TempFile("", "TestGetJSONEdgeCases")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())

	yamlContent := `
simple_struct: '{"name": "test", "count": 42}'
nested_struct: '{"outer": {"inner": {"value": "deep"}}}'
array_of_structs: '[{"id": 1, "name": "first"}, {"id": 2, "name": "second"}]'
partial_struct: '{"name": "partial"}'
extra_fields: '{"name": "test", "count": 42, "extra": "ignored"}'
invalid_json: 'not valid json at all'
empty_object: '{}'
null_value: 'null'
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Test simple struct
	type SimpleStruct struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	var simple SimpleStruct
	err = conf.GetJSON("simple_struct", &simple)
	assert.NoError(t, err)
	assert.Equal(t, "test", simple.Name)
	assert.Equal(t, 42, simple.Count)

	// Test nested struct
	type NestedStruct struct {
		Outer struct {
			Inner struct {
				Value string `json:"value"`
			} `json:"inner"`
		} `json:"outer"`
	}
	var nested NestedStruct
	err = conf.GetJSON("nested_struct", &nested)
	assert.NoError(t, err)
	assert.Equal(t, "deep", nested.Outer.Inner.Value)

	// Test array of structs
	type Item struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	var items []Item
	err = conf.GetJSON("array_of_structs", &items)
	assert.NoError(t, err)
	assert.Len(t, items, 2)
	assert.Equal(t, 1, items[0].ID)
	assert.Equal(t, "first", items[0].Name)
	assert.Equal(t, 2, items[1].ID)
	assert.Equal(t, "second", items[1].Name)

	// Test partial struct (missing fields get zero values)
	var partial SimpleStruct
	err = conf.GetJSON("partial_struct", &partial)
	assert.NoError(t, err)
	assert.Equal(t, "partial", partial.Name)
	assert.Equal(t, 0, partial.Count) // zero value for missing field

	// Test extra fields are ignored
	var extra SimpleStruct
	err = conf.GetJSON("extra_fields", &extra)
	assert.NoError(t, err)
	assert.Equal(t, "test", extra.Name)
	assert.Equal(t, 42, extra.Count)

	// Test invalid JSON returns error
	var invalid SimpleStruct
	err = conf.GetJSON("invalid_json", &invalid)
	assert.Error(t, err)

	// Test non-existent key returns error
	var nonExistent SimpleStruct
	err = conf.GetJSON("non_existent_key", &nonExistent)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Test empty object
	var emptyObj SimpleStruct
	err = conf.GetJSON("empty_object", &emptyObj)
	assert.NoError(t, err)
	assert.Equal(t, "", emptyObj.Name)
	assert.Equal(t, 0, emptyObj.Count)

	// Test GetJSON into a map
	var mapResult map[string]interface{}
	err = conf.GetJSON("simple_struct", &mapResult)
	assert.NoError(t, err)
	assert.Equal(t, "test", mapResult["name"])
	assert.Equal(t, float64(42), mapResult["count"]) // JSON numbers are float64

	// Test GetJSON into a slice
	var sliceResult []map[string]interface{}
	err = conf.GetJSON("array_of_structs", &sliceResult)
	assert.NoError(t, err)
	assert.Len(t, sliceResult, 2)
}

func TestGetMethodsWithYAMLNativeTypes(t *testing.T) {
	// Test that helper methods work with both YAML native types and JSON strings
	file, err := ioutil.TempFile("", "TestGetMethodsWithYAMLNativeTypes")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())

	yamlContent := `
# YAML native array
yaml_array:
  - item1
  - item2

# JSON string array
json_array: '["item1", "item2"]'

# YAML native map
yaml_map:
  key1: value1
  key2: value2

# YAML native int array
yaml_int_array:
  - 1
  - 2
  - 3

# YAML native nested structure
yaml_nested:
  org1:
    tag1:
      - val1
      - val2
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Both YAML native and JSON string should work for string slice
	yamlArray := conf.GetStringSlice("yaml_array", nil)
	jsonArray := conf.GetStringSlice("json_array", nil)
	assert.Equal(t, yamlArray, jsonArray)
	assert.Equal(t, []string{"item1", "item2"}, yamlArray)

	// YAML native map should work with GetStringMap
	yamlMap := conf.GetStringMap("yaml_map", nil)
	assert.Equal(t, "value1", yamlMap["key1"])
	assert.Equal(t, "value2", yamlMap["key2"])

	// YAML native int array should work with GetIntSlice
	yamlIntArray := conf.GetIntSlice("yaml_int_array", nil)
	assert.Equal(t, []int{1, 2, 3}, yamlIntArray)

	// YAML native nested structure should work with GetNestedStringSliceMap
	yamlNested := conf.GetNestedStringSliceMap("yaml_nested", nil)
	assert.Equal(t, []string{"val1", "val2"}, yamlNested["org1"]["tag1"])
}
