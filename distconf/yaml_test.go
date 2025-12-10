package distconf

import (
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
