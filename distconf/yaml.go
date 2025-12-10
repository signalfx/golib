package distconf

import (
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/signalfx/golib/v3/errors"
	"github.com/spf13/viper"
)

// yamlCallbackMap manages callbacks for YAML config keys.
type yamlCallbackMap struct {
	mu        sync.Mutex
	callbacks map[string][]backingCallbackFunction
}

func (c *yamlCallbackMap) add(key string, val backingCallbackFunction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callbacks[key] = append(c.callbacks[key], val)
}

func (c *yamlCallbackMap) copy() map[string][]backingCallbackFunction {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := make(map[string][]backingCallbackFunction, len(c.callbacks))
	for k, v := range c.callbacks {
		ret[k] = append([]backingCallbackFunction{}, v...)
	}
	return ret
}

// yamlFileDisco is a struct that implements the Reader and Dynamic interfaces for YAML files.
type yamlFileDisco struct {
	noopCloser
	filename  string
	v         *viper.Viper
	mu        sync.RWMutex    // Protects concurrent access to the Viper instance
	callbacks yamlCallbackMap // Stores registered callbacks for dynamic updates
}

// Get retrieves the value for a given key from the YAML file.
func (y *yamlFileDisco) Get(key string) ([]byte, error) {
	y.mu.RLock()
	defer y.mu.RUnlock()

	// Fetch the value from Viper
	value := y.v.GetString(key)
	if value == "" {
		return nil, nil // Return nil if the key is not found
	}
	return []byte(value), nil
}

// Watch registers a callback function for a given key.
// The callback will be invoked when the YAML file changes.
func (y *yamlFileDisco) Watch(key string, callback backingCallbackFunction) error {
	y.callbacks.add(key, callback)
	return nil
}

// watchFile watches for changes to the YAML file and reloads the configuration automatically.
func (y *yamlFileDisco) watchFile() error {
	y.v.WatchConfig()
	y.v.OnConfigChange(func(e fsnotify.Event) {
		// Handle file change events (Viper automatically reloads the config)
		fmt.Printf("Configuration file changed: %s\n", e.Name)

		// Copy callbacks before invoking them to avoid holding locks during callback execution.
		// This prevents deadlock since callbacks may call Get() which needs the read lock.
		callbacksCopy := y.callbacks.copy()

		// Notify all registered callbacks about the change
		for key, callbacks := range callbacksCopy {
			for _, c := range callbacks {
				c(key)
			}
		}
	})
	return nil
}

// Yaml creates a backing config reader that reads properties from a YAML file.
// It supports watching the file for real-time updates.
func Yaml(filename string) (Reader, error) {
	v := viper.New()

	// Set the configuration file and format
	v.SetConfigFile(filename)
	v.SetConfigType("yaml")

	// Attempt to read the configuration file
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Annotatef(err, "Unable to open file %s", filename)
	}

	// Create a new yamlFileDisco instance
	yamlDisco := &yamlFileDisco{
		filename: filename,
		v:        v,
		callbacks: yamlCallbackMap{
			callbacks: make(map[string][]backingCallbackFunction),
		},
	}

	// Start watching the file for changes
	if err := yamlDisco.watchFile(); err != nil {
		return nil, errors.Annotatef(err, "Unable to watch file %s", filename)
	}

	return yamlDisco, nil
}

// YamlLoader is a helper for loading from YAML files.
// It returns a BackingLoader that can be used to initialize a config reader.
func YamlLoader(filename string) BackingLoader {
	return BackingLoaderFunc(func() (Reader, error) {
		return Yaml(filename)
	})
}
