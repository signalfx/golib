package distconf

import log "github.com/Sirupsen/logrus"

// A BackingLoader should run a single time and get a Reader for Config
type BackingLoader interface {
	Get() (Reader, error)
}

// BackingLoaderFunc can wrap a function to turn it into a BackingLoader
type BackingLoaderFunc func() (Reader, error)

// Get a Reader for Config, or an error if the Reader cannot be loaded
func (f BackingLoaderFunc) Get() (Reader, error) {
	return f()
}

// FromLoaders creates a Config from an array of loaders, only using loaders that don't load with
// error
func FromLoaders(loaders []BackingLoader) *Config {
	readers := make([]Reader, 0, len(loaders))
	for _, l := range loaders {
		r, err := l.Get()
		if err != nil {
			log.WithField("err", err).Warn("Unable to load reader")
			continue
		}
		readers = append(readers, r)
	}
	return FromReaders(readers)
}

// FromReaders creates a Config from the list of config values readers
func FromReaders(readers []Reader) *Config {
	return &Config{
		readers:        readers,
		registeredVars: make(map[string]configVariable),
	}
}
