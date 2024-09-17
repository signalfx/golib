// Deprecated: This package will be removed in a future release.
package reportsha

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/log"
	"github.com/signalfx/golib/v3/logkey"
	"github.com/signalfx/golib/v3/sfxclient"
)

// SHA1Reporter reports a gauge to Client that is the commit SHA1 of the current image
type SHA1Reporter struct {
	RepoURL  string
	FileName string
	Logger   log.Logger
	Fi       fileInfo
	Tag      string
	oc       sync.Once
}

type fileInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Builder string `json:"builder"`
	Commit  string `json:"commit"`
	Source  string
}

func load(fileName string) (fileInfo, error) {
	var fi fileInfo
	contents, err := ioutil.ReadFile(fileName)
	if err != nil {
		return fi, err
	}
	if err := json.Unmarshal(contents, &fi); err != nil {
		return fi, err
	}
	return fi, nil
}

func (s *SHA1Reporter) loadInfo() {
	s.oc.Do(func() {
		s.Tag = os.Getenv("DOCKER_TAG")
		fi, err := load(s.FileName)
		if err != nil {
			s.Logger.Log(log.Err, err, logkey.Name, s.FileName, "Cannot load file info!")
			return
		}
		fi.Source = fmt.Sprintf("%s/tree/%s", s.RepoURL, fi.Commit)
		s.Fi = fi
	})
}

// Var returns an expvar that is the build file info
func (s *SHA1Reporter) Var() expvar.Var {
	s.loadInfo()
	return expvar.Func(func() interface{} {
		return s.Fi
	})
}

// Datapoints returns a single datapoint that includes the commit sha loaded from a config file
func (s *SHA1Reporter) Datapoints() []*datapoint.Datapoint {
	s.loadInfo()
	dims := map[string]string{"commit": s.Fi.Commit}
	if len(s.Tag) > 0 {
		dims["dockerTag"] = s.Tag
	}
	return []*datapoint.Datapoint{
		sfxclient.Gauge("fileinfo_commit", dims, int64(1)),
	}
}
