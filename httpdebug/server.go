package httpdebug

import (
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/signalfx/golib/v3/explorable"
	"github.com/signalfx/golib/v3/expvar2"
	"github.com/signalfx/golib/v3/log"
	"github.com/signalfx/golib/v3/pointer"
)

// Server exposes private debugging information
type Server struct {
	http.Server
	Exp2 *expvar2.Handler
	Mux  *http.ServeMux
}

// Config controls optional parameters for the debug server
type Config struct {
	Logger        log.Logger
	ReadTimeout   *time.Duration
	WriteTimeout  *time.Duration
	ExplorableObj interface{}
}

// DefaultConfig is used by default for unset config parameters
var DefaultConfig = &Config{
	Logger:       log.DefaultLogger.CreateChild(),
	ReadTimeout:  pointer.Duration(time.Duration(0)),
	WriteTimeout: pointer.Duration(time.Duration(0)),
}

// LogKeyHTTPClass is appended as a key to subloggers of the debug server
var LogKeyHTTPClass = log.Key("http_class")

// New creates a new debug server
func New(conf *Config) *Server {
	conf, _ = pointer.FillDefaultFrom(conf, DefaultConfig).(*Config)
	m := http.NewServeMux()
	s := &Server{
		Server: http.Server{
			// TODO: Also put logger in http.Server
			Handler:      m,
			ReadTimeout:  *conf.ReadTimeout,
			WriteTimeout: *conf.WriteTimeout,
		},
		Exp2: expvar2.New(),
		Mux:  m,
	}
	m.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	m.HandleFunc("/debug/pprof/profile", pprof.Profile)
	m.HandleFunc("/debug/pprof/", pprof.Index)
	m.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	setupTrace(m)
	if conf.ExplorableObj != nil {
		e := &explorable.Handler{
			Val:      conf.ExplorableObj,
			BasePath: "/debug/explorer/",
			Logger:   log.NewContext(conf.Logger).With(LogKeyHTTPClass, "explorable"),
		}
		m.Handle("/debug/explorer/", e)
	}
	m.Handle("/debug/vars", s.Exp2)
	return s
}
