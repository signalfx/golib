package expvar2

import (
	"bytes"
	"encoding/json"
	"expvar"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// ExpvarHandler can serve via HTTP expvar variables as well as custom variables inside
// Exported
type ExpvarHandler struct {
	Exported map[string]expvar.Var
}

// Init the ExpvarHandler, creating any datastructures needed
func (e *ExpvarHandler) Init() {
	e.Exported = make(map[string]expvar.Var)
}

// EnviromentalVariables returns an expvar that also shows env variables
func EnviromentalVariables() expvar.Var {
	return enviromentalVariables(os.Environ)
}

func enviromentalVariables(osEnviron func() []string) expvar.Var {
	return expvar.Func(func() interface{} {
		ret := make(map[string]string)
		for _, env := range osEnviron() {
			parts := strings.SplitN(env, "=", 2)
			if len(parts) <= 1 {
				continue
			}
			ret[parts[0]] = parts[1]
		}
		return ret
	})
}

var _ http.Handler = &ExpvarHandler{}

// ServeHTTP is a copy/past of the private expvar.expvarHandler that I sometimes want to
// register to my own handler.
func (e *ExpvarHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	tmp := &bytes.Buffer{}
	fmt.Fprintf(tmp, "{\n")
	first := true
	usedKeys := map[string]struct{}{}
	f := func(kv expvar.KeyValue) {
		if _, exists := usedKeys[kv.Key]; exists {
			return
		}
		if !first {
			fmt.Fprintf(tmp, ",\n")
		}
		first = false
		fmt.Fprintf(tmp, "%q: %s", kv.Key, kv.Value)
	}
	for k, v := range e.Exported {
		f(expvar.KeyValue{
			Key:   k,
			Value: v,
		})
		usedKeys[k] = struct{}{}
	}
	expvar.Do(f)
	fmt.Fprintf(tmp, "\n}\n")
	buf := &bytes.Buffer{}
	json.Indent(buf, tmp.Bytes(), "", "\t")
	w.Write(buf.Bytes())
}
