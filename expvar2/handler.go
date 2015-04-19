package expvar2

import (
	"expvar"
	"fmt"
	"net/http"
)

// ExpvarHandler is a copy/past of the private expvar.expvarHandler that I sometimes want to
// register to my own handler.  I may also one day add the explorable variables here too.
func ExpvarHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}
