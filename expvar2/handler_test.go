package expvar2

import (
	"expvar"
	"net/http/httptest"
	"testing"

	"os"

	"github.com/stretchr/testify/assert"
)

var toFind = "asdfasdfdsa"

func init() {
	expvar.NewString("expvar2.TestExpvarHandler.a").Set("abc")
	expvar.NewString("expvar2.TestExpvarHandler.b").Set(toFind)
}

func TestExpvarHandler(t *testing.T) {
	w := httptest.NewRecorder()
	e := ExpvarHandler{}
	e.Init()
	e.Exported["hello"] = expvar.Func(func() interface{} {
		return "world"
	})
	e.ServeHTTP(w, nil)
	assert.Contains(t, w.Body.String(), toFind)

	w.Body.Reset()
	e.Exported["expvar2.TestExpvarHandler.b"] = expvar.Func(func() interface{} {
		return "replaced"
	})
	e.ServeHTTP(w, nil)
	assert.NotContains(t, w.Body.String(), toFind)
}

func TestEnviromentalVariables(t *testing.T) {
	os.Setenv("TestEnviromentalVariables", "abcdefg")
	s := EnviromentalVariables().String()
	assert.Contains(t, s, "TestEnviromentalVariables")
	assert.Contains(t, s, "abcdefg")

	s = enviromentalVariables(func() []string {
		return []string{
			"abc=b",
			"abcd",
			"z=",
			"a=b=c",
		}
	}).String()
	assert.NotContains(t, s, "abcd")
	assert.Contains(t, s, "z")
	assert.Contains(t, s, "b=c")
}
