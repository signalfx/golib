package log

import (
	"errors"
	. "github.com/smartystreets/goconvey/convey"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func TestFilter(t *testing.T) {
	Convey("filtered logger", t, func() {
		counter := &Counter{}
		counter2 := &Counter{}
		logger := &Filter{
			PassTo:          counter,
			ErrCallback:     counter2,
			MissingValueKey: "msg",
		}
		So(counter.Count, ShouldEqual, 0)
		Convey("should start disabled", func() {
			So(IsDisabled(logger), ShouldBeTrue)
			So(logger.Disabled(), ShouldBeTrue)
		})
		Convey("should not log at first", func() {
			logger.Log("hello world")
			So(counter.Count, ShouldEqual, 0)
		})
		Convey("When enabled", func() {
			logger.SetFilters(map[string]*regexp.Regexp{
				"msg": regexp.MustCompile("hello bob"),
			})
			So(logger.Disabled(), ShouldBeFalse)
			So(counter.Count, ShouldEqual, 1)
			Convey("should export stats", func() {
				So(logger.Var().String(), ShouldContainSubstring, "hello bob")
			})
			Convey("Should log", func() {
				logger.Log("hello world")
				So(counter.Count, ShouldEqual, 1)
				logger.Log("hello bob")
				So(counter.Count, ShouldEqual, 2)
				logger.Log("hello bob two")
				So(counter.Count, ShouldEqual, 3)
			})
			Convey("Should not log missing dimensions", func() {
				logger.Log("missing", "10")
				So(counter.Count, ShouldEqual, 1)
			})
			Convey("Should not log unconvertable dimensions", func() {
				So(counter2.Count, ShouldEqual, 0)

				logger.Log(func() {})
				So(counter.Count, ShouldEqual, 1)
				So(counter2.Count, ShouldEqual, 1)
			})
		})
	})
}

func TestMatchesWorksOnNilSet(t *testing.T) {
	if matches(nil, nil, nil) {
		t.Error("Expected matches not to match on all nil")
	}
}

func TestMultiFilter(t *testing.T) {
	Convey("A multi filter", t, func() {
		mf := MultiFilter{}
		Convey("starts off disabled", func() {
			So(mf.Disabled(), ShouldBeTrue)
		})
		Convey("Can add a filter", func() {
			counter := &Counter{}
			logger := &Filter{
				PassTo:          counter,
				ErrCallback:     counter,
				MissingValueKey: "msg",
			}
			mf.Filters = append(mf.Filters, logger)
			So(mf.Disabled(), ShouldBeTrue)
			mf.Log("hello")
			So(counter.Count, ShouldEqual, 0)
			logger.SetFilters(map[string]*regexp.Regexp{
				"msg": regexp.MustCompile("hello bob"),
			})
			So(counter.Count, ShouldEqual, 1)
			mf.Log("hello")
			So(counter.Count, ShouldEqual, 1)
			mf.Log("hello bob")
			So(counter.Count, ShouldEqual, 2)
			So(mf.Var().String(), ShouldContainSubstring, "hello bob")
		})
	})
}

type errResponseWriter struct {
	http.ResponseWriter
}

func (e *errResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("cannot write")
}

func TestFilterChangeHandler(t *testing.T) {
	Convey("A filter and handler", t, func() {
		counter := &Counter{}
		logger := &Filter{
			PassTo:          counter,
			ErrCallback:     Panic,
			MissingValueKey: Msg,
		}

		handler := FilterChangeHandler{
			Filter: logger,
			Log:    Discard,
		}
		Convey("Should get as Empty", func() {
			rw := httptest.NewRecorder()
			req, err := http.NewRequest("GET", "", nil)
			So(err, ShouldBeNil)
			handler.ServeHTTP(rw, req)
			So(rw.Code, ShouldEqual, http.StatusOK)
			So(rw.Body.String(), ShouldContainSubstring, "There are currently 0 filters")
			So(len(logger.GetFilters()), ShouldEqual, 0)
		})
		Convey("Should 404 on non GET/POST", func() {
			rw := httptest.NewRecorder()
			req, err := http.NewRequest("PUT", "", nil)
			So(err, ShouldBeNil)
			handler.ServeHTTP(rw, req)
			So(rw.Code, ShouldEqual, http.StatusNotFound)
		})
		Convey("Should error if cannot write template", func() {
			rw := httptest.NewRecorder()
			req, err := http.NewRequest("GET", "", nil)
			So(err, ShouldBeNil)
			handler.ServeHTTP(&errResponseWriter{rw}, req)
			So(rw.Code, ShouldEqual, http.StatusInternalServerError)
		})

		Convey("Should be updatable", func() {
			rw := httptest.NewRecorder()
			req, err := http.NewRequest("POST", "", strings.NewReader(""))
			So(err, ShouldBeNil)
			So(req.ParseForm(), ShouldBeNil)
			req.Form.Add("newregex", "id:1")
			handler.ServeHTTP(rw, req)
			So(rw.Code, ShouldEqual, http.StatusOK)
			So(rw.Body.String(), ShouldContainSubstring, "There are currently 1 filters")
			So(len(logger.GetFilters()), ShouldEqual, 1)
			Convey("Does nothing on invalid updates", func() {
				rw := httptest.NewRecorder()
				req, err := http.NewRequest("POST", "", strings.NewReader(""))
				So(err, ShouldBeNil)
				So(req.ParseForm(), ShouldBeNil)
				req.Form.Add("newregex", "id")
				handler.ServeHTTP(rw, req)
				So(rw.Code, ShouldEqual, http.StatusOK)
				So(rw.Body.String(), ShouldContainSubstring, "There are currently 1 filters")
				So(len(logger.GetFilters()), ShouldEqual, 1)
			})
			Convey("Does nothing on invalid regex", func() {
				rw := httptest.NewRecorder()
				req, err := http.NewRequest("POST", "", strings.NewReader(""))
				So(err, ShouldBeNil)
				So(req.ParseForm(), ShouldBeNil)
				req.Form.Add("newregex", "id:[123")
				handler.ServeHTTP(rw, req)
				So(rw.Code, ShouldEqual, http.StatusOK)
				So(rw.Body.String(), ShouldContainSubstring, "There are currently 1 filters")
				So(len(logger.GetFilters()), ShouldEqual, 1)
			})

			Convey("Does nothing on invalid POST", func() {
				rw := httptest.NewRecorder()
				req, err := http.NewRequest("POST", "", nil)
				So(err, ShouldBeNil)
				handler.ServeHTTP(rw, req)
				So(rw.Code, ShouldEqual, http.StatusOK)
				So(rw.Body.String(), ShouldContainSubstring, "There are currently 1 filters")
				So(len(logger.GetFilters()), ShouldEqual, 1)
			})

			Convey("and can change back", func() {
				rw := httptest.NewRecorder()
				req, err := http.NewRequest("POST", "", strings.NewReader(""))
				So(err, ShouldBeNil)
				So(req.ParseForm(), ShouldBeNil)
				req.Form.Add("newregex", "")
				handler.ServeHTTP(rw, req)
				So(rw.Code, ShouldEqual, http.StatusOK)
				So(rw.Body.String(), ShouldContainSubstring, "There are currently 0 filters")
				So(len(logger.GetFilters()), ShouldEqual, 0)
			})
		})
	})
}

func BenchmarkFilterDisabled(b *testing.B) {
	counter := &Counter{}
	logger := &Filter{
		PassTo:          counter,
		ErrCallback:     counter,
		MissingValueKey: "msg",
	}
	for i := 0; i < b.N; i++ {
		logger.Log("hello world")
	}
	if counter.Count != 0 {
		b.Errorf("Expected a zero count, got %d\n", counter.Count)
	}
}

func BenchmarkFilterEnabled(b *testing.B) {
	counter := &Counter{}
	logger := &Filter{
		PassTo:          counter,
		ErrCallback:     counter,
		MissingValueKey: "msg",
	}
	logger.SetFilters(map[string]*regexp.Regexp{
		"id": regexp.MustCompile(`^1234$`),
	})
	for i := 0; i < b.N; i++ {
		var idToLog int64
		if i%2 == 0 {
			idToLog = 1235
		} else {
			idToLog = 1234
		}
		logger.Log("id", idToLog, "hello world")
	}
	if int(counter.Count) != b.N/2 {
		b.Errorf("Expected %d count, got %d\n", b.N/2, counter.Count)
	}
}

func BenchmarkFilterEnabled30(b *testing.B) {
	counter := &Counter{}
	logger := &Filter{
		PassTo:          counter,
		ErrCallback:     counter,
		MissingValueKey: "msg",
	}
	logger.SetFilters(map[string]*regexp.Regexp{
		"id": regexp.MustCompile(`^1234$`),
	})
	wg := sync.WaitGroup{}
	wg.Add(30)
	for n := 0; n < 30; n++ {
		go func() {
			for i := 0; i < b.N; i++ {
				var idToLog int64
				if i%2 == 0 {
					idToLog = 1235
				} else {
					idToLog = 1234
				}
				logger.Log("id", idToLog, "hello world")
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if int(counter.Count) != b.N/2*30 {
		b.Errorf("Expected %d count, got %d\n", b.N/2*30, counter.Count)
	}
}
