package web

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/signalfx/golib/v3/errors"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

type IncrHandler struct {
	before int64
	after  int64
}

func (i *IncrHandler) ServeHTTPCN(ctx context.Context, rw http.ResponseWriter, r *http.Request, next ContextHandler) {
	atomic.AddInt64(&i.before, 1)
	next.ServeHTTPC(ctx, rw, r)
	atomic.AddInt64(&i.after, 1)
}

func (i *IncrHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request, next http.Handler) {
	atomic.AddInt64(&i.before, 1)
	next.ServeHTTP(rw, r)
	atomic.AddInt64(&i.after, 1)
}

func (i *IncrHandler) makeHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		i.ServeHTTP(rw, r, next)
	})
}

func TestInvalidContentType(t *testing.T) {
	Convey("Invalid content type should work", t, func() {
		rec := httptest.NewRecorder()
		req, err := http.NewRequestWithContext(context.Background(), "", "", nil)
		So(err, ShouldBeNil)
		req.Header.Add("Content-Type", "bob")
		InvalidContentType(rec, req)
		So(rec.Code, ShouldEqual, http.StatusBadRequest)
	})
}

func TestHandler(t *testing.T) {
	i := IncrHandler{}
	expectAfter := 0
	expectBefore := 2
	destination := HandlerFunc(func(ctx context.Context, rw http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "value", ctx.Value("key"))
		assert.EqualValues(t, expectAfter, i.after)
		assert.EqualValues(t, expectBefore, i.before)
	})
	ctx := context.Background()
	h := NewHandler(ctx, destination)
	v1 := VarAdder{
		Key:   "key",
		Value: "value",
	}

	h.Add(ConstructorFunc(v1.Generate), HTTPConstructor(i.makeHTTP), NextHTTP(i.ServeHTTP))
	rw := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
	h.ServeHTTP(rw, req)
	assert.EqualValues(t, 2, i.after)

	expectAfter = 2
	expectBefore = 4
	h.ServeHTTPC(ctx, rw, req)
	assert.EqualValues(t, 4, i.after)

	bodyTest := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		errors.PanicIfErrWrite(rw.Write([]byte("test")))
	})

	ToHTTP(ctx, FromHTTP(bodyTest)).ServeHTTP(rw, req)
	assert.Equal(t, "test", rw.Body.String())
}

type writeType string

const (
	toWrite writeType = "towrite"
)

func addTowrite(ctx context.Context, rw http.ResponseWriter, r *http.Request, next ContextHandler) {
	next.ServeHTTPC(context.WithValue(ctx, toWrite, []byte(r.Header.Get("towrite"))), rw, r)
}

func TestMany(t *testing.T) {
	incrHandler := IncrHandler{}

	destination := HandlerFunc(func(ctx context.Context, rw http.ResponseWriter, r *http.Request) {
		errors.PanicIfErrWrite(rw.Write(ctx.Value(toWrite).([]byte)))
	})

	ctx := context.Background()
	h := NewHandler(ctx, destination).Add(NextConstructor(addTowrite), HTTPConstructor(incrHandler.makeHTTP))

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if j%11 == 0 {
					time.Sleep(time.Nanosecond)
				}
				rw := httptest.NewRecorder()
				req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
				req.Header.Add("towrite", fmt.Sprintf("%d", j))
				h.ServeHTTP(rw, req)
				assert.Equal(t, fmt.Sprintf("%d", j), rw.Body.String())
			}
		}()
	}
	wg.Wait()
}

func TestNoMiddleware(t *testing.T) {
	destination := HandlerFunc(func(ctx context.Context, rw http.ResponseWriter, r *http.Request) {
		errors.PanicIfErrWrite(rw.Write([]byte("Hello")))
	})

	ctx := context.Background()
	h := NewHandler(ctx, destination)
	rw := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
	h.ServeHTTP(rw, req)
	assert.Equal(t, "Hello", rw.Body.String())
}

func TestPanicCheck(t *testing.T) {
	var hand http.Handler
	destination := HandlerFunc(func(ctx context.Context, rw http.ResponseWriter, r *http.Request) {
		hand.ServeHTTP(rw, r)
	})
	hcreate := HTTPConstructor(func(next http.Handler) http.Handler {
		return next
	})
	middle := hcreate.CreateMiddleware(destination)
	ctx := context.Background()
	hand = ToHTTP(ctx, middle)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
	rw := httptest.NewRecorder()
	assert.Panics(t, func() {
		middle.ServeHTTPC(ctx, rw, req)
	})
}

func BenchmarkSendWithContext(b *testing.B) {
	incrHandler := IncrHandler{}

	destination := HandlerFunc(func(ctx context.Context, rw http.ResponseWriter, r *http.Request) {
		b.StopTimer()
		errors.PanicIfErrWrite(rw.Write(ctx.Value("towrite").([]byte)))
		b.StartTimer()
	})

	ctx := context.Background()
	h := NewHandler(ctx, destination).Add(NextConstructor(addTowrite), HTTPConstructor(incrHandler.makeHTTP))

	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for j := 0; j < b.N; j++ {
		rw := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
		req.Header.Add("towrite", fmt.Sprintf("%d", j))
		b.StartTimer()
		h.ServeHTTP(rw, req)
		b.StopTimer()
		assert.Equal(b, fmt.Sprintf("%d", j), rw.Body.String())
	}
}

func BenchmarkMinimal(b *testing.B) {
	destination := HandlerFunc(func(ctx context.Context, rw http.ResponseWriter, r *http.Request) {
	})

	ctx := context.Background()
	h := NewHandler(ctx, destination)

	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for j := 0; j < b.N; j++ {
		rw := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
		b.StartTimer()
		h.ServeHTTP(rw, req)
		b.StopTimer()
	}
}

func BenchmarkSingle(b *testing.B) {
	incrHandler := IncrHandler{}
	destination := HandlerFunc(func(ctx context.Context, rw http.ResponseWriter, r *http.Request) {
	})

	ctx := context.Background()
	h := NewHandler(ctx, destination).Add(NextConstructor(incrHandler.ServeHTTPCN))

	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for j := 0; j < b.N; j++ {
		rw := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
		b.StartTimer()
		h.ServeHTTP(rw, req)
		b.StopTimer()
	}
}
