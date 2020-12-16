package timekeeper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimer(t *testing.T) {
	r := RealTime{}
	timer := r.NewTimer(time.Millisecond)
	v := <-timer.Chan()
	assert.NotNil(t, v)
}

func TestStop(t *testing.T) {
	r := RealTime{}
	timer := r.NewTimer(time.Minute)
	assert.True(t, timer.Stop())
	assert.False(t, timer.Stop())
}

func TestAfterClose(t *testing.T) {
	timer := time.Millisecond * 10
	x := time.NewTimer(timer)
	x.Stop()
	select {
	case <-x.C:
		panic("NOPE")
	case <-time.After(timer * 2):
	}
}

func TestRealTime(t *testing.T) {
	r := RealTime{}
	now := r.Now()
	timer := time.Microsecond * 10
	r.Sleep(timer)
	<-r.After(timer)
	<-r.NewTimer(timer).Chan()
	assert.True(t, time.Now().After(now))
}
