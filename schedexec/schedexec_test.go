package schedexec

import (
	"errors"
	"testing"
	"time"

	"runtime"
	"sync/atomic"

	"github.com/signalfx/golib/timekeeper/timekeepertest"
	"github.com/stretchr/testify/assert"
)

type testScheduled struct {
	calledIteratorCount  int32
	runOneIterationError error
}

func (s *testScheduled) runOneIteration() error {
	atomic.AddInt32(&s.calledIteratorCount, 1)
	return s.runOneIterationError
}

func TestNewScheduleExecutor(t *testing.T) {
	scheduled := &testScheduled{}
	se := NewScheduledExecutor(time.Second)
	assert.NotNil(t, se)
	doneChan := make(chan struct{})
	var err error
	go func() {
		err = se.Start(scheduled.runOneIteration)
		close(doneChan)
	}()
	se.Close()
	<-doneChan
	assert.Nil(t, err)
}

func TestScheduleExecutorTick(t *testing.T) {
	scheduled := &testScheduled{}
	se := NewScheduledExecutor(time.Second)
	stubTime := timekeepertest.NewStubClock(time.Now())
	se.TimeKeeper = stubTime
	runChan := make(chan struct{})
	f := func() error {
		defer func() { runChan <- struct{}{} }()
		return scheduled.runOneIteration()
	}

	doneChan := make(chan struct{})
	var err error
	go func() {
		err = se.Start(f)
		close(doneChan)
	}()

	for atomic.LoadUint32(&se.startedCount) == 0 {
		runtime.Gosched()
	}
	stubTime.Incr(time.Second)
	<-runChan

	assert.Equal(t, int32(1), atomic.LoadInt32(&scheduled.calledIteratorCount))

	scheduled.runOneIterationError = errors.New("MOO")
	for atomic.LoadUint32(&se.runCount) == 0 {
		runtime.Gosched()
	}
	stubTime.Incr(time.Second)

	<-runChan
	<-doneChan
	assert.Equal(t, scheduled.runOneIterationError, err)

}

func TestScheduleExecutorUpdateScheduleRate(t *testing.T) {
	scheduled := &testScheduled{}
	se := NewScheduledExecutor(time.Second)
	stubTime := timekeepertest.NewStubClock(time.Now())
	se.TimeKeeper = stubTime
	runChan := make(chan struct{})
	f := func() error {
		defer func() { runChan <- struct{}{} }()
		return scheduled.runOneIteration()
	}

	doneChan := make(chan struct{})
	var err error
	go func() {
		err = se.Start(f)
		close(doneChan)
	}()

	for atomic.LoadUint32(&se.startedCount) == 0 {
		runtime.Gosched()
	}
	// make sure we only update the scheduled rate after the initial timer was created
	se.SetScheduleRate(2 * time.Second)

	stubTime.Incr(time.Second)
	<-runChan
	assert.Equal(t, int32(1), atomic.LoadInt32(&scheduled.calledIteratorCount))

	for atomic.LoadUint32(&se.runCount) == 0 {
		runtime.Gosched()
	}
	stubTime.Incr(2 * time.Second)
	<-runChan
	assert.Equal(t, int32(2), atomic.LoadInt32(&scheduled.calledIteratorCount))

	se.Close()
	<-doneChan
	assert.Equal(t, scheduled.runOneIterationError, err)

}
