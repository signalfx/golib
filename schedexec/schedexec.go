package schedexec

import (
	"time"

	"sync"
	"sync/atomic"

	"github.com/signalfx/golib/timekeeper"
)

//ScheduledExecutor is an abstraction for running things on a schedule
type ScheduledExecutor struct {
	scheduleRate int64
	TimeKeeper   timekeeper.TimeKeeper
	closeChan    chan struct{}
	wg           sync.WaitGroup
	startedCount uint32
	runCount     uint32
}

//NewScheduledExecutor returns a new ScheduledExecutor that will file
func NewScheduledExecutor(scheduleRate time.Duration) *ScheduledExecutor {
	return &ScheduledExecutor{
		scheduleRate: int64(scheduleRate),
		TimeKeeper:   timekeeper.RealTime{},
		closeChan:    make(chan struct{}),
	}
}

//SetScheduleRate sets the new rate at which runs should run for this ScheduledExecutor
func (se *ScheduledExecutor) SetScheduleRate(scheduleRate time.Duration) {
	atomic.StoreInt64(&se.scheduleRate, int64(scheduleRate))
}

// Start the Scheduled
func (se *ScheduledExecutor) Start(iterationFunc func() error) error {
	timer := se.TimeKeeper.NewTimer(time.Duration(atomic.LoadInt64(&se.scheduleRate)))
	atomic.AddUint32(&se.startedCount, 1)
	se.wg.Add(1)
	defer se.wg.Done()
	for {
		select {
		case <-timer.Chan():
			if err := iterationFunc(); err != nil {
				return err
			}
			timer = se.TimeKeeper.NewTimer(time.Duration(atomic.LoadInt64(&se.scheduleRate)))
			atomic.AddUint32(&se.runCount, 1)
		case <-se.closeChan:
			timer.Stop()
			return nil
		}
	}
}

//Close stops the ScheduledExecutor
func (se *ScheduledExecutor) Close() error {
	close(se.closeChan)
	se.wg.Wait()
	return nil
}
