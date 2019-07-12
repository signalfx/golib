package template

// nolint: dupl

import (
	"context"
	"sync/atomic"

	"github.com/mauricelam/genny/generic"
)

//go:generate genny -ast -pkg=writer -in=$GOFILE -out=../datapoint_ring.gen.go -imp "github.com/signalfx/golib/datapoint" gen "Instance=Datapoint:datapoint.Datapoint"
//go:generate genny -ast -pkg=writer -in=$GOFILE -out=../span_ring.gen.go -imp "github.com/signalfx/golib/trace" gen "Instance=Span:trace.Span"

type Instance generic.Type

// InstancePreprocessor is used to filter out or otherwise change instances
// before being sent.  If the return value is false, the instance won't be
// sent.
type InstancePreprocessor func(*Instance) bool

// InstanceSender is what sends a slice of instances.  It should block until
// the instances have been sent, or an error has occurred.
type InstanceSender func(context.Context, []*Instance) error

// InstanceRingWriter is an abstraction that accepts a bunch of instances,
// buffers them in a ring buffer and sends them out in batches.
type InstanceRingWriter struct {
	inputChan    chan *Instance
	shutdownFlag chan struct{}

	preprocessFunc InstancePreprocessor
	sendFunc       InstanceSender

	MaxBuffered  int
	MaxRequests  int
	MaxBatchSize int

	requestsActive  int64
	requestsWaiting int64
	totalWaiting    int64

	totalReceived           int64
	totalFilteredOut        int64
	totalInFlight           int64
	totalSent               int64
	totalFailedToSend       int64
	totalPotentiallyDropped int64
}

// NewInstanceRingWriter returns an initialized but not running instance of the
// InstanceRingWriter. You must call Run on the returned instance for it to work.
// preprocessFunc can be used for filtering or modifying instances before
// being sent.  If preprocessFunc returns false, the instance will not be
// sent.  preprocessFunc can be nil, in which case all instances will be sent.
// sendFunc must be provided as the writer is useless without it.
func NewInstanceRingWriter(preprocessFunc InstancePreprocessor, sendFunc InstanceSender) *InstanceRingWriter {
	return &InstanceRingWriter{
		// Give the input channel a bit of buffer, but in general the writer
		// should be pulling off of it faster than instances would normally be
		// generated.
		inputChan:      make(chan *Instance, 1000),
		preprocessFunc: preprocessFunc,
		sendFunc:       sendFunc,

		MaxBuffered:  10000,
		MaxRequests:  10,
		MaxBatchSize: 1000,
	}
}

// InputChan returns the channel that should be used to send instances to this
// writer.
func (w *InstanceRingWriter) InputChan() chan *Instance {
	return w.inputChan
}

func (w *InstanceRingWriter) WaitForShutdown() {
	if w.shutdownFlag == nil {
		panic("should not wait for writer shutdown when not running")
	}
	<-w.shutdownFlag
}

// Run waits for things to come in on the provided
// channels and forwards them to SignalFx.  This function blocks until the
// provided context is finished.
// nolint: dupl
func (w *InstanceRingWriter) Run(ctx context.Context) {
	w.shutdownFlag = make(chan struct{})
	defer func() {
		close(w.shutdownFlag)
	}()

	bufferSize := w.MaxBuffered
	// Ring buffer of datapoints, initialized to its maximum length to avoid
	// reallocations.
	buffer := make([]*Instance, bufferSize)
	// The index that marks the end of the last chunk of datapoints that was
	// sent.  It is one greater than the actual index, to match the golang
	// slice high range.
	lastHighStarted := 0
	// The next index within the buffer that a datapoint should be added to.
	nextDatapointIdx := 0
	// Corresponds to nextDatapointIdx but is easier to work with without modulo
	batched := 0
	requestDoneCh := make(chan int64, w.MaxRequests)

	// How many times around the ring buffer we have gone when putting
	// datapoints onto the buffer
	bufferedCircuits := int64(0)
	// How many times around the ring buffer we have gone when starting
	// requests
	startedCircuits := int64(0)

	targetHighStarted := func() int {
		if nextDatapointIdx < lastHighStarted {
			// Wrap around happened, just take what we have left until wrap
			// around so that we can take a single slice of it since slice
			// ranges can't wrap around.
			return bufferSize
		}

		return nextDatapointIdx
	}

	tryToSendBufferChunk := func(newHigh int) bool {
		if newHigh == lastHighStarted { // Nothing added
			return false
		}

		if w.requestsActive >= int64(w.MaxRequests) {
			w.requestsWaiting++
			w.totalWaiting += int64(newHigh - lastHighStarted)
			return false
		}

		count := int64(newHigh - lastHighStarted)
		w.totalInFlight += count
		w.requestsActive++

		go func(low, high int) {
			err := w.sendFunc(ctx, buffer[low:high])
			if err != nil {
				atomic.AddInt64(&w.totalFailedToSend, count)
			} else {
				atomic.AddInt64(&w.totalSent, count)
			}

			requestDoneCh <- count
		}(lastHighStarted, newHigh)

		lastHighStarted = newHigh
		if lastHighStarted == bufferSize { // Wrap back to 0
			lastHighStarted = 0
			startedCircuits++
		}

		batched = 0
		w.requestsWaiting = 0
		w.totalWaiting = 0
		return true
	}

	handleRequestDone := func(count int64) {
		w.requestsActive--
		w.totalInFlight -= count
		if w.requestsWaiting > 0 {
			tryToSendBufferChunk(targetHighStarted())
		}
	}

	process := func(inst *Instance) {
		w.totalReceived++

		if w.preprocessFunc != nil && !w.preprocessFunc(inst) {
			w.totalFilteredOut++
			return
		}

		buffer[nextDatapointIdx] = inst

		if lastHighStarted-1-int(w.totalInFlight) < nextDatapointIdx && bufferedCircuits > startedCircuits {
			w.totalPotentiallyDropped++
		}
		batched++

		nextDatapointIdx++
		if nextDatapointIdx == bufferSize { // Wrap around the buffer
			nextDatapointIdx = 0
			bufferedCircuits++
		}

		if batched >= w.MaxBatchSize {
			tryToSendBufferChunk(targetHighStarted())
		}
	}

	waitForRequests := func() {
		for w.requestsActive > 0 {
			select {
			case count := <-requestDoneCh:
				handleRequestDone(count)
			}
		}
	}

	flush := func() {
		newHigh := targetHighStarted()
		// Could be less if wrapped around
		if newHigh != lastHighStarted {
			tryToSendBufferChunk(newHigh)
		}
	}

	drainInput := func() {
		defer waitForRequests()
		defer flush()
		for {
			select {
			case inst := <-w.inputChan:
				process(inst)
			default:
				return
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			drainInput()
			return

		case inst := <-w.inputChan:
			process(inst)

		case count := <-requestDoneCh:
			handleRequestDone(count)

		default:
			flush()

			select {
			case <-ctx.Done():
				drainInput()
				return

			case count := <-requestDoneCh:
				handleRequestDone(count)

			case inst := <-w.inputChan:
				process(inst)
			}
		}
	}
}
