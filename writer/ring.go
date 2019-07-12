package writer

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/mauricelam/genny/generic"
)

//go:generate genny -ast -in=$GOFILE -out=ring.gen.go -imp "github.com/signalfx/golib/trace" -imp "github.com/signalfx/golib/datapoint" gen "Instance=Datapoint:datapoint.Datapoint,Span:trace.Span"

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
	inputChan chan *Instance

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
	totalPotentiallyDropped int64
}

// NewInstanceRingWriter returns an initialized but not running instance of the
// InstanceRingWriter. You must call Run on the returned instance for it to work.
func NewInstanceRingWriter(preprocessFunc InstancePreprocessor, sendFunc InstanceSender) *InstanceRingWriter {
	return &InstanceRingWriter{
		inputChan:      make(chan *Instance),
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

// Run waits for things to come in on the provided
// channels and forwards them to SignalFx.  This function blocks until the
// provided context is finished.
func (w *InstanceRingWriter) Run(ctx context.Context) {
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
	requestDoneCh := make(chan struct{}, w.MaxRequests)

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

		w.requestsActive++
		go func(low, high int) {
			count := int64(high - low)
			atomic.AddInt64(&w.totalInFlight, count)

			w.sendFunc(ctx, buffer[low:high])

			atomic.AddInt64(&w.totalInFlight, -count)
			atomic.AddInt64(&w.totalSent, count)

			requestDoneCh <- struct{}{}
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

	handleRequestDone := func() {
		w.requestsActive--
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

		nextDatapointIdx++
		if nextDatapointIdx == bufferSize { // Wrap around the buffer
			nextDatapointIdx = 0
			bufferedCircuits++
		}

		if lastHighStarted < nextDatapointIdx && bufferedCircuits > startedCircuits {
			w.totalPotentiallyDropped = int64(bufferSize)*(bufferedCircuits-startedCircuits) + int64(nextDatapointIdx-lastHighStarted)
			log.Printf("SignalFx writer: ring buffer overflowed, some instances were dropped. Set MaxBuffered to something higher (currently %d)", bufferSize)
		}
		batched++

		if batched >= w.MaxBatchSize {
			tryToSendBufferChunk(targetHighStarted())
		}
	}

	for {
		select {
		case <-ctx.Done():
			return

		case inst := <-w.inputChan:
			process(inst)

		case <-requestDoneCh:
			handleRequestDone()

		default:
			newHigh := targetHighStarted()
			// Could be less if wrapped around
			if newHigh != lastHighStarted {
				tryToSendBufferChunk(newHigh)
			}

			select {
			case <-ctx.Done():
				return

			case <-requestDoneCh:
				handleRequestDone()

			case inst := <-w.inputChan:
				process(inst)
			}
		}
	}
}
