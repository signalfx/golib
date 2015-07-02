package event

import (
	"fmt"
	"time"
)

// An Event is a noteworthy occurrence of something
type Event struct {
	// Source of the event
	Source string
	// EventType encodes where the event came from and some of the meaning
	EventType string
	// Dimensions of what is being measured.  They are intrinsic.  Contributes to the identity of
	// the metric. If this changes, we get a new metric identifier
	Dimensions map[string]string
	// Meta is information that's not particularly important to the event, but may be important
	// to the pipeline that uses the event.  They are extrinsic.  It provides additional
	// information about the metric. changes in this set doesn't change the metric identity
	Meta      map[string]interface{}
	Timestamp time.Time
}

func (e *Event) String() string {
	return fmt.Sprintf("E[%s\t%s\t%s\t%s\t%s]", e.Source, e.EventType, e.Dimensions, e.Meta, e.Timestamp.String())
}

// New creates a new event with empty meta data
func New(source string, eventType string, dimensions map[string]string, timestamp time.Time) *Event {
	return NewWithMeta(source, eventType, dimensions, map[string]interface{}{}, timestamp)
}

// NewWithMeta creates a new event with passed metadata
func NewWithMeta(source string, eventType string, dimensions map[string]string, meta map[string]interface{}, timestamp time.Time) *Event {
	return &Event{
		Source:     source,
		EventType:  eventType,
		Dimensions: dimensions,
		Meta:       meta,
		Timestamp:  timestamp,
	}
}
