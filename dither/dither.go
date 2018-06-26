package dither

import (
	"math/rand"
	"time"
)

// Dither keeps track of an interval in seconds
type Dither struct {
	nextTime int64
	interval []int64
}

func (d *Dither) advance() {
	// use the current interval in the list
	d.nextTime = time.Now().Add(time.Duration(d.interval[0]) * time.Second).Unix()
	// if there are more than one remaining interval advance them
	if len(d.interval) > 1 {
		d.interval = d.interval[1:]
	}
}

// Now returns a boolean indicating if the current time is greater than or equal
// to the "next" time and the number of seconds until the "next" time
func (d *Dither) Now() (bool, int64) {
	currentTime := time.Now().Unix()
	// if the nextTime hasn't been set
	if currentTime >= d.nextTime {
		d.advance()
		return true, d.nextTime - currentTime
	}
	return false, d.nextTime - currentTime
}

// New returns *Dither with configured with the interval:
// 0 + (0~60)s
// 60s
// 1h + (0~60)s
// 1d + (0~10)m
func New() *Dither {
	var r = rand.New(rand.NewSource(time.Now().UnixNano()))
	return NewWithInterval([]int64{
		r.Int63n(60),
		60,
		r.Int63n(60) + 3600,
		r.Int63n(600) + 86400,
	})
}

// NewWithInterval returns a new *Dither with the provided interval
func NewWithInterval(interval []int64) *Dither {
	return &Dither{
		nextTime: time.Now().Add(time.Duration(interval[0]) * time.Second).Unix(),
		interval: interval,
	}
}
