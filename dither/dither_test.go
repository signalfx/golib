package dither

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	t.Run("test dither", func(t *testing.T) {
		d := New()
		now, remain := d.Now()
		if now {
			if remain <= 0 {
				t.Error("remaining time is unrealistic and may not have been advanced")
			}
		}
	})
}

func TestNewWithInterval(t *testing.T) {
	t.Run("test advancement", func(t *testing.T) {
		interval := []int64{3, 5, 10}
		d := NewWithInterval(interval)
		count := 0
		for {
			now, remain := d.Now()
			if now {
				count++
			}
			if count == 3 {
				return
			}
			if count > 3 {
				t.Error("dither isn't managing the interval correctly")
			}
			time.Sleep(time.Duration(remain) * time.Second)
		}
	})
}
