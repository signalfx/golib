package log
import "sync/atomic"

type Gate struct {
	DisabledFlag int64
	Logger Logger
}

func (g *Gate) Log(kvs ...interface{}) {
	if !g.Disabled() {
		g.Logger.Log(kvs...)
	}
}

func (g *Gate) Disabled() bool {
	return atomic.LoadInt64(&g.DisabledFlag) == 1 || isDisabled(g.Logger)
}

func (g *Gate) Disable() {
	atomic.StoreInt64(&g.DisabledFlag, 1)
}

func (g *Gate) Enable() {
	atomic.StoreInt64(&g.DisabledFlag, 0)
}