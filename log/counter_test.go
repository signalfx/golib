package log
import
(
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	"sync"
)

func TestCounter(t *testing.T) {
	Convey("A counter logger", t, func() {
		c := Counter{}
		Convey("Should start out empty", func() {
			So(c.Count, ShouldEqual, 0)
		})
		Convey("Should work for one log", func() {
			c.Log()
			So(c.Count, ShouldEqual, 1)
		})
		Convey("Should be able to count errors", func() {
			c.ErrorLogger(nil).Log("bad?")
			So(c.Count, ShouldEqual, 1)
		})
		Convey("Should be thread safe", func() {
			numRoutines := 10
			numIter := 10
			wg := sync.WaitGroup{}
			wg.Add(numRoutines)
			for i :=0;i<numRoutines;i++ {
				go func() {
					defer wg.Done()
					for j:=0;j<numIter;j++ {
						c.Log("hello")
					}
				}()
			}
			wg.Wait()
			So(c.Count, ShouldEqual, numRoutines * numIter)
		})
	})
}