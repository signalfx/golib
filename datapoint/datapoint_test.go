package datapoint

import (
	"encoding/json"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestDatapointHelperFunctions(t *testing.T) {
	dp := New("aname", map[string]string{}, nil, Gauge, time.Now())
	assert.Contains(t, dp.String(), "aname")
}

func TestDatapointJSONDecode(t *testing.T) {

	datapointInOut := func(dpIn *Datapoint) Datapoint {
		var dpOut Datapoint
		b, err := json.Marshal(dpIn)
		So(err, ShouldBeNil)
		So(json.Unmarshal(b, &dpOut), ShouldBeNil)
		So(dpIn.Metric, ShouldEqual, dpOut.Metric)
		So(dpIn.Dimensions, ShouldResemble, dpOut.Dimensions)
		So(dpIn.MetricType, ShouldEqual, dpOut.MetricType)
		So(dpIn.Timestamp.Nanosecond(), ShouldEqual, dpOut.Timestamp.Nanosecond())
		So(dpIn.Value, ShouldEqual, dpOut.Value)
		return dpOut
	}

	Convey("Integer datapoints encode/decode correctly", t, func() {
		start := time.Now()
		dpIn := New("test", map[string]string{"a": "b"}, NewIntValue(123), Gauge, start)
		So(datapointInOut(dpIn).Value.(IntValue).Int(), ShouldEqual, 123)
	})

	Convey("Float datapoints encode/decode correctly", t, func() {
		start := time.Now()
		dpIn := New("test", map[string]string{"a": "b"}, NewFloatValue(.5), Gauge, start)
		So(datapointInOut(dpIn).Value.(FloatValue).Float(), ShouldEqual, .5)
	})

	Convey("String datapoints encode/decode correctly", t, func() {
		start := time.Now()
		dpIn := New("test", map[string]string{"a": "b"}, NewStringValue("hi"), Gauge, start)
		So(datapointInOut(dpIn).Value.(StringValue).String(), ShouldEqual, "hi")
	})
}

func TestDatapointInvalidJSONDecode(t *testing.T) {
	Convey("Invalid JSON decodes should error", t, func() {
		var dpOut Datapoint
		So((&dpOut).UnmarshalJSON([]byte("INVALID_JSON")), ShouldNotBeNil)
	})
}

func TestAddDatapoints(t *testing.T) {
	Convey("Adding datapoints", t, func() {
		m1 := map[string]string{"name": "jack"}
		m2 := map[string]string{"name": "john"}
		So(len(AddMaps(nil, nil)), ShouldEqual, 0)
		So(AddMaps(m1, nil), ShouldEqual, m1)
		So(AddMaps(nil, m2), ShouldEqual, m2)
		So(AddMaps(m1, m2)["name"], ShouldEqual, "john")
	})
}
