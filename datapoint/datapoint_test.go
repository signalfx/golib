package datapoint

import (
	"encoding/json"
	"strings"
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
		dpIn := New("test", map[string]string{"a": "b"}, NewFloatValue(.5), Count, start)
		So(datapointInOut(dpIn).Value.(FloatValue).Float(), ShouldEqual, .5)
	})

	Convey("String datapoints encode/decode correctly", t, func() {
		start := time.Now()
		dpIn := New("test", map[string]string{"a": "b"}, NewStringValue("hi"), Counter, start)
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

func TestDatapointProperties(t *testing.T) {
	Convey("Given a datapoint", t, func() {
		dp := New("datapoint", map[string]string{}, NewIntValue(10), Gauge, time.Now())
		Convey("GetProperties should return nil", func() {
			So(dp.GetProperties(), ShouldBeNil)
		})

		Convey("When you add a key value", func() {
			dp.SetProperty("foo", "bar")
			Convey("GetProperties should return map with key value", func() {
				So(dp.GetProperties(), ShouldResemble, map[string]interface{}{"foo": "bar"})
			})
			Convey("and then remove it", func() {
				dp.RemoveProperty("foo")
				Convey("GetProperties should return nil", func() {
					So(dp.GetProperties(), ShouldBeNil)
				})
			})
		})

		Convey("When You remove a missing key", func() {
			dp.RemoveProperty("foo1")
			Convey("GetProperties should return nil", func() {
				So(dp.GetProperties(), ShouldBeNil)
			})
		})
	})
}

func TestMetricTypeMarshaller(t *testing.T) {
	metricTypeToStringMap := map[MetricType]string{
		Gauge:     "gauge",
		Count:     "counter",
		Enum:      "enum",
		Counter:   "cumulative counter",
		Rate:      "rate",
		Timestamp: "timestamp",
	}

	Convey("MetricType Stringer", t, func() {
		Convey("shall handle standard values", func() {
			for metricType, expectedText := range metricTypeToStringMap {
				So(metricType.String(), ShouldEqual, expectedText)
			}
		})
		Convey("shall handle out of range values", func() {
			So(MetricType(123).String(), ShouldEqual, "MetricType(123)")
		})
	})

	Convey("MetricType Text Unmarshaller", t, func() {
		Convey("shall handle standard values", func() {
			for expectedMetricType, text := range metricTypeToStringMap {
				var mt MetricType
				err := mt.UnmarshalText([]byte(text))
				So(err, ShouldBeNil)
				So(mt, ShouldEqual, expectedMetricType)
			}
		})
		Convey("shall handle out of range values", func() {
			var mt MetricType
			So(mt.UnmarshalText([]byte("???")), ShouldNotBeNil)
		})
	})

	Convey("MetricType JSON Unmarshaller", t, func() {
		Convey("invalid metric type should error", func() {
			dp := New("", nil, NewIntValue(1), Gauge, time.Now())
			validJSON, _ := json.Marshal(dp)
			invalidJSON := strings.Replace(string(validJSON), "metric_type\":0", "metric_type\":\"???\"", 1)
			So(json.Unmarshal([]byte(invalidJSON), &dp), ShouldNotBeNil)
		})
	})
}
