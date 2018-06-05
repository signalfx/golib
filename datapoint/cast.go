package datapoint

import (
	"fmt"
	"reflect"
)

// CastIntegerValue casts a signed integer to a datapoint Value
func CastIntegerValue(value interface{}) (metricValue Value, err error) {
	switch value.(type) {
	case int64:
		metricValue = intWire(value.(int64))
	case int32:
		metricValue = intWire(int64(value.(int32)))
	case int16:
		metricValue = intWire(int64(value.(int16)))
	case int8:
		metricValue = intWire(int64(value.(int8)))
	case int:
		metricValue = intWire(int64(value.(int)))
	default:
		err = fmt.Errorf("unknown metric value type %s", reflect.TypeOf(value))
	}
	return
}

// CastUnsignedIntegerValue casts an unsigned integer to a datapoint Value
func CastUnsignedIntegerValue(value interface{}) (metricValue Value, err error) {
	switch value.(type) {
	case uint64:
		metricValue = intWire(int64(value.(uint64)))
	case uint32:
		metricValue = intWire(int64(value.(uint32)))
	case uint16:
		metricValue = intWire(int64(value.(uint16)))
	case uint8:
		metricValue = intWire(int64(value.(uint8)))
	case uint:
		metricValue = intWire(int64(value.(uint)))
	default:
		err = fmt.Errorf("unknown metric value type %s", reflect.TypeOf(value))
	}
	return
}

// CastFloatValue casts a float to datapoint Value
func CastFloatValue(value interface{}) (metricValue Value, err error) {
	switch value.(type) {
	case float64:
		metricValue = floatWire(value.(float64))
	case float32:
		metricValue = floatWire(float64(value.(float32)))
	default:
		err = fmt.Errorf("unknown metric value type %s", reflect.TypeOf(value))
	}
	return
}

// CastMetricValue casts an interface to datapoint Value
func CastMetricValue(value interface{}) (metricValue Value, err error) {
	switch value.(type) {
	case int64, int32, int16, int8, int:
		metricValue, err = CastIntegerValue(value)
	case uint64, uint32, uint16, uint8, uint:
		metricValue, err = CastUnsignedIntegerValue(value)
	case float64, float32:
		metricValue, err = CastFloatValue(value)
	default:
		err = fmt.Errorf("unknown metric value type %s", reflect.TypeOf(value))
	}
	return
}
