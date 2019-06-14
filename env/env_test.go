package env

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/signalfx/golib/pointer"
	"github.com/smartystreets/goconvey/convey"
)

const testKey = "SFX_TEST_ENV_VAR"

func TestGetStringEnvVar(t *testing.T) {
	convey.Convey("getStringEnvVar", t, func() {
		convey.Convey("should return the value if the environment variable is set", func() {
			testVal := "testStringValue"
			os.Setenv(testKey, testVal)
			loaded := GetStringEnvVar(testKey, pointer.String("defaultVal"))
			convey.So(*loaded, convey.ShouldEqual, testVal)
		})
		convey.Convey("should return the default value if the environment variable is not set", func() {
			testVal := pointer.String("defaultVal")
			loaded := GetStringEnvVar(testKey, testVal)
			convey.So(loaded, convey.ShouldNotEqual, testVal)
			convey.So(*loaded, convey.ShouldEqual, "defaultVal")
		})
		convey.Convey("should return nil if the default value is nil and the environment variable is not set", func() {
			loaded := GetStringEnvVar(testKey, nil)
			convey.So(loaded, convey.ShouldEqual, nil)
		})
		convey.Reset(func() {
			os.Unsetenv(testKey)
		})
	})
}

func TestGetDurationEnvVar(t *testing.T) {
	convey.Convey("getDurationEnvVar", t, func() {
		convey.Convey("should return the value if the environment variable is set", func() {
			testVal := "5s"
			os.Setenv(testKey, testVal)
			loaded := GetDurationEnvVar(testKey, pointer.Duration(0*time.Second))
			convey.So(*loaded, convey.ShouldEqual, time.Second*5)
		})
		convey.Convey("should return the default value if the environment variable is not set", func() {
			testVal := pointer.Duration(1 * time.Second)
			loaded := GetDurationEnvVar(testKey, testVal)
			convey.So(loaded, convey.ShouldNotEqual, testVal)
			convey.So(*loaded, convey.ShouldEqual, time.Second*1)
		})
		convey.Convey("should return nil if the default value is nil and the environment variable is not set", func() {
			loaded := GetDurationEnvVar(testKey, nil)
			convey.So(loaded, convey.ShouldEqual, nil)
		})
		convey.Reset(func() {
			os.Unsetenv(testKey)
		})
	})
}

func TestGetUint64EnvVar(t *testing.T) {
	convey.Convey("getUint64EnvVar", t, func() {
		convey.Convey("should return the value if the environment variable is set", func() {
			testVal := "5"
			os.Setenv(testKey, testVal)
			loaded := GetUint64EnvVar(testKey, pointer.Uint64(1))
			convey.So(*loaded, convey.ShouldEqual, 5)
		})
		convey.Convey("should return the default value if the environment variable is not set", func() {
			testVal := pointer.Uint64(1)
			loaded := GetUint64EnvVar(testKey, testVal)
			convey.So(loaded, convey.ShouldNotEqual, testVal)
			convey.So(*loaded, convey.ShouldEqual, 1)
		})
		convey.Convey("should return nil if the default value is nil and the environment variable is not set", func() {
			loaded := GetUint64EnvVar(testKey, nil)
			convey.So(loaded, convey.ShouldEqual, nil)
		})
		convey.Reset(func() {
			os.Unsetenv(testKey)
		})
	})
}

func TestGetUintEnvVar(t *testing.T) {
	convey.Convey("getUintEnvVar", t, func() {
		convey.Convey("should return the value if the environment variable is set", func() {
			testVal := "10"
			os.Setenv(testKey, testVal)
			loaded := GetUintEnvVar(testKey, pointer.Uint(5))
			convey.So(*loaded, convey.ShouldEqual, 10)
		})
		convey.Convey("should return the default value if the environment variable is not set", func() {
			testVal := pointer.Uint(1)
			loaded := GetUintEnvVar(testKey, testVal)
			convey.So(loaded, convey.ShouldNotEqual, testVal)
			convey.So(*loaded, convey.ShouldEqual, 1)
		})
		convey.Convey("should return nil if the default value is nil and the environment variable is not set", func() {
			loaded := GetUintEnvVar(testKey, nil)
			convey.So(loaded, convey.ShouldEqual, nil)
		})
		convey.Reset(func() {
			os.Unsetenv(testKey)
		})
	})
}

func TestGetCommaSeparatedStringEnvVar(t *testing.T) {
	convey.Convey("GetCommaSeparatedStringEnvVar", t, func() {
		convey.Convey("should parse comma separated strings", func() {
			testVal := []string{"127.0.0.1:9999", "127.0.0.2:9999", "127.0.0.3:9999"}
			os.Setenv(testKey, strings.Join(testVal, ","))
			loaded := GetCommaSeparatedStringEnvVar(testKey, []string{})
			convey.So(len(loaded), convey.ShouldEqual, 3)
			convey.So(strings.Join(loaded, ","), convey.ShouldEqual, strings.Join(testVal, ","))
		})
		convey.Convey("should return the default value if the environment variable is not set", func() {
			defaultVal := []string{"one", "two", "three"}
			loaded := GetCommaSeparatedStringEnvVar(testKey, defaultVal)
			convey.So(len(loaded), convey.ShouldEqual, 3)
			convey.So(strings.Join(loaded, ","), convey.ShouldEqual, strings.Join(defaultVal, ","))
		})
		convey.Reset(func() {
			os.Unsetenv(testKey)
		})
	})
}
