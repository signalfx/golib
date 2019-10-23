package sfxclient

import (
	"context"
	"github.com/mailru/easyjson"
	"github.com/open-telemetry/opentelemetry-collector/config/configgrpc"
	"github.com/open-telemetry/opentelemetry-collector/exporter/opencensusexporter"
	"github.com/signalfx/golib/trace"
	"github.com/signalfx/golib/trace/format"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
	"unsafe"
)

func TestRTT(t *testing.T) {
	config := &opencensusexporter.Config{
		GRPCSettings: configgrpc.GRPCSettings{
			// TODO implement local test grpc server to test payload
			Endpoint: "ingest.omnition.io:443",
			Headers: map[string]string{
				"x-omnition-api-key": "TEST",
			},
			Compression: "gzip",
			UseSecure:   true,
		},
		NumWorkers:        1,
		ReconnectionDelay: time.Second * 2,
	}
	sink, err := NewOCSink(config, 3)
	tests := []struct {
		name string
		span string
		err  string
	}{
		{name: "test1", span: test1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			Convey(test.name, t, func() {
				So(err, ShouldBeNil) // from the creation
				x := getSpansFromString(test.span)
				err = sink.AddSpans(context.Background(), x)
				if err != nil {
					So(err.Error(), ShouldEqual, test.err)
				}
			})
		})
	}
}

func getSpansFromString(spans string) []*trace.Span {
	var y traceformat.Trace
	So(easyjson.Unmarshal([]byte(spans), &y), ShouldBeNil)
	x := (*[]*trace.Span)(unsafe.Pointer(&y))
	return *x
}

func TestConvertToOC(t *testing.T) {
	tests := []struct {
		name   string
		span   string
		result string
		err    string
	}{
		//{name: "test1", span: test1},
		//{name: "nil", span: "[null]", err: "non-nil Zipkin span expected"},
		//{name: "bad trace id", span: `[{"traceId":"barg"}]`, err: "traceID: ID is not a 16 or 32 byte hex string"},
		//{name: "bad span id", span: `[{"traceId":"fc1d47a571ad31d5", "spanId":"blarg"}]`, err: "spanID: ID is not a 16 or 32 byte hex string"},
		{name: "bad parent id", span: `[{"traceId":"fc1d47a571ad31d5", "id":"fc1d47a571ad31d5", "parentId":"blarg"}]`, err: "parentSpanID: ID is not a 16 or 32 byte hex string"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			Convey(test.name, t, func() {
				x := getSpansFromString(test.span)
				y, err := internalToTraceSpans(x)
				if err != nil {
					So(err.Error(), ShouldEqual, test.err)
					return
				}
				So(y, ShouldNotBeNil)
			})

		})
	}
}

const test1 = `[{"traceId":"fc1d47a571ad31d5","name":"getMigrationBlocks","parentId":"35ddcf8a1edcc64c","id":"9dc7eec28a74e351","kind":"SERVER","timestamp":1571855061350000,"duration":224,"localEndpoint":{"serviceName":"quantizer-bravo","ipv4":"172.18.42.2"},"tags":{"hostname":"quantizer-bravo-2","jaeger.version":"Java-0.34.0","host":"i-06f8b37cb1f9fb70a","sf_source":"quantizer-bravo-2","sf_metricized":"1","sf_cluster":"lab0","message.type":"2","message.name":"getMigrationBlocks","component":"java-thrift","realm":"lab0","AWSUniqueId":"i-06f8b37cb1f9fb70a_us-east-1_134183635603","message.seqid":"68"}}]`
