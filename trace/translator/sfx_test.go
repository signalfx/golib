// Copyright 2019 Splunk, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package translator

import (
	"sort"
	"testing"
	"time"

	jaegerpb "github.com/jaegertracing/jaeger/model"
	"github.com/signalfx/golib/v3/pointer"
	"github.com/signalfx/golib/v3/trace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sapmpb "github.com/signalfx/sapm-proto/gen"
)

var (
	ClientKind   = "CLIENT"
	ServerKind   = "SERVER"
	ProducerKind = "PRODUCER"
	ConsumerKind = "CONSUMER"
)

func TestTimeTranslator(t *testing.T) {
	timeInMicros := int64(1575986204988181)

	want, err := time.Parse(time.RFC3339Nano, "2019-12-10T13:56:44.988181Z")
	require.NoError(t, err)

	got := timeFromMicrosecondsSinceEpoch(timeInMicros).UTC()
	assert.Equal(t, want, got)
}

func TestDurationTranslator(t *testing.T) {
	cases := map[int64]time.Duration{
		1:      time.Microsecond,
		6e+7:   time.Minute,
		3.6e+9: time.Hour,
	}

	for ms, want := range cases {
		got := durationFromMicroseconds(ms)
		assert.Equal(t, want, got)
	}
}

func TestTranslator(t *testing.T) {
	got := SFXToSAPMPostRequest(sourceSpans)
	require.Equal(t, len(wantPostRequest.Batches), len(got.Batches))
	sortBatches(wantPostRequest.Batches)
	sortBatches(got.Batches)

	for i := 0; i < len(got.Batches); i++ {
		assertBatchesAreEqual(t, got.Batches[i], wantPostRequest.Batches[i])
	}

}

func assertBatchesAreEqual(t *testing.T, got, want *jaegerpb.Batch) {
	require.Equal(t, len(got.Spans), len(want.Spans))
	assertProcessesAreEqual(t, got.Process, want.Process)
	assertSpansAreEqual(t, got.Spans, want.Spans)

}

func assertProcessesAreEqual(t *testing.T, got, want *jaegerpb.Process) {
	sortTags(want.Tags)
	sortTags(got.Tags)
	assert.Equal(t, got, want)
}

func assertSpansAreEqual(t *testing.T, got, want []*jaegerpb.Span) {
	sortSpans(got)
	sortSpans(want)
	for i := 0; i < len(got); i++ {
		require.Equal(t, sortedSpan(got[i]), sortedSpan(want[i]))
	}
}

func sortBatches(batches []*jaegerpb.Batch) {
	sort.Slice(batches, func(i, j int) bool {
		s1, s2 := "", ""
		if batches[i].Process != nil {
			s1 = batches[i].Process.ServiceName
		}
		if batches[j].Process != nil {
			s2 = batches[j].Process.ServiceName
		}
		return s1 <= s2
	})
}

func sortSpans(spans []*jaegerpb.Span) {
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].SpanID < spans[j].SpanID
	})
}

func sortRefs(t []jaegerpb.SpanRef) {
	sort.Slice(t, func(i, j int) bool {
		return t[i].String() <= t[j].String()
	})
}

func sortedSpan(s *jaegerpb.Span) *jaegerpb.Span {
	sortLogs(s.Logs)
	sortTags(s.Tags)
	sortRefs(s.References)
	sort.Strings(s.Warnings)
	return s
}

func sortLogs(t []jaegerpb.Log) {
	sort.Slice(t, func(i, j int) bool {
		return t[i].String() <= t[j].String()
	})
	for _, l := range t {
		sortTags(l.Fields)
	}
}

var wantPostRequest = sapmpb.PostSpansRequest{
	Batches: []*jaegerpb.Batch{
		{
			Process: &jaegerpb.Process{
				ServiceName: "api1",
				Tags: []jaegerpb.KeyValue{
					{
						Key:   "ip",
						VType: jaegerpb.ValueType_STRING,
						VStr:  "10.53.69.61",
					},
					{
						Key:   "hostname",
						VType: jaegerpb.ValueType_STRING,
						VStr:  "api246-sjc1",
					},
					{
						Key:   "jaeger.version",
						VType: jaegerpb.ValueType_STRING,
						VStr:  "Python-3.1.0",
					},
				},
			},
			Spans: []*jaegerpb.Span{
				{
					SpanID:        jaegerpb.SpanID(0x147d98),
					TraceID:       jaegerpb.TraceID{Low: 11715721395283892799},
					OperationName: "get",
					StartTime:     time.Date(2017, 01, 26, 21, 46, 31, 639875000, time.UTC),
					Duration:      time.Duration(22938000),
					Flags:         0,
					Process:       nil,
					ProcessID:     "",
					References: []jaegerpb.SpanRef{
						{
							TraceID: jaegerpb.TraceID{Low: 11715721395283892799},
							SpanID:  jaegerpb.SpanID(0x68c4e3),
							RefType: jaegerpb.SpanRefType_CHILD_OF,
						},
					},
					Tags: []jaegerpb.KeyValue{
						{
							Key:   "peer.ipv4",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "192.53.69.61",
						},
						{
							Key:    "peer.port",
							VType:  jaegerpb.ValueType_INT64,
							VInt64: 53931,
						},
						{
							Key:   "span.kind",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "server",
						},
						{
							Key:   "someFalseBool",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "false",
						},
						{
							Key:   "someDouble",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "129.8",
						},
						{
							Key:   "http.url",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "http://127.0.0.1:15598/client_transactions",
						},
						{
							Key:   "someBool",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "true",
						},
					},
					Logs: []jaegerpb.Log{
						{
							Timestamp: time.Date(2017, 01, 26, 21, 46, 31, 639875000, time.UTC),
							Fields: []jaegerpb.KeyValue{
								{
									Key:   "key1",
									VType: jaegerpb.ValueType_STRING,
									VStr:  "value1",
								},
								{
									Key:   "key2",
									VType: jaegerpb.ValueType_STRING,
									VStr:  "value2",
								},
							},
						},
					},
					Warnings: nil,
				},
				{
					TraceID:       jaegerpb.TraceID{Low: 12868642899890739775, High: 1},
					SpanID:        jaegerpb.SpanID(0x21d092272e),
					OperationName: "post",
					StartTime:     time.Date(2017, 01, 26, 21, 46, 31, 639875000, time.UTC),
					Duration:      time.Microsecond * 22938,
					References: []jaegerpb.SpanRef{{
						TraceID: jaegerpb.TraceID{Low: 12868642899890739775, High: 1},
						SpanID:  jaegerpb.SpanID(6866147),
						RefType: jaegerpb.SpanRefType_CHILD_OF,
					}},
					Tags: []jaegerpb.KeyValue{
						{
							Key:   "span.kind",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "client",
						},
						{
							Key:    "peer.port",
							VType:  jaegerpb.ValueType_INT64,
							VInt64: 53931,
						},
						{
							Key:   "peer.ipv4",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "10.0.0.1",
						},
					},
					Logs: []jaegerpb.Log{},
				}, {
					TraceID:       jaegerpb.TraceID{Low: 14021564404497586751},
					SpanID:        jaegerpb.SpanID(213952636718),
					OperationName: "post",
					StartTime:     time.Date(2017, 01, 26, 21, 46, 31, 639875000, time.UTC),
					Duration:      time.Microsecond * 22938,
					References: []jaegerpb.SpanRef{{
						TraceID: jaegerpb.TraceID{Low: 14021564404497586751},
						SpanID:  jaegerpb.SpanID(6866147),
						RefType: jaegerpb.SpanRefType_CHILD_OF,
					}},
					Tags: []jaegerpb.KeyValue{
						{
							Key:   "span.kind",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "consumer",
						},
					},
					Logs: []jaegerpb.Log{},
				}, {

					TraceID:       jaegerpb.TraceID{Low: 15174485909104433727},
					SpanID:        jaegerpb.SpanID(47532398882098234),
					OperationName: "post",
					StartTime:     time.Date(2017, 01, 26, 21, 46, 31, 639875000, time.UTC),
					Duration:      time.Microsecond * 22938,
					Flags:         2,
					References: []jaegerpb.SpanRef{
						{
							RefType: jaegerpb.SpanRefType_CHILD_OF,
							TraceID: jaegerpb.TraceID{Low: 15174485909104433727},
							SpanID:  jaegerpb.SpanID(6866147),
						},
					},
					Tags: []jaegerpb.KeyValue{
						{
							Key:   "span.kind",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "producer",
						},
						{
							Key:   "peer.ipv6",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "::1",
						},
					},
					Logs: []jaegerpb.Log{},
				}, {

					TraceID:       jaegerpb.TraceID{Low: 16327407413711280703},
					SpanID:        jaegerpb.SpanID(52035998509468730),
					OperationName: "post",
					StartTime:     time.Date(2017, 01, 26, 21, 46, 31, 639875000, time.UTC),
					Duration:      time.Microsecond * 22938,
					Flags:         2,
					References: []jaegerpb.SpanRef{
						{
							RefType: jaegerpb.SpanRefType_CHILD_OF,
							TraceID: jaegerpb.TraceID{Low: 16327407413711280703},
							SpanID:  jaegerpb.SpanID(7914723),
						},
					},
					Tags: []jaegerpb.KeyValue{
						{
							Key:   "span.kind",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "producer",
						},
						{
							Key:   "peer.ipv6",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "::1",
						},
						{
							Key:   "elements",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "100",
						},
					},
					Logs: []jaegerpb.Log{},
				},
			},
		}, {
			Process: &jaegerpb.Process{
				ServiceName: "api2",
				Tags: []jaegerpb.KeyValue{
					{
						Key:   "ip",
						VType: jaegerpb.ValueType_STRING,
						VStr:  "10.53.69.70",
					},
					{
						Key:   "jaeger.version",
						VType: jaegerpb.ValueType_STRING,
						VStr:  "Python-3.6.0",
					},
					{
						Key:   "hostname",
						VType: jaegerpb.ValueType_STRING,
						VStr:  "api2-233",
					},
				},
			},
			Spans: []*jaegerpb.Span{
				{
					TraceID:       jaegerpb.TraceID{Low: 17480328918319176255},
					SpanID:        jaegerpb.SpanID(58525199627357242),
					OperationName: "post",
					StartTime:     time.Date(2017, 01, 26, 21, 46, 31, 639875000, time.UTC),
					Duration:      time.Microsecond * 22938,
					Flags:         2,
					References: []jaegerpb.SpanRef{
						{
							RefType: jaegerpb.SpanRefType_CHILD_OF,
							TraceID: jaegerpb.TraceID{Low: 17480328918319176255},
							SpanID:  jaegerpb.SpanID(0x35c4e2),
						},
					},
					Tags: []jaegerpb.KeyValue{
						{
							Key:   "span.kind",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "producer",
						},
						{
							Key:   "peer.ipv6",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "::1",
						},
					},
					Logs: []jaegerpb.Log{},
				},
			},
		}, {
			Process: &jaegerpb.Process{
				ServiceName: "api3",
				Tags: []jaegerpb.KeyValue{
					{
						Key:   "ip",
						VType: jaegerpb.ValueType_STRING,
						VStr:  "10.53.67.53",
					},
					{
						Key:   "jaeger.version",
						VType: jaegerpb.ValueType_STRING,
						VStr:  "Python-3.6.0",
					},
					{
						Key:   "hostname",
						VType: jaegerpb.ValueType_STRING,
						VStr:  "api3-sjc1",
					},
				},
			},
			Spans: []*jaegerpb.Span{
				{
					TraceID:       jaegerpb.TraceID{Low: 18025686685695023674},
					SpanID:        jaegerpb.SpanID(63028799254727738),
					OperationName: "get",
					StartTime:     time.Date(2017, 01, 26, 21, 46, 31, 639875000, time.UTC),
					Duration:      time.Microsecond * 22938,
					Tags: []jaegerpb.KeyValue{
						{
							Key:   "span.kind",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "client",
						},
						{
							Key:   "peer.ipv6",
							VType: jaegerpb.ValueType_STRING,
							VStr:  "::1",
						},
					},
					Logs: []jaegerpb.Log{},
				},
			},
		},
	},
}

var sourceSpans = []*trace.Span{
	{
		TraceID:  "a2969a8955571a3f",
		ParentID: pointer.String("000000000068c4e3"),
		ID:       "0000000000147d98",
		Name:     pointer.String("get"),
		Kind:     &ServerKind,
		LocalEndpoint: &trace.Endpoint{
			ServiceName: pointer.String("api1"),
			Ipv4:        pointer.String("10.53.69.61"),
		},
		RemoteEndpoint: &trace.Endpoint{
			ServiceName: pointer.String("rtapi"),
			Ipv4:        pointer.String("192.53.69.61"),
			Port:        pointer.Int32(53931),
		},
		Timestamp: pointer.Int64(1485467191639875),
		Duration:  pointer.Int64(22938),
		Debug:     nil,
		Shared:    nil,
		Annotations: []*trace.Annotation{
			{Timestamp: pointer.Int64(1485467191639875), Value: pointer.String("{\"key1\":\"value1\",\"key2\":\"value2\"}")},
			{Timestamp: pointer.Int64(1485467191639875), Value: pointer.String("nothing")},
		},
		Tags: map[string]string{
			"http.url":       "http://127.0.0.1:15598/client_transactions",
			"someBool":       "true",
			"someFalseBool":  "false",
			"someDouble":     "129.8",
			"hostname":       "api246-sjc1",
			"jaeger.version": "Python-3.1.0",
		},
	},
	{
		TraceID:  "0000000000000001b2969a8955571a3f",
		ParentID: pointer.String("000000000068c4e3"),
		ID:       "00000021d092272e",
		Name:     pointer.String("post"),
		Kind:     &ClientKind,
		LocalEndpoint: &trace.Endpoint{
			ServiceName: pointer.String("api1"),
			Ipv4:        pointer.String("10.53.69.61"),
		},
		RemoteEndpoint: &trace.Endpoint{
			Ipv4: pointer.String("10.0.0.1"),
			Port: pointer.Int32(53931),
		},
		Timestamp:   pointer.Int64(1485467191639875),
		Duration:    pointer.Int64(22938),
		Debug:       nil,
		Shared:      nil,
		Annotations: []*trace.Annotation{},
		Tags: map[string]string{
			"hostname":       "api246-sjc1",
			"jaeger.version": "Python-3.1.0",
		},
	},
	{
		TraceID:  "c2969a8955571a3f",
		ParentID: pointer.String("000000000068c4e3"),
		ID:       "0031d092272e",
		Name:     pointer.String("post"),
		Kind:     &ConsumerKind,
		LocalEndpoint: &trace.Endpoint{
			ServiceName: pointer.String("api1"),
			Ipv4:        pointer.String("10.53.69.61"),
		},
		RemoteEndpoint: nil,
		Timestamp:      pointer.Int64(1485467191639875),
		Duration:       pointer.Int64(22938),
		Debug:          nil,
		Shared:         nil,
		Annotations:    []*trace.Annotation{},
		Tags: map[string]string{
			"hostname":       "api246-sjc1",
			"jaeger.version": "Python-3.1.0",
		},
	},
	{
		TraceID:  "d2969a8955571a3f",
		ParentID: pointer.String("000000000068c4e3"),
		ID:       "A8DE7706B08C3A",
		Name:     pointer.String("post"),
		Kind:     &ProducerKind,
		LocalEndpoint: &trace.Endpoint{
			ServiceName: pointer.String("api1"),
			Ipv4:        pointer.String("10.53.69.61"),
		},
		RemoteEndpoint: &trace.Endpoint{
			Ipv6: pointer.String("::1"),
		},
		Timestamp:   pointer.Int64(1485467191639875),
		Duration:    pointer.Int64(22938),
		Debug:       pointer.Bool(true),
		Shared:      nil,
		Annotations: []*trace.Annotation{},
		Tags: map[string]string{
			"hostname":       "api246-sjc1",
			"jaeger.version": "Python-3.1.0",
		},
	},
	{
		TraceID:  "e2969a8955571a3f",
		ParentID: pointer.String("000000000078c4e3"),
		ID:       "B8DE7706B08C3A",
		Name:     pointer.String("post"),
		Kind:     &ProducerKind,
		LocalEndpoint: &trace.Endpoint{
			ServiceName: pointer.String("api1"),
			Ipv4:        pointer.String("10.53.69.61"),
		},
		RemoteEndpoint: &trace.Endpoint{
			Ipv6: pointer.String("::1"),
		},
		Timestamp:   pointer.Int64(1485467191639875),
		Duration:    pointer.Int64(22938),
		Debug:       pointer.Bool(true),
		Shared:      nil,
		Annotations: []*trace.Annotation{},
		Tags: map[string]string{
			"elements":       "100",
			"hostname":       "api246-sjc1",
			"jaeger.version": "Python-3.1.0",
		},
	},
	{
		TraceID:  "f2969a8955671a3f",
		ParentID: pointer.String("000000000035c4e2"),
		ID:       "CFEC5BE6328C3A",
		Name:     pointer.String("post"),
		Kind:     &ProducerKind,
		LocalEndpoint: &trace.Endpoint{
			ServiceName: pointer.String("api2"),
			Ipv4:        pointer.String("10.53.69.70"),
		},
		RemoteEndpoint: &trace.Endpoint{
			Ipv6: pointer.String("::1"),
		},
		Debug:     pointer.Bool(true),
		Timestamp: pointer.Int64(1485467191639875),
		Duration:  pointer.Int64(22938),
		Tags: map[string]string{
			"hostname":       "api2-233",
			"jaeger.version": "Python-3.6.0",
		},
	},
	{
		TraceID: "fa281a8955571a3a",
		ID:      "DFEC5BE6328C3A",
		Name:    pointer.String("get"),
		Kind:    &ClientKind,
		LocalEndpoint: &trace.Endpoint{
			ServiceName: pointer.String("api3"),
			Ipv4:        pointer.String("10.53.67.53"),
		},
		RemoteEndpoint: &trace.Endpoint{
			Ipv6: pointer.String("::1"),
		},
		Timestamp: pointer.Int64(1485467191639875),
		Duration:  pointer.Int64(22938),
		Tags: map[string]string{
			"hostname":       "api3-sjc1",
			"jaeger.version": "Python-3.6.0",
		},
	},
}
