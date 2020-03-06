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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	jaegerpb "github.com/jaegertracing/jaeger/model"
	"github.com/signalfx/golib/v3/trace"

	gen "github.com/signalfx/sapm-proto/gen"
)

const (
	clientKind   = "CLIENT"
	serverKind   = "SERVER"
	producerKind = "PRODUCER"
	consumerKind = "CONSUMER"

	tagJaegerVersion = "jaeger.version"
	tagIP            = "ip"
	tagHostname      = "hostname"

	nanosInOneMicro = time.Microsecond
)

// SFXToSAPMPostRequest takes a slice spans in the SignalFx format and converts it to a SAPM PostSpansRequest
func SFXToSAPMPostRequest(spans []*trace.Span) *gen.PostSpansRequest {
	sr := &gen.PostSpansRequest{}

	batcher := SpanBatcher{}

	for _, sfxSpan := range spans {
		span := SAPMSpanFromSFXSpan(sfxSpan)
		batcher.Add(span)
	}

	sr.Batches = batcher.Batches()
	return sr
}

// GetLocalEndpointInfo sets the jaeger span's local endpoint extracted from the SignalFx span
func GetLocalEndpointInfo(sfxSpan *trace.Span, span *jaegerpb.Span) {
	if sfxSpan.LocalEndpoint != nil {
		if sfxSpan.LocalEndpoint.ServiceName != nil {
			span.Process.ServiceName = *sfxSpan.LocalEndpoint.ServiceName
		}
		if sfxSpan.LocalEndpoint.Ipv4 != nil {
			span.Process.Tags = append(span.Process.Tags, jaegerpb.KeyValue{
				Key:   "ip",
				VType: jaegerpb.ValueType_STRING,
				VStr:  *sfxSpan.LocalEndpoint.Ipv4,
			})
		}
	}
}

// SAPMSpanFromSFXSpan converts an individual SignalFx format span to a SAPM span
func SAPMSpanFromSFXSpan(sfxSpan *trace.Span) (span *jaegerpb.Span) {
	spanID, err := jaegerpb.SpanIDFromString(sfxSpan.ID)
	if err == nil {
		traceID, err := jaegerpb.TraceIDFromString(sfxSpan.TraceID)
		if err == nil {
			span = &jaegerpb.Span{
				SpanID:  spanID,
				TraceID: traceID,
				Process: &jaegerpb.Process{},
			}

			if sfxSpan.Name != nil {
				span.OperationName = *sfxSpan.Name
			}

			if sfxSpan.Duration != nil {
				span.Duration = DurationFromMicroseconds(*sfxSpan.Duration)
			}

			if sfxSpan.Timestamp != nil {
				span.StartTime = TimeFromMicrosecondsSinceEpoch(*sfxSpan.Timestamp)
			}

			if sfxSpan.Debug != nil && *sfxSpan.Debug {
				span.Flags.SetDebug()
			}

			span.Tags, span.Process.Tags = SFXTagsToJaegerTags(sfxSpan.Tags, sfxSpan.RemoteEndpoint, sfxSpan.Kind)

			GetLocalEndpointInfo(sfxSpan, span)

			if sfxSpan.ParentID != nil {
				parentID, err := jaegerpb.SpanIDFromString(*sfxSpan.ParentID)
				if err == nil {
					span.References = append(span.References, jaegerpb.SpanRef{
						TraceID: traceID,
						SpanID:  parentID,
						RefType: jaegerpb.SpanRefType_CHILD_OF,
					})
				}
			}

			span.Logs = sfxAnnotationsToJaegerLogs(sfxSpan.Annotations)
		}
	}
	return span
}

// SFXTagsToJaegerTags returns process tags and span tags from the SignalFx span tags, endpoint (remote), and kind
func SFXTagsToJaegerTags(tags map[string]string, remoteEndpoint *trace.Endpoint, kind *string) ([]jaegerpb.KeyValue, []jaegerpb.KeyValue) {
	processTags := make([]jaegerpb.KeyValue, 0, len(tags))
	spanTags := make([]jaegerpb.KeyValue, 0, len(tags)+3)

	if remoteEndpoint != nil {
		if remoteEndpoint.Ipv4 != nil {
			spanTags = append(spanTags, jaegerpb.KeyValue{
				Key:   peerHostIPv4,
				VType: jaegerpb.ValueType_STRING,
				VStr:  *remoteEndpoint.Ipv4,
			})
		}

		if remoteEndpoint.Ipv6 != nil {
			spanTags = append(spanTags, jaegerpb.KeyValue{
				Key:   peerHostIPv6,
				VType: jaegerpb.ValueType_STRING,
				VStr:  *remoteEndpoint.Ipv6,
			})
		}

		if remoteEndpoint.Port != nil {
			spanTags = append(spanTags, jaegerpb.KeyValue{
				Key:    peerPort,
				VType:  jaegerpb.ValueType_INT64,
				VInt64: int64(*remoteEndpoint.Port),
			})
		}
	}

	if kind != nil {
		kindTag, err := sfxKindToJaeger(*kind)
		if err == nil {
			spanTags = append(spanTags, kindTag)
		}
	}

	for k, v := range tags {
		kv := jaegerpb.KeyValue{
			Key:   k,
			VType: jaegerpb.ValueType_STRING,
			VStr:  v,
		}
		switch k {
		case tagJaegerVersion, tagHostname, tagIP:
			processTags = append(processTags, kv)
		default:
			spanTags = append(spanTags, kv)
		}
	}

	return spanTags, processTags
}

func sfxAnnotationsToJaegerLogs(annotations []*trace.Annotation) []jaegerpb.Log {
	logs := make([]jaegerpb.Log, 0, len(annotations))
	for _, ann := range annotations {
		if ann.Value != nil {
			log := jaegerpb.Log{}
			if ann.Timestamp != nil {
				log.Timestamp = TimeFromMicrosecondsSinceEpoch(*ann.Timestamp)
			}
			var err error
			log.Fields, err = FieldsFromJSONString(*ann.Value)
			if err != nil {
				continue
			}
			logs = append(logs, log)
		}
	}
	return logs
}

// FieldsFromJSONString returns an array of jaeger KeyValues from a json string
func FieldsFromJSONString(jStr string) ([]jaegerpb.KeyValue, error) {
	fields := make(map[string]string)
	kv := make([]jaegerpb.KeyValue, 0, len(fields))
	err := json.Unmarshal([]byte(jStr), &fields)
	if err != nil {
		kv = append(kv, jaegerpb.KeyValue{
			Key:   "event",
			VType: jaegerpb.ValueType_STRING,
			VStr:  jStr,
		})
		return kv, err
	}

	for k, v := range fields {
		kv = append(kv, jaegerpb.KeyValue{
			Key:   k,
			VType: jaegerpb.ValueType_STRING,
			VStr:  v,
		})
	}
	return kv, nil
}

func sfxKindToJaeger(kind string) (jaegerpb.KeyValue, error) {
	kv := jaegerpb.KeyValue{
		Key: spanKind,
	}

	// Normalize to uppercase before checking against uppercase constant values
	kind = strings.ToUpper(kind)
	switch kind {
	case clientKind:
		kv.VStr = spanKindRPCClient
	case serverKind:
		kv.VStr = spanKindRPCServer
	case producerKind:
		kv.VStr = spanKindProducer
	case consumerKind:
		kv.VStr = spanKindConsumer
	default:
		return kv, fmt.Errorf("unknown span kind %s", kind)
	}
	return kv, nil
}

// DurationFromMicroseconds returns the number of microseconds as a duration
func DurationFromMicroseconds(micros int64) time.Duration {
	return time.Duration(micros) * nanosInOneMicro
}

// TimeFromMicrosecondsSinceEpoch returns the number of microseconds since the epoch as a time.Time
func TimeFromMicrosecondsSinceEpoch(micros int64) time.Time {
	nanos := micros * int64(nanosInOneMicro)
	return time.Unix(0, nanos).UTC()
}

func sortTags(t []jaegerpb.KeyValue) {
	if t == nil {
		return
	}
	sort.Slice(t, func(i, j int) bool {
		return t[i].Key <= t[j].Key
	})
}
