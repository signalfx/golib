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
	"testing"

	jaegerpb "github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
)

func TestBatcher(t *testing.T) {
	p1 := &jaegerpb.Process{
		ServiceName: "s1",
		Tags: []jaegerpb.KeyValue{
			{
				Key:   "k1",
				VType: jaegerpb.ValueType_STRING,
				VStr:  "v1",
			},
			{
				Key:   "k2",
				VType: jaegerpb.ValueType_STRING,
				VStr:  "v2",
			},
		},
	}
	p2 := &jaegerpb.Process{
		ServiceName: "s1",
		Tags: []jaegerpb.KeyValue{
			{
				Key:   "k2",
				VType: jaegerpb.ValueType_STRING,
				VStr:  "v2",
			},
			{
				Key:   "k1",
				VType: jaegerpb.ValueType_STRING,
				VStr:  "v1",
			},
		},
	}
	p3 := &jaegerpb.Process{ServiceName: "s2"}
	p4 := &jaegerpb.Process{ServiceName: "s3"}

	spans := []*jaegerpb.Span{
		{Process: p1, SpanID: jaegerpb.SpanID(1)},
		{Process: p1, SpanID: jaegerpb.SpanID(2)},
		{Process: p2, SpanID: jaegerpb.SpanID(3)},
		{Process: p3, SpanID: jaegerpb.SpanID(4)},
		{Process: p3, SpanID: jaegerpb.SpanID(5)},
		{Process: p4, SpanID: jaegerpb.SpanID(6)},
		{Process: p4, SpanID: jaegerpb.SpanID(7)},
		{SpanID: jaegerpb.SpanID(8)},
		{SpanID: jaegerpb.SpanID(9)},
	}

	b := &spanBatcher{}
	for _, s := range spans {
		b.add(s)
	}

	batches := b.batches()
	assert.Len(t, batches, 4)

	b1 := findBatchWithProcessServiceName(batches, p1)
	assert.NotNil(t, b1)
	assert.Equal(t, b1.Process, p1)
	assertSpansAreEqual(t, b1.Spans, []*jaegerpb.Span{spans[0], spans[1], spans[2]})

	b2 := findBatchWithProcessServiceName(batches, p2)
	assert.Equal(t, b1, b2)

	b3 := findBatchWithProcessServiceName(batches, p3)
	assert.NotNil(t, b3)
	assert.Equal(t, b3.Process, p3)
	assertSpansAreEqual(t, b3.Spans, []*jaegerpb.Span{spans[3], spans[4]})

	b4 := findBatchWithProcessServiceName(batches, p4)
	assert.NotNil(t, b4)
	assert.Equal(t, b4.Process, p4)
	assertSpansAreEqual(t, b4.Spans, []*jaegerpb.Span{spans[5], spans[6]})

	var nilProcess *jaegerpb.Process
	b5 := findBatchWithProcessServiceName(batches, nil)
	assert.NotNil(t, b5)
	assert.Equal(t, b5.Process, nilProcess)
	assertSpansAreEqual(t, b5.Spans, []*jaegerpb.Span{spans[7], spans[8]})

	for _, s := range spans {
		assert.Nil(t, s.Process)
	}
}

func findBatchWithProcessServiceName(batches []*jaegerpb.Batch, p *jaegerpb.Process) *jaegerpb.Batch {
	for _, b := range batches {
		if p == nil {
			if b.Process == nil {
				return b
			}
		} else {
			if b.Process != nil && b.Process.ServiceName == p.ServiceName {
				return b
			}
		}
	}
	return nil
}
