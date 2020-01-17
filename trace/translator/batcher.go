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
	"crypto/sha256"
	"fmt"

	"github.com/gogo/protobuf/proto"
	jaegerpb "github.com/jaegertracing/jaeger/model"
)

type bucketID [32]byte

// spanBatcher is simpler version of OpenTelemetry's Node Batcher.
// spanBatcher takes spans and groups them into Jaeger Batches using
// the Span Process objects.
type spanBatcher struct {
	buckets map[bucketID]*jaegerpb.Batch
}

func (b *spanBatcher) add(span *jaegerpb.Span) {
	if b.buckets == nil {
		b.buckets = make(map[bucketID]*jaegerpb.Batch)
	}

	batchByProcess := span.Process
	id, err := b.genBucketID(span.Process)
	if err != nil {
		batchByProcess = nil
	}

	batch := b.getOrAddBatch(id, batchByProcess)
	if batch.Process != nil {
		span.Process = nil
	}
	batch.Spans = append(batch.Spans, span)
}

func (b *spanBatcher) batches() []*jaegerpb.Batch {
	batches := make([]*jaegerpb.Batch, 0, len(b.buckets))
	for _, b := range b.buckets {
		batches = append(batches, b)
	}
	return batches
}

func (b *spanBatcher) genBucketID(process *jaegerpb.Process) (bucketID, error) {
	if process != nil {
		sortTags(process.Tags)
		key, err := proto.Marshal(process)
		if err != nil {
			return bucketID{}, fmt.Errorf("error generating bucket ID: %s", err.Error())
		}
		return sha256.Sum256(key), nil
	}
	return bucketID{}, nil
}

func (b *spanBatcher) getOrAddBatch(id bucketID, p *jaegerpb.Process) *jaegerpb.Batch {
	batch, ok := b.buckets[id]
	if !ok {
		batch = &jaegerpb.Batch{
			Process: p,
		}
		b.buckets[id] = batch
	}
	return batch
}
