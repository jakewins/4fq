package queue_test

import (
	"github.com/jakewins/4fq/pkg/queue"
	"sync/atomic"
	"testing"
)

func TestSPSCBasic(t *testing.T) {
	q, _ := queue.NewSingleProducerSingleConsumer(queue.Options{})

	slot, _ := q.NextFree()
	slot.Val = 1337
	q.Publish(slot)

	var published []int
	q.Drain(func(slot *queue.Slot) {
		published = append(published, slot.Val.(int))
	})
	if len(published) != 1 {
		t.Errorf("Expected 1 published item, found %d", len(published))
	}
}

func BenchmarkSpscQueue(b *testing.B) {
	keepRunning := int32(1)
	q, _ := queue.NewSingleProducerSingleConsumer(queue.Options{
		Size: 1024,
	})

	go func() {
		var receivedCount int
		for atomic.LoadInt32(&keepRunning) > 0 {
			q.Drain(func(slot *queue.Slot) {
				receivedCount += 1
			})
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		slot, _ := q.NextFree()
		slot.Val = i
		q.Publish(slot)
		i += 1
	}

	atomic.AddInt32(&keepRunning, -1)
}
