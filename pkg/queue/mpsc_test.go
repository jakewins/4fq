package queue_test

import (
	"github.com/jakewins/4fq/pkg/queue"
	"sync/atomic"
	"testing"
)

func TestMPSCBasic(t *testing.T) {
	q, _ := queue.NewMultiProducerSingleConsumer(queue.Options{})

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

func BenchmarkMpscQueue(b *testing.B) {
	producerCount := 4
	runningProducers := int64(producerCount)
	q, _ := queue.NewMultiProducerSingleConsumer(queue.Options{
		Size: 1024,
	})

	go func() {
		var receivedCount int
		for atomic.LoadInt64(&runningProducers) > 0 {
			q.Drain(func(slot *queue.Slot) {
				receivedCount += 1
			})
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		atomic.AddInt64(&runningProducers, 1)
		i := 0
		for pb.Next() {
			slot, _ := q.NextFree()
			slot.Val = i
			q.Publish(slot)
			i += 1
		}
		atomic.AddInt64(&runningProducers, -1)
	})
	atomic.AddInt64(&runningProducers, -1)
}

// For reference, same use case as above but using regular channels
func BenchmarkChannel(b *testing.B) {
	producerCount := 4
	runningProducers := int64(producerCount)
	ch := make(chan int, 1024)

	go func() {
		var receivedCount int
		for atomic.LoadInt64(&runningProducers) > 0 {
			slot := <-ch
			receivedCount += slot
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		atomic.AddInt64(&runningProducers, 1)
		i := 0
		for pb.Next() {
			ch <- i
			i += 1
		}
		atomic.AddInt64(&runningProducers, -1)
	})
	atomic.AddInt64(&runningProducers, -1)
}
