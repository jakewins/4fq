package queue_test

import (
	"github.com/jakewins/4fq/pkg/queue"
	"log"
	"net/http"
	_ "net/http"
	_ "net/http/pprof"
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

func TestSPSCBufferWrapAround(t *testing.T) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	length := 360
	for a := 0; a < 100; a++ {
		q, _ := queue.NewSingleProducerSingleConsumer(queue.Options{
			Size: 8,
		})

		go func(pid int) {
			for i := 0; i < length; i++ {
				slot, _ := q.NextFree()
				slot.Val = &testItem{
					producer: pid,
					val:      i,
				}
				q.Publish(slot)
			}
		}(1)

		var received = make([]int, 0, length)
		var receivedCount int
		for receivedCount < length {
			q.Drain(func(slot *queue.Slot) {
				item := slot.Val.(*testItem)
				received = append(received, item.val)
				receivedCount += 1
			})
		}

		for i, v := range received {
			if v != i {
				t.Errorf("Expected incremental sequence of messages, but message %d is %d", i, v)
			}
		}
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
