package queue_test

import (
	"log"
	"net/http"
	_ "net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"testing"
	"time"
	"github.com/jakewins/4fq/pkg/queue"
)

func TestMPSCBasic(t *testing.T) {
	q, _ := queue.New(queue.Options{})

	slot, _ := q.Next()
	slot.Val = 1337
	q.Publish(slot)

	var published []int
	q.Drain(func(slots []*queue.Slot) {
		{
			for _, s := range slots {
				published = append(published, s.Val.(int))
			}
		}
	})
	if len(published) != 1 {
		t.Errorf("Expected 1 published item, found %d", len(published))
	}
}

type testItem struct {
	producer int
	val      int
}

func TestMPSCBufferWrapAround(t *testing.T) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// This tests both transferring more messages than fit in the queue,
	// as well as wrapping the sequence counters by starting them super high
	producerCount := 4
	length := 36
	for a := 0; a < 100; a++ {
		q, _ := queue.New(queue.Options{
			Size: 8,
		})

		for producerId := 0; producerId < producerCount; producerId++ {
			go func(pid int) {
				for i := 0; i < length; i++ {
					slot, _ := q.Next()
					slot.Val = &testItem{
						producer: pid,
						val:      i,
					}
					q.Publish(slot)
				}
			}(producerId)
		}

		var received = make([][]int, producerCount)
		var receivedCount int
		for receivedCount < length*producerCount {
			q.Drain(func(slots []*queue.Slot) {
				for _, s := range slots {
					item := s.Val.(*testItem)
					received[item.producer] = append(received[item.producer], item.val)
					receivedCount += 1
				}
			})
		}

		for _, messages := range received {
			for i, v := range messages {
				if v != i {
					t.Errorf("Expected incremental sequence of messages, but message %d is %d", i, v)
				}
			}
		}
	}
}

func BenchmarkMpscQueue(b *testing.B) {
	producerCount := 1
	runningProducers := int64(producerCount)
	q, _ := queue.New(queue.Options{
		Size: 1024,
	})

	go func() {
		var receivedCount int
		for atomic.LoadInt64(&runningProducers) > 0 {
			q.Drain(func(slots []*queue.Slot) {
				receivedCount += len(slots)
			})
			time.Sleep(time.Microsecond)
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		atomic.AddInt64(&runningProducers, 1)
		i := 0
		for pb.Next() {
			slot, _ := q.Next()
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
	producerCount := 1
	runningProducers := int64(producerCount)
	ch := make(chan int, 1024)

	go func() {
		var receivedCount int
		for atomic.LoadInt64(&runningProducers) > 0 {
			slot := <-ch
			receivedCount += slot
			time.Sleep(time.Microsecond)
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
