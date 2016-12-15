package queue_test

import (
	"github.com/jakewins/4fq/pkg/queue"
	_ "net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"testing"
	"time"
)

func TestMPMCBasic(t *testing.T) {
	q, _ := queue.NewMultiProducerMultiConsumer(queue.Options{})

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

func TestMPMCBufferWrapAround(t *testing.T) {
	producerCount := 2
	consumerCount := 2
	length := 36
	for a := 0; a < 100; a++ {
		q, _ := queue.NewMultiProducerMultiConsumer(queue.Options{
			Size: 8,
		})

		for producerId := 0; producerId < producerCount; producerId++ {
			go func(pid int) {
				for i := 0; i < length; i++ {
					slot, _ := q.NextFree()
					slot.Val = &testItem{
						producer: pid,
						val:      i,
					}
					q.Publish(slot)
				}
			}(producerId)
		}

		totalReceived := int64(0)
		// Array of consumerId, producerId, messages
		globalReceived := make([][][]int, consumerCount)
		for consumerId := 0; consumerId < consumerCount; consumerId++ {
			consumerReceived := make([][]int, producerCount)
			globalReceived[consumerId] = consumerReceived
			go func(cid int, received [][]int) {
				for {
					q.Drain(func(slot *queue.Slot) {
						item := slot.Val.(*testItem)
						received[item.producer] = append(received[item.producer], item.val)
						atomic.AddInt64(&totalReceived, 1)
					})
				}
			}(consumerId, consumerReceived)
		}

		// Wait until we've seen all messages
		for atomic.LoadInt64(&totalReceived) < int64(producerCount * length) {
			time.Sleep(time.Microsecond)
		}

		// Assertion section

		// For each consumer, an array with a slot for each message by index - we go through the
		// received messages for each consumer and increment the appropriate message slot for each message id,
		// and verify we've seen each exactly once.
		countPerMessage := make([][]int, producerCount)
		for producerId := 0; producerId < producerCount; producerId++ {
			countPerMessage[producerId] = make([]int, length)
		}

		// Sort all the messages into the countPerMessage structure, verifying as we go that all messages are
		// received in order from the perspective of each consumer (that's that the 'high' stuff is about)
		for consumerId, consumerReceived := range globalReceived {
			for producerId, messages := range consumerReceived {
				high := -1
				for _, message := range messages {
					countPerMessage[producerId][message] += 1
					if high >= message {
						t.Errorf("Consumer %d received message %d from P%d before message %d",
							consumerId, high, producerId, message)
					}
					high = message
				}
			}
		}

		// And now verify every message arrived exactly once
		for producerId, messages := range countPerMessage {
			for message, count := range messages {
				if count != 1 {
					t.Errorf("Message %d from producer %d was received %d times, expected exactly-once delivery",
						message, producerId, count)
				}
			}
		}

	}
}

func BenchmarkMPMCQueue(b *testing.B) {
	producerCount := 4
	runningProducers := int64(producerCount)
	q, _ := queue.NewMultiProducerMultiConsumer(queue.Options{
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