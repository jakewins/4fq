package queue_test

import (
	"sync/atomic"
	"time"
	"github.com/jakewins/4fq/pkg/queue"
	"testing"
)

// A scenario that we want to test
type testScenario struct {
	name string
	// Constructor for the queue to test
	newQueue func(queue.Options) (queue.Queue, error)
	// Configuration for it
	options queue.Options
	// Number of producing goroutines
	numProducers int
	// Number of consuming goroutines
	numConsumers int
	// Number of messages sent by each producer
	messagesPerProducer int
}

type testItem struct {
	producer int
	val      int
}

var cases = []testScenario{
	{ "MPMC WrapAround", queue.NewMultiProducerMultiConsumer, queue.Options{ Size: 8 }, 2, 2, 36 },
	{ "MPSC WrapAround", queue.NewMultiProducerSingleConsumer, queue.Options{ Size: 8 }, 2, 1, 36 },
	{ "SPMC WrapAround", queue.NewSingleProducerMultiConsumer, queue.Options{ Size: 8 }, 1, 2, 36 },
	{ "SPSC WrapAround", queue.NewSingleProducerSingleConsumer, queue.Options{ Size: 8 }, 1, 1, 36 },
}

// Verifies each scenario above by transferring messages that contain
// an atomically incrementing value per producer, plus a producer id.
// This allows us to verify that all expected messages were received and
// were received in order
func TestCorrectness(t *testing.T) {
	for _, scenario := range cases {

		for a := 0; a < 100; a++ {
			q, _ := scenario.newQueue(scenario.options)

			for producerId := 0; producerId < scenario.numProducers; producerId++ {
				go func(pid, mumMessages int) {
					for i := 0; i < mumMessages; i++ {
						slot, _ := q.NextFree()
						slot.Val = &testItem{
							producer: pid,
							val:      i,
						}
						q.Publish(slot)
					}
				}(producerId, scenario.messagesPerProducer)
			}

			totalReceived := int64(0)
			// Array of consumerId, producerId, messages
			globalReceived := make([][][]int, scenario.numConsumers)
			for consumerId := 0; consumerId < scenario.numConsumers; consumerId++ {
				consumerReceived := make([][]int, scenario.numProducers)
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
			for atomic.LoadInt64(&totalReceived) < int64(scenario.numProducers * scenario.messagesPerProducer) {
				time.Sleep(time.Microsecond)
			}

			// Assertion section

			// For each consumer, an array with a slot for each message by index - we go through the
			// received messages for each consumer and increment the appropriate message slot for each message id,
			// and verify we've seen each exactly once.
			countPerMessage := make([][]int, scenario.numProducers)
			for producerId := 0; producerId < scenario.numProducers; producerId++ {
				countPerMessage[producerId] = make([]int, scenario.messagesPerProducer)
			}

			// Sort all the messages into the countPerMessage structure, verifying as we go that all messages are
			// received in order from the perspective of each consumer (that's that the 'high' stuff is about)
			for consumerId, consumerReceived := range globalReceived {
				for producerId, messages := range consumerReceived {
					high := -1
					for _, message := range messages {
						countPerMessage[producerId][message] += 1
						if high >= message {
							t.Errorf("[%s] Consumer %d received message %d from P%d before message %d",
								scenario.name, consumerId, high, producerId, message)
						}
						high = message
					}
				}
			}

			// And now verify every message arrived exactly once
			for producerId, messages := range countPerMessage {
				for message, count := range messages {
					if count != 1 {
						t.Errorf("[%s] Message %d from producer %d was received %d times, expected exactly-once delivery",
							scenario.name, message, producerId, count)
					}
				}
			}
		}
	}
}
