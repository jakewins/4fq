package queue_test

import (
	"fmt"
	"github.com/jakewins/4fq/pkg/queue"
)

func ExampleMultiProducerSingleConsumer() {
	// Create an MPSC Queue
	q, err := queue.NewMultiProducerSingleConsumer(queue.Options{})
	if err != nil {
		// May happen if options are invalid, for instance
		panic(err)
	}

	// Put something on the queue
	// 1: Get a queue slot to stick our value in
	slot, err := q.NextFree()
	slot.Set("Hello, world!")

	// 2: Publish the slot
	q.Publish(slot)

	// Read from the queue
	// Drain reads in bulk, blocking until at least one message is available,
	q.Drain(func(slot *queue.Slot) {
		fmt.Printf("Received: %s\n", slot.Get())
	})

	// Output:
	// Received: Hello, world!
}

// If you promise to just have one go routine publish to the queue,
// you can use this variant which improves latency by removing concurrency
// checks that are only needed if there are multiple publishers
func ExampleSingleProducerSingleConsumer() {
	// Create an SPSC Queue - note that there is no safety net: If you
	// have multiple go routines publishing to this simultaneously bad things
	// will happen. Use the MultiProducer variant if you're uncertain,
	// and/or use the go test -race flag to test your programs safety.
	q, err := queue.NewSingleProducerSingleConsumer(queue.Options{})
	if err != nil {
		// May happen if options are invalid, for instance
		panic(err)
	}

	// Put something on the queue
	// 1: Get a queue slot to stick our value in
	slot, err := q.NextFree()
	slot.Set("Hello, world!")

	// 2: Publish the slot
	q.Publish(slot)

	// Read from the queue
	// Drain reads in bulk, blocking until at least one message is available,
	q.Drain(func(slot *queue.Slot) {
		fmt.Printf("Received: %s\n", slot.Get())
	})

	// Output:
	// Received: Hello, world!
}
