package queue_test

import (
	"fmt"
	"github.com/jakewins/4fq/pkg/queue"
)

// This variant is safe for multiple producers and multiple consumers,
// making it the most general of the four queues - if you're unsure which
// one to use, this is the safe choice.
// To be safe for multiple producers and consumers, this version of the
// queue has to take some extra precautions. If you know for a fact that
// there will be, for instance, just one consumer or just one producer,
// you may consider looking at one of the specialized options.
func ExampleMultiProducerMultiConsumer() {
	q, err := queue.NewSingleProducerSingleConsumer(queue.Options{})
	if err != nil {
		// May happen if options are invalid, for instance
		panic(err)
	}

	// Put something on the queue
	// 1: Get a queue slot to stick our value in
	slot, err := q.NextFree()
	slot.Val = "Hello, world!"

	// 2: Publish the slot
	q.Publish(slot)

	// Read from the queue
	// Drain reads in bulk, blocking until at least one message is available,
	q.Drain(func(slot *queue.Slot) {
		fmt.Printf("Received: %s\n", slot.Val)
	})

	// Output:
	// Received: Hello, world!
}

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
	slot.Val = "Hello, world!"

	// 2: Publish the slot
	q.Publish(slot)

	// Read from the queue
	// Drain reads in bulk, blocking until at least one message is available,
	q.Drain(func(slot *queue.Slot) {
		fmt.Printf("Received: %s\n", slot.Val)
	})

	// Output:
	// Received: Hello, world!
}

// If you promise to just have one go routine publish to the queue, and only
// one reading from it, you can use this variant which improves latency by removing concurrency
// checks that are only needed if there are multiple publishers and subscribers
func ExampleSingleProducerSingleConsumer() {
	// Create an SPSC Queue - note that there is no safety net: If you
	// have multiple go routines publishing to or reading from this simultaneously bad things
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
	slot.Val = "Hello, world!"

	// 2: Publish the slot
	q.Publish(slot)

	// Read from the queue
	// Drain reads in bulk, blocking until at least one message is available,
	q.Drain(func(slot *queue.Slot) {
		fmt.Printf("Received: %s\n", slot.Val)
	})

	// Output:
	// Received: Hello, world!
}


// If you promise to just have one go routine publish to the queue,
// you can use this variant which improves latency by removing concurrency
// checks that are only needed if there are multiple publishers
func ExampleSingleProducerMultiConsumer() {
	// Create an SPMC Queue - note that there is no safety net: If you
	// have multiple go routines publishing to this simultaneously bad things
	// will happen. Use the MultiProducer variant if you're uncertain,
	// and/or use the go test -race flag to test your programs safety.
	q, err := queue.NewSingleProducerMultiConsumer(queue.Options{})
	if err != nil {
		// May happen if options are invalid, for instance
		panic(err)
	}

	// Put something on the queue
	// 1: Get a queue slot to stick our value in
	slot, err := q.NextFree()
	slot.Val = "Hello, world!"

	// 2: Publish the slot
	q.Publish(slot)

	// Read from the queue
	// Drain reads in bulk, blocking until at least one message is available,
	q.Drain(func(slot *queue.Slot) {
		fmt.Printf("Received: %s\n", slot.Val)
	})

	// Output:
	// Received: Hello, world!
}
