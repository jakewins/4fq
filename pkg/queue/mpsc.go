package queue

import (
	"fmt"
)

// Create a new queue that safely handles multiple producers publishing items,
// and one consumer receiving them. Note that the onus is on you to ensure there
// is just one consumer - the queue will do crazy things if multiple consumers
// run concurrently.
//
// Options are, as implied, optional. The queue defaults to 64 slots fixed size,
// and initializes the Val on each slot to nil.
func NewMultiProducerSingleConsumer(opts Options) (Queue, error) {
	if opts.Size == 0 {
		opts.Size = 64
	}
	if !isPowerOfTwo(opts.Size) {
		return nil, fmt.Errorf("Queue size must be a power of two, got %d", opts.Size)
	}
	if opts.Allocate == nil {
		opts.Allocate = func() interface{} { return nil }
	}

	slots := make([]*Slot, opts.Size)
	for i := range slots {
		slots[i] = &Slot{}
		slots[i].Val = opts.Allocate()
	}

	waitStrategy := &SleepWaitStrategy{}

	consumed := &singleWriterBarrier{
		waitStrategy: waitStrategy,
		barrierSequence: &sequence{
			value: -1,
		},
	}

	sequencer := newSequencer(opts.Size, waitStrategy, -1, consumed)

	published := newMultiWriterBarrier(opts.Size, waitStrategy, sequencer.cursor)

	q := &singleConsumerQueue{
		baseQueue: baseQueue{
			slots:     slots,
			sequencer: sequencer,
			published: published,
			mod:       int64(opts.Size) - 1,
		},
		consumed:  consumed,
	}

	return q, nil
}
