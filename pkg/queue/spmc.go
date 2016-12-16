package queue

import (
	"fmt"
)

// Create a new queue that safely handles one producer publishing items,
// and multiple consumer receiving them.
//
// Options are, as implied, optional. The queue defaults to 64 slots fixed size,
// and initializes the Val on each slot to nil.
func NewSingleProducerMultiConsumer(opts Options) (Queue, error) {
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

	consumed := newMultiWriterBarrier(opts.Size, waitStrategy, &sequence{value:-1})

	sequencer := newSequencer(opts.Size, waitStrategy, -1, consumed)

	published := &singleWriterBarrier{
		waitStrategy: waitStrategy,
		barrierSequence: &sequence{
			value: -1,
		},
	}

	q := &multiConsumerQueue{
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
