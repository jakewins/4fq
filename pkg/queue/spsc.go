package queue

import (
	"fmt"
)

// Create a new queue that safely handles one producer publishing items,
// and one consumer receiving them. Note that the onus is on you to ensure there
// is just one consumer and one producer - the queue will do crazy things if this
// rule is broken.
//
// Options are, as implied, optional. The queue defaults to 64 slots fixed size,
// and initializes the Val on each slot to nil.
func NewSingleProducerSingleConsumer(opts Options) (Queue, error) {
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
		slots[i].Set(opts.Allocate())
	}

	waitStrategy := &SleepWaitStrategy{}

	consumed := &singleWriterBarrier{
		waitStrategy: waitStrategy,
		barrierSequence: &sequence{
			value: -1,
		},
	}

	sequencer := newSequencer(opts.Size, waitStrategy, -1, consumed)

	published := &singleWriterBarrier{
		waitStrategy: waitStrategy,
		barrierSequence: &sequence{
			value: -1,
		},
	}

	q := &singleConsumerQueue{
		slots:     slots,
		sequencer: sequencer,
		published: published,
		consumed:  consumed,
		mod:       int64(opts.Size) - 1,
	}

	return q, nil
}
