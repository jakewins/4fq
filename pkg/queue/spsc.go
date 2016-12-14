package queue

import (
	"fmt"
)

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
