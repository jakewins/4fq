package queue

import (
	"fmt"
	"sync"
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
		slots[i] = &Slot{
			Val: opts.Allocate(),
		}
	}

	consumed := &sequence{
		value: -1,
	}

	publishedSeq := newSequencer(opts.Size, &SleepWaitStrategy{}, -1, consumed)

	q := &mpscQueue{
		slots:     slots,
		published: publishedSeq,
		consumed:  consumed,
		mod:       int64(opts.Size) - 1,
	}

	return q, nil
}

// Multi-producer single-consumer allocation-free ring buffer
type mpscQueue struct {

	// Highest published slot, transfers slot ownership from producers to consumers
	published *sequencer

	// Highest consumed slot
	consumed *sequence

	slots []*Slot

	// For quick remainder calculation, this is len(slots) - 1
	mod int64
}

func (q *mpscQueue) NextFree() (*Slot, error) {
	acquired := q.published.next(1)
	slot := q.slots[acquired&q.mod]
	slot.s = acquired
	return slot, nil
}

func (q *mpscQueue) Publish(slot *Slot) error {
	q.published.publish(slot.s, slot.s)
	return nil
}

func (q *mpscQueue) Drain(handler func(*Slot)) error {
	next := q.consumed.value + 1
	published := q.published.waitFor(next)

	if published < next {
		return nil
	}

	numConsumed := published - next + 1

	for ; next <= published; next++ {
		handler(q.slots[next&q.mod])
	}
	q.consumed.add(numConsumed)
	return nil
}

// For debugging
var lm = &sync.Mutex{}

func (q *mpscQueue) describe(pre string) {
	lm.Lock()
	defer lm.Unlock()
	fmt.Printf("%d\n  ", pre)
	for _, s := range q.slots {
		v := s.Val
		if v == nil {
			v = "-"
		}
		fmt.Printf("[%v]", v)
	}
	fmt.Println()
	fmt.Printf("  {%d -> %d}\n", q.published.cursor.value, q.consumed.value)
}