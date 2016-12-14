package queue

import (
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

type Options struct {
	// Size of the queue, must be a power-of-two number (2,4,8,16,32..)
	Size int
	// Optionally pre-allocate slots Val's in the ring; this is useful if you want
	// to use buffer without allocating heap memory on the hot path - allocate records for yourself
	// once, and then populate those records with appropriate values
	Allocate func() interface{}
}

// A specialized alternative to Go Channels. Lets you send values from one
// Go Routine to another in a safe way, along with many other cool use cases.
type Queue interface {
	// For publishers - get the next free slot to write data to
	NextFree() (*Slot, error)
	// For publishers - publish a slot after filling it with data
	Publish(slot *Slot) error

	// Receive up to queue length items in bulk, blocking if there are no
	// items available
	Drain(handler func(*Slot)) error
}

type Slot struct {
	s   int64
	ptr unsafe.Pointer
}

func (s *Slot) Get() interface{} {
	return *(*interface{})(atomic.LoadPointer(&s.ptr))
}

func (s *Slot) Set(v interface{}) {
	atomic.StorePointer(&s.ptr, unsafe.Pointer(&v))
}

// How to approach the queue being empty
type WaitStrategy interface {
	// Called when producers stick new stuff in the queue. A wait strategy can use this to
	// snooze go routines in WaitFor, and wake them up when this gets called.
	SignalAllWhenBlocking()
	// Called when the queue is empty. The implementation should wait for sequence to be <= dependentSequence,
	// or until it gets tired of waiting according to whatever criteria.
	// When this returns, it should return the current value of dependentSequence - which is allowed to be < sequence.
	WaitFor(sequence int64, dependentSequence *sequence) int64
}

// Default wait strategy - spinlock for 100 cycles, then fall back to letting the go scheduler
// schedule other goroutines a hundred times, and if we're still not done waiting it will start sleeping in
// nanosecond intervals.
type SleepWaitStrategy struct {
}

func (w *SleepWaitStrategy) SignalAllWhenBlocking() {

}
func (w *SleepWaitStrategy) WaitFor(sequence int64, dependentSequence *sequence) int64 {
	var availableSequence int64
	counter := 200

	for availableSequence = dependentSequence.get(); availableSequence < sequence; availableSequence = dependentSequence.get() {
		if counter > 100 {
			counter--
		} else if counter > 0 {
			counter--
			runtime.Gosched()
		} else {
			time.Sleep(time.Nanosecond)
		}
	}
	return availableSequence
}

// A thin wrapper around a int64, giving some convenience methods for ordered writes and CAS
//
// On Overflow: If the queue is processing 10 million messages per second,
// it will transfer 13140000000000 messages a year. int64 fits 9223372036854775807 before overflow,
// meaning at 10M/s, the queue can run for ~100K years without overflowing. Hence, the code does
// knowingly not account for this value wrapping around. Off-the-cuff, throughput may be able
// to reach the low billions before hitting actual physical limits (something something speed of light,
// something something nano metres), but even then the queue can run for thousands of years before wrapping.
type sequence struct {
	_lpad [56]byte
	value int64
	_rpad [56]byte
}

func (s *sequence) get() int64 {
	return atomic.LoadInt64(&s.value)
}

func (s *sequence) set(v int64) {
	atomic.StoreInt64(&s.value, v)
}
func (s *sequence) compareAndSet(old, new int64) bool {
	return atomic.CompareAndSwapInt64(&s.value, old, new)
}
func (s *sequence) add(delta int64) {
	atomic.AddInt64(&s.value, delta)
}

// The sequencer is the center piece of these queues - it is based entirely on the brilliant work at LMAX.
// Each queue has one sequencer, and it controls the entry of new items into the queue.
//
// The core abstraction is an infinite sequence of numbers, with various parts of the queue tracking
// which part of the number sequence they've reached. The sequencer sits in front of them all, controlling the
// entry into "new territory". Once the sequencer increments further into the number sequence, other pointers
// in the queue may increment up to the point the sequencer is at.
//
// The sequencer does this, while maintaining a key invariant: The infinite sequence of numbers is mapped onto
// a circular buffer, which is distinctly not infinite. Hence, the sequencer keeps track of all the secondary
// sequence pointers, and ensures the delta from the lowest pointer to the sequencer pointer never is greater
// than the size of the buffer.
//
// At the end of the day, the sequencer is really just:
//
//   func next(n) {
//       if cursor + n < min(otherPointersInTheQueue) {
//           cursor += n
//       }
//       return cursor
//   }
//
// The neat thing is that it does the above using some really clever techniques that make the sequencer
// safe for concurrent use, and extremely low overhead to boot.
type sequencer struct {
	bufferSize   int64
	waitStrategy WaitStrategy

	// This is the golden cursor - it points to the highest number the sequencer (and hence any other pointer)
	// has reached in our supposedly infinite sequence of numbers
	cursor *sequence

	finalBarrier        barrier
	gatingSequenceCache *sequence
}

// Get control of n items, returning the end item sequence
func (s *sequencer) next(n int64) int64 {
	for {
		current := s.cursor.get()
		next := current + n

		wrapPoint := next - s.bufferSize
		cachedGatingSequence := s.gatingSequenceCache.get()

		if wrapPoint > cachedGatingSequence || cachedGatingSequence > current {
			gatingSequence := min(s.finalBarrier.waitFor(current), current)
			if wrapPoint > gatingSequence {
				s.waitStrategy.SignalAllWhenBlocking()
				time.Sleep(time.Nanosecond)
				continue
			}
			s.gatingSequenceCache.set(gatingSequence)
		} else if s.cursor.compareAndSet(current, next) {
			return next
		}
	}
}

func newSequencer(bufferSize int, ws WaitStrategy, initial int64, finalBarrier barrier) *sequencer {
	s := &sequencer{
		bufferSize:   int64(bufferSize),
		waitStrategy: ws,
		cursor: &sequence{
			value: initial,
		},
		finalBarrier:        finalBarrier,
		gatingSequenceCache: &sequence{value: -1},
	}

	return s
}

// Allocation-free ring buffer
type baseQueue struct {
	// Core source of coordination, points to the highest sequence reached and
	// ensures producers don't overrun consumers (and thus overwriting unread values)
	sequencer *sequencer

	// Barrier for published items - consumers wait on this
	published barrier

	// This is the ring buffer
	slots []*Slot

	// For quick remainder calculation, this is len(slots) - 1
	mod int64
}

func (q *baseQueue) NextFree() (*Slot, error) {
	acquired := q.sequencer.next(1)
	slot := q.slots[acquired&q.mod]
	slot.s = acquired
	return slot, nil
}

func (q *baseQueue) Publish(slot *Slot) error {
	q.published.publish(slot.s, slot.s)
	return nil
}

type singleConsumerQueue struct {
	baseQueue

	// Barrier for consumed items - producers wait on this
	consumed *singleWriterBarrier
}

func (q *singleConsumerQueue) Drain(handler func(*Slot)) error {
	next := q.consumed.barrierSequence.value + 1
	published := q.published.waitFor(next)

	if published < next {
		return nil
	}

	for ; next <= published; next++ {
		handler(q.slots[next&q.mod])
	}
	q.consumed.publish(published, published)
	return nil
}


type multiConsumerQueue struct {
	baseQueue

	// Used to coordinate between consumers on who handles what sequence items
	consumerAcquired *sequence

	// Barrier for consumed items - producers wait on this
	consumed *multiWriterBarrier
}

func (q *multiConsumerQueue) Drain(handler func(*Slot)) error {
	// This loop to handle us racing with other consumers - keep trying
	// until we get a range via the CAS further down
	for {
		next := q.consumerAcquired.get() + 1
		published := q.published.waitFor(next)

		if published < next {
			return nil
		}

		// Got a candidate for what to handle, coordinate
		// with other consumers to check if we can take it on
		if (q.consumerAcquired.compareAndSet(next, published)) {
			// We claimed the range, drain it.

			for ; next <= published; next++ {
				handler(q.slots[next & q.mod])
			}
			q.consumed.publish(published, published)
			return nil
		}
	}
}

func isPowerOfTwo(x int) bool {
	return (x != 0) && (x&(x-1)) == 0
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func log2(i int) uint {
	r := uint(0)
	for i >>= 1; i != 0; i >>= 1 {
		r++
	}
	return r
}
