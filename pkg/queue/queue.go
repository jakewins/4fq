package queue

import (
	"runtime"
	"sync/atomic"
	"time"
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
	Val interface{}
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

	//fmt.Printf("WaitFor(%d >= %d)\n", dependentSequence.value, sequence)
	for availableSequence = dependentSequence.value; availableSequence < sequence; availableSequence = dependentSequence.value {
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

	// This is the other pointer (soon to be multiple, but one for now) in the queue - the sequencer makes sure
	// it doesn't get further ahead of this than bufferSize, otherwise we'd wrap around the buffer and overwrite
	// slots before they had been processed.
	gatingSequence      *sequence
	gatingSequenceCache *sequence

	// Tracks published slots. This could be implemented with a simple counter;
	// however, when we have multiple producers, they would need to block and wait on one
	// another to mark their items published (since we publish in the sequence order)
	// This data structure, instead, has a slot for each item in the ring, each slot gets
	// the highest "lap number" published for that slot. The wrapPoint code in next() ensures
	// we don't overrun.
	//
	// This means that if we have a slow publisher, other publishers can mark their items
	// available ahead of time by writing their sequences lap number into the appropriate slot,
	// meaning they don't have to wait for the slow publisher to publish.
	availableBuffer []int32

	indexMask  int64
	indexShift uint
}

// Get control of n items, returning the end item sequence
func (s *sequencer) next(n int64) int64 {
	for {
		current := s.cursor.get()
		next := current + n

		wrapPoint := next - s.bufferSize
		cachedGatingSequence := s.gatingSequenceCache.get()

		if wrapPoint > cachedGatingSequence || cachedGatingSequence > current {
			gatingSequence := min(s.gatingSequence.value, current)
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

func (s *sequencer) publish(lo, hi int64) {
	for l := lo; l <= hi; l++ {
		s.setAvailable(l)
	}
	s.waitStrategy.SignalAllWhenBlocking()
}

func (s *sequencer) setAvailable(sequence int64) {
	s.setAvailableBufferValue(s.calculateIndex(sequence), s.calculateAvailabilityFlag(sequence))
}

// Try and wait for the given sequence to be available. How long this will wait depends on
// the wait strategy used - in any case, the actual sequence reached is returned and may be less
// than the requested sequence.
func (s *sequencer) waitFor(sequence int64) int64 {
	published := s.waitStrategy.WaitFor(sequence, s.cursor)

	if published < sequence {
		return published
	}

	high := s.getHighestPublishedSequence(sequence, published)
	return high
}

func (s *sequencer) getHighestPublishedSequence(lowerBound, availableSequence int64) int64 {
	for sequence := lowerBound; sequence <= availableSequence; sequence++ {
		if !s.isAvailable(sequence) {
			return sequence - 1
		}
	}
	return availableSequence
}

func (s *sequencer) isAvailable(sequence int64) bool {
	return atomic.LoadInt32(&s.availableBuffer[s.calculateIndex(sequence)]) == s.calculateAvailabilityFlag(sequence)
}

// The availability "flag" is a "lap counter", sequence / ring size
func (s *sequencer) calculateAvailabilityFlag(sequence int64) int32 {
	return int32(sequence >> s.indexShift)
}

func (s *sequencer) calculateIndex(sequence int64) int {
	return int(sequence & s.indexMask)
}

func (s *sequencer) setAvailableBufferValue(index int, flag int32) {
	atomic.StoreInt32(&s.availableBuffer[index], flag)
}

func newSequencer(bufferSize int, ws WaitStrategy, initial int64, gatingSequence *sequence) *sequencer {
	s := &sequencer{
		bufferSize:   int64(bufferSize),
		waitStrategy: ws,
		cursor: &sequence{
			value: initial,
		},
		gatingSequence:      gatingSequence,
		gatingSequenceCache: &sequence{value: -1},
		availableBuffer:     make([]int32, bufferSize),
		indexMask:           int64(bufferSize - 1),
		indexShift:          log2(bufferSize),
	}

	for i := bufferSize - 1; i != 0; i-- {
		s.setAvailableBufferValue(i, -1)
	}
	s.setAvailableBufferValue(0, -1)

	return s
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
