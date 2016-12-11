package queue

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Options struct {
	// Size of the ring buffer, must be a power-of-two number (2,4,8,16,32..)
	Size int
	// Optionally pre-allocate slots Val's in the ring; this is useful if you want
	// to use buffer without allocating heap memory on the hot path - allocate records for yourself
	// once, and then populate those records with appropriate values
	Allocate       func() interface{}
	XXX_InitialSeq int64
}

// A specialized alternative to Go Channels. Lets you send values from one
// Go Routine to another in a safe way, along with many other cool use cases.
type Queue interface {
	// For publishers - get the next free slot to write data to
	Next() (*Slot, error)
	// For publishers - publish a slot after filling it with data
	Publish(slot *Slot) error

	// Receive up to queue length items in bulk, blocking if there are no
	// items available
	Drain(handler func([]*Slot)) error
}

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

type Slot struct {
	s   int64
	Val interface{}
}

type WaitStrategy interface {
	SignalAllWhenBlocking()
	WaitFor(sequence int64, dependentSequence *sequence) int64
}

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
//
// On Cache lines: The LMAX sequence implementation pads this number to force it to sit on its own
// cache line. However, trying to pad this by adding [56]byte fields on either side had no effect on performance.
// I'm not sure if this is because the layout doesn't suffer from false sharing, or if the go compiler removes or
// reorders the unused fields.. In any case, I'd want evidence it makes a difference before adding it.
type sequence struct {
	value int64
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

// Lets clients "acquire" the next sequence, and then publish that sequence
// when they are done with it; so this is about tracking control of sequences.
// You could imagine this being implemented using two counters, "acquired" and "published",
// chasing each other.
// The implenentation reality is more complex - the "published" counter is represented
// as a richer data type than a simple counter for efficiency reasons - but the behavior is the same.
type sequencer struct {
	bufferSize   int64
	waitStrategy WaitStrategy
	cursor       *sequence
	// This is the sequence "ahead" of us that we're not allowed to surpass
	gatingSequence      *sequence
	gatingSequenceCache *sequence

	// Tracks published sequence items. This could be implemented with a simple counter;
	// however, when we have multiple producers, they would need to block and wait on one
	// another to mark their items published (since we publish in the sequence order)
	// This data structure, instead, has a slot for each item in the ring, each slot gets
	// written to it the "lap number" that has been published to it.
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
		current := s.cursor.value
		next := current + n

		wrapPoint := next - s.bufferSize
		cachedGatingSequence := s.gatingSequenceCache.value

		//fmt.Printf("Cursor: %d\n", s.cursor.value)
		//fmt.Printf("next: %d > %d || %d > %d\n", wrapPoint, cachedGatingSequence, cachedGatingSequence, current)
		if wrapPoint > cachedGatingSequence || cachedGatingSequence > current {
			//fmt.Printf("  true.. gate: %d  current: %d wrap: %d\n", s.gatingSequence.value, current, wrapPoint)
			gatingSequence := min(s.gatingSequence.value, current)
			if wrapPoint > gatingSequence {
				s.waitStrategy.SignalAllWhenBlocking()
				time.Sleep(time.Nanosecond)
				continue
			}
			//fmt.Printf("  cache gate %d, min(%d %d)\n", gatingSequence, s.gatingSequence.value, current)
			s.gatingSequenceCache.set(gatingSequence)
		} else if s.cursor.compareAndSet(current, next) {
			//fmt.Printf("  next -> %d\n", next)
			return next
		}
	}
}

func (s *sequencer) publish(lo, hi int64) {
	for l := lo; l <= hi; l++ {
		//fmt.Printf("Publish(%d @%d)\n", l, s.cursor.value)
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
		//fmt.Printf("  Wait: published < sequence (%d < %d)\n", published, sequence)
		return published
	}

	high := s.getHighestPublishedSequence(sequence, published)
	//fmt.Printf("  Wait high (%d, %d -> %d)\n", published, sequence, high)
	return high
}

func (s *sequencer) getHighestPublishedSequence(lowerBound, availableSequence int64) int64 {
	//fmt.Printf("  Available: %v\n", s.availableBuffer)
	for sequence := lowerBound; sequence <= availableSequence; sequence++ {
		if !s.isAvailable(sequence) {
			return sequence - 1
		}
	}
	return availableSequence
}

func (s *sequencer) isAvailable(sequence int64) bool {
	//v := atomic.LoadInt32(&s.availableBuffer[s.calculateIndex(sequence)])
	//fmt.Printf("@%d[%d] -> %d == %d (%v)\n", sequence, s.calculateIndex(sequence), v,  s.calculateAvailabilityFlag(sequence), s.availableBuffer)
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

func (q *mpscQueue) Next() (*Slot, error) {
	acquired := q.published.next(1)
	slot := q.slots[acquired&q.mod]
	slot.s = acquired
	return slot, nil
}

func (q *mpscQueue) Publish(slot *Slot) error {
	q.published.publish(slot.s, slot.s)
	return nil
}

func (q *mpscQueue) Drain(handler func([]*Slot)) error {
	bufferSize := int64(len(q.slots))
	next := q.consumed.value + 1
	published := q.published.waitFor(next)

	if published < next {
		return nil
	}

	from, to := next&q.mod, (published)&q.mod

	// If from > to, we've wrapped around the buffer, so we split into two calls
	if from > to {
		handler(q.slots[from:])
		q.consumed.add(bufferSize - from)
	} else if from <= to {
		handler(q.slots[from : to+1])
		q.consumed.add(to - from + 1)
	}
	return nil
}

// For debugging
var lm = &sync.Mutex{}

func (q *mpscQueue) describe(pre string) {
	lm.Lock()
	defer lm.Unlock()
	fmt.Println(pre)
	fmt.Printf("  ")
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
