package queue

import "sync/atomic"

// See the sequencer doc for context.
//
// Given the core construct of infinitely increasing sequences, the idea is that
// a consumer can pick up items in the queue once a producer signals it's "moved past"
// that items sequence number. Likewise, a producer can pick up an item and re-use it
// once a consumer signals it's done with it.
//
// The barrier is a data structure that we use to safely and efficiently convey
// the message "I'm done up to sequence N".
type barrier interface {
	publish(lo, hi int64)

	// Try and wait for the given sequence to be available. How long this will wait depends on
	// the wait strategy used - in any case, the actual sequence reached is returned and may be less
	// than the requested sequence.
	waitFor(sequence int64) int64
}

// Note: If you're familiar with the LMAX design, you'll note that this
//       differs - the barriers track publishing data on their own, they don't
//       connect back to the sequencer for that like LMAX did.

type multiWriterBarrier struct {
	bufferSize   int64
	waitStrategy WaitStrategy

	dependentSequence *sequence

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

func (b *multiWriterBarrier) publish(lo, hi int64) {
	for l := lo; l <= hi; l++ {
		b.setAvailable(l)
	}
	b.waitStrategy.SignalAllWhenBlocking()
}

func (b *multiWriterBarrier) setAvailable(sequence int64) {
	b.setAvailableBufferValue(b.calculateIndex(sequence), b.calculateAvailabilityFlag(sequence))
}

func (b *multiWriterBarrier) waitFor(sequence int64) int64 {
	published := b.waitStrategy.WaitFor(sequence, b.dependentSequence)

	if published < sequence {
		return published
	}

	high := b.getHighestPublishedSequence(sequence, published)
	return high
}

func (b *multiWriterBarrier) getHighestPublishedSequence(lowerBound, availableSequence int64) int64 {
	for sequence := lowerBound; sequence <= availableSequence; sequence++ {
		if !b.isAvailable(sequence) {
			return sequence - 1
		}
	}
	return availableSequence
}

func (b *multiWriterBarrier) isAvailable(sequence int64) bool {
	return atomic.LoadInt32(&b.availableBuffer[b.calculateIndex(sequence)]) == b.calculateAvailabilityFlag(sequence)
}

// The availability "flag" is a "lap counter", sequence / ring size
func (s *multiWriterBarrier) calculateAvailabilityFlag(sequence int64) int32 {
	return int32(sequence >> s.indexShift)
}

func (s *multiWriterBarrier) calculateIndex(sequence int64) int {
	return int(sequence & s.indexMask)
}

func (s *multiWriterBarrier) setAvailableBufferValue(index int, flag int32) {
	atomic.StoreInt32(&s.availableBuffer[index], flag)
}

func newMultiWriterBarrier(bufferSize int, waitStrategy WaitStrategy, dependentOn *sequence) barrier {
	b := &multiWriterBarrier{
		bufferSize:        int64(bufferSize),
		waitStrategy:      waitStrategy,
		dependentSequence: dependentOn,
		availableBuffer:   make([]int32, bufferSize),
		indexMask:         int64(bufferSize - 1),
		indexShift:        log2(bufferSize),
	}
	for i := int(bufferSize - 1); i != 0; i-- {
		b.setAvailableBufferValue(i, -1)
	}
	b.setAvailableBufferValue(0, -1)

	return b
}


type singleWriterBarrier struct {
	waitStrategy WaitStrategy
	barrierSequence *sequence
}

func (b *singleWriterBarrier) publish(lo, hi int64) {
	b.barrierSequence.set(hi)
	b.waitStrategy.SignalAllWhenBlocking()
}

func (b *singleWriterBarrier) waitFor(sequence int64) int64 {
	s :=  b.waitStrategy.WaitFor(sequence, b.barrierSequence)
	return s
}
