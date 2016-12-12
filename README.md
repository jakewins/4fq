# 4FQ (Four Fast Queues)

An ambitious name for a project with just one queue in it!

This repository contains a fast Multi-Producer/Single-Consumer message queue for Go. 
As the name implies, the intention is to add the three missing variants (SPSC, SPMC, MPMC).

For some use cases, the approach these queues take to goroutine coordination can substantially improve your systems performance.
Specifically, you may benefit from using this queue if:

- Your application deals with high throughput processing of discrete messages
- Your application is excessively using CPU cycles for garbage collection
- Your message processor benefits from batching - for instance an IO operation where writing a message is cheap but flushing is expensive

## Examples

- [Multi-producer/Single-consumer queue](pkg/queue/example_test.go#L8)

## Key features

- Batch reading, "wait until there's at least one message, then give me all immediately available messages"
- Allows sharing a pre-allocated set of records between subscribers and producers,
  meaning allocation-free message passing
- As fast as or faster than channels

## Performance

This was built for a specific use case - multiple go routines producing messages that 
are then processed in batch by a single go routine. That use case has a chan-based and
a queue-based benchmark you can find [here](pkg/queue/mpsc_test.go#L86).

For those benchmarks, latency is:

    Channels:
    95 ns/op
    
    4FQ:
    83 ns/op

Obviously this is a meaningless number. Your use case is likely different, it's a micro benchmark 
of a component that is usually not a bottleneck in the first place, and to my knowledge the go 
benchmark suite does not account for coordinated omission, so real-world performance will see 
higher latencies. 

It's also notably high - the sequencer is expected to see latencies closer to 0.5-4ns, so something
is off in my benchmark or implementation. However, the implementation currently provides a batch-drain
capable low-latency queue, which is what my immediate need was. I'll come back to this and improve
as time permits.

## Technical details

This uses the Sequencer design from LMAX to control access to slots in a circular buffer.
While this is not a re-implementation of the Disruptor message processing system, it uses the exact 
same core coordination primitive ("Sequences").

For coordinating control of memory the queues use CPU concurrency primitives (ordered writes and CAS),
meaning they bypass the overhead of OS arbitration. 

This works very well for continuous high throughput load.
However, for load with sporadic lulls, busy-spin strategies excessively use CPU resources. 
Hence, when waiting on an empty or full queue, wait strategies can be plugged in that do use
regular scheduler primitives for signaling when to wake go routines back up.

## Contributions

Contributions are super welcome - but *please* do reach out ahead of time if you're making major changes,
rejecting work because it doesn't quite go in the intended direction is the worst thing in the world. 
I'm super happy to collaborate and help guide contributions.

Things that would be brilliant contributions:

- Timeouts
- Close(), that interrupts waiters
- A multi-consumer version (leveraging something similar to the producer sequencer already in place)
- A single-producer version (no need for the CAS or complex publishing if there's just one producer)
- Wait strategy that falls back to regular Mutexes for long waits (see SignalAllWhenBlocking in WaitStrategy)
- Sorting out why latency is in the 80ns range instead of 1ns range as expected

# License

Apache 2, see LICENSE