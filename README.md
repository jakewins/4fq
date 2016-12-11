# 4FQ (Four Fast Queues)

An ambitious name for a project with just one queue in it!

This repository contains a fast Multi-Producer/Single-Consumer message queue for Go. 
As the name implies, the intention is to add the three missing variants (SPSC, SPMC, MPMC).

For some use cases, the approach these queues take to goroutine coordination can substantially improve your systems performance.
Specifically, you may benefit from using this queue if:

- Your application is focused on high throughput low-latency processing (as in, the cost of processing is low, but volume is high)
- Your application has been found to excessively use CPU cycles for garbage collection
- The receiver of messages is able to process multiple messages at a time

## Technical details



# License

Apache 2, see LICENSE