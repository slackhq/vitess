# Snake Benchmark Results

Apple M4 Max (darwin/arm64), 16 cores, Go 1.24. Medians from 6 iterations via
`benchstat` (95% CI shown as ±%).

## Uncontended Latency

Measures the fast-path cost of Acquire+Release and basic CoDel queue operations when no
other goroutines are competing. This is the floor: every request pays at least this much.
The CoDel queue itself is cheap; most of the Snake overhead comes from the mutex, channel
signaling, and valve bookkeeping layered on top.

| Benchmark                                      | ns/op | ±  | B/op | allocs/op |
|------------------------------------------------|------:|---:|-----:|----------:|
| Uncontended Acquire + Release (no valve ID)    | 206   | 0% | 416  | 7         |
| Uncontended Acquire + Release (with valve ID)  | 277   | 1% | 416  | 7         |
| 8-contention Acquire + Release (with valve ID) | 480   | 0% | 416  | 7         |
| CoDel Queue Enqueue + Dequeue                  | 64    | 1% | 240  | 4         |
| CoDel Queue Enqueue only                       | 86    | 6% | 240  | 4         |

## Valve ID Scaling

This test asserts that the bookkeeping for the various valve data structures is all O(1).
The operation is an acquire+release for a given valve ID.

| Distinct valve IDs | ns/op | ±  |
|-------------------:|------:|---:|
| 1                  | 291   | 1% |
| 10                 | 292   | 0% |
| 100                | 291   | 0% |
| 1000               | 294   | 1% |

## GOMAXPROCS Scaling

This test measures how the single-mutex approach fares as the number of threads competing
for the lock increases. Degradation is sublinear.

| GOMAXPROCS | ns/op | ±  |
|-----------:|------:|---:|
| 1          | 278   | 0% |
| 2          | 393   | 1% |
| 4          | 421   | 1% |
| 8          | 442   | 1% |

## findLowestPriorityDroppable (drop target selection)

| Queue Depth | ns/op |  ± |
|------------:|------:|---:|
| 10          | 10    | 3% |
| 100         | 97    | 2% |
| 1000        | 999   | 3% |

This is currently linear, which is expected based on our implementation. We're planning on
modifying our data structures to make the various queue operations O(log(n)). Based on drop
timer intervals for the parameters we expect, this isn't a mandatory optimization, but it's
desirable nonetheless since the drop timer runs more frequently under overload.

## Self-Contention Throughput

| Parallelism (same valve ID) | ns/op |  ± |
|----------------------------:|------:|---:|
| P2                          | 557   | 1% |
| P4                          | 557   | 1% |
| P8                          | 556   | 2% |

When multiple goroutines contend on the same valve ID, throughput is stable. The valve
serializes them anyway (only one enters the CoDel queue at a time), so adding more parallel
goroutines for the same ID doesn't help throughput — but it also doesn't hurt it. This
confirms the valve design works as intended: self-contention is absorbed without degrading
the system.
