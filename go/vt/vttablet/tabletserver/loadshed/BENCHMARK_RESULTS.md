# Snake Benchmark Results

Benchmarks run on Apple M-series (darwin/arm64), Go 1.24, 3 iterations per benchmark.

## Uncontended Latency

| Benchmark                   | ns/op | B/op | allocs/op |
|-----------------------------|------:|-----:|----------:|
| Uncontended (no valve ID)   | 229   | 432  | 9         |
| Uncontended (with valve ID) | 297   | 432  | 9         |

The valve bookkeeping (map lookup + outstanding count increment/decrement) adds ~28% latency
in the uncontended case. Memory allocation is identical — the overhead is pure map operations.
Both paths are well under 1µs, which is negligible compared to any real work the lock protects.

## Contended Latency (parallel goroutines)

| Parallelism | ns/op |
|------------:|------:|
| P4          | 437   |
| P16         | 441   |
| P64         | 444   |
| P256        | 451   |

Remarkably flat across 4–256 goroutines. The single mutex serializes all acquires, so adding
more goroutines doesn't degrade per-operation latency — it just means more goroutines wait.
This is the intended behavior: the lock serializes access to a shared resource, and CoDel
handles the case where wait times grow too large.

## Valve ID Scaling

| Distinct IDs | ns/op |
|-------------:|------:|
| 1            | 303   |
| 10           | 300   |
| 100          | 305   |
| 1000         | 303   |

Flat. The valve maps (`pendingRequests`, `outstandingCounts`, `activePerValve`) use Go maps
with O(1) amortized lookup. Even with 1000 distinct valve IDs cycling through, there's no
measurable degradation. In production, the number of concurrent valve IDs is bounded by the
number of in-flight requests — typically hundreds, not thousands.

## GOMAXPROCS Scaling

| GOMAXPROCS | ns/op |
|-----------:|------:|
| 1          | 293   |
| 2          | 344   |
| 4          | 409   |
| 8          | 431   |

Modest increase with more OS threads. The mutex contention grows as more threads compete for
the lock, but the degradation is sublinear — doubling from 4 to 8 procs adds only ~5%.
The single-mutex design pays a small tax here but keeps the implementation simple and
eliminates an entire class of concurrency bugs.

## Valve Overhead (side-by-side)

| Benchmark           | ns/op |
|---------------------|------:|
| No valve (empty ID) | 240   |
| With valve          | 300   |

Confirms the ~25% overhead from valve bookkeeping. For the uncontended fast path (which is
the common case in production — most requests don't self-contend), this translates to ~60ns
of additional latency per acquire/release cycle.

## Low-Level CoDel Queue Operations

| Benchmark         | ns/op | B/op | allocs/op |
|-------------------|------:|-----:|----------:|
| Enqueue + Dequeue | 93    | 248  | 5         |
| Enqueue only      | 59    | 248  | 5         |

The CoDel queue itself is cheap — most of the Snake overhead comes from the mutex, channel
signaling, and valve bookkeeping layered on top.

## findLowestPriorityDroppable (drop target selection)

| Queue Depth | ns/op |
|------------:|------:|
| 10          | 11    |
| 100         | 118   |
| 1000        | 1,350 |

Linear as expected — the scan walks the entire doubly-linked list to find the lowest-priority
droppable request. At depth 1000 it's ~1.35µs, which is acceptable because the CoDel drop
timer fires infrequently (only during sustained overload) and production queue depths rarely
exceed a few hundred entries. Zero allocations — the scan is pure pointer chasing.

## Self-Contention-Aware Queue

| Benchmark                       | ns/op  |
|---------------------------------|-------:|
| Enqueue + Promote (single pair) | 164    |
| Chain length 2                  | 387    |
| Chain length 5                  | 1,070  |
| Chain length 10                 | 2,148  |
| Chain length 50                 | 10,708 |

Linear scaling at ~215 ns/item in the chain. Each promotion involves a map delete + map insert
+ list push to the CoDel queue. The chain benchmark measures the worst case: N requests arrive
for the same valve ID, then all N are dequeued sequentially. In production, chains are typically
short (2–5 items) because they represent parallel goroutines from a single request handler's
fan-out.

## Allocation Profile

| Benchmark             | B/op | allocs/op |
|-----------------------|-----:|----------:|
| Uncontended           | 432  | 9         |
| With valve            | 432  | 9         |
| Contended (8 workers) | 432  | 9         |

Allocation count is stable regardless of contention level. The 9 allocations per
acquire/release cycle come from: Request struct, channel, list element, priority float,
timer (if armed), and map operations. No allocation scaling with contention — the design
avoids per-waiter allocation growth.

## Self-Contention Throughput

| Parallelism (same valve ID) | ns/op |
|----------------------------:|------:|
| P2                          | 558   |
| P4                          | 562   |
| P8                          | 560   |

When multiple goroutines contend on the same valve ID, throughput is stable. The valve
serializes them anyway (only one enters the CoDel queue at a time), so adding more parallel
goroutines for the same ID doesn't help throughput — but it also doesn't hurt it. This
confirms the valve design works as intended: self-contention is absorbed without degrading
the system.

## Key Takeaways

1. **Fast path is fast.** Uncontended acquire/release is ~230ns — dominated by mutex
   lock/unlock and channel operations, not algorithmic overhead.

2. **Contention is handled gracefully.** Latency doesn't explode under high parallelism;
   it plateaus around ~450ns because the mutex serializes everything predictably.

3. **Valve overhead is modest.** ~25% additional latency for self-contention awareness is
   a good trade for preventing artificial queue pressure from fan-out patterns.

4. **No allocation surprises.** Memory usage is flat regardless of contention level or
   valve ID cardinality.

5. **Linear costs are in the right places.** The only linear operation (valve chain
   promotion) scales at ~215ns/item, which matters only for unusually deep fan-outs.
