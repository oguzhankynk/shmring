## How to read these numbers

**The machine is WSL2.** The CPUs are virtual, and the hypervisor decides which host core each one
runs on. The guest cannot control that, and `sched_setaffinity` only pins to a virtual CPU. The
visible effect is that the same code, on the same pair of CPUs, shows different round-trip
latencies from one moment to the next. The *Run-to-run variation* section below repeats one
measurement 16 times and shows two or three clusters of values, not one. On bare metal with isolated
cores one would expect a single, stable value; that was not measured here.

Because of this, a single run, or a median over runs, says little about the code:

* The busy-poll rows report the **lowest** and the **highest** p50 seen across repetitions. Compare
  rows with the *p50 min* column (the fastest phase of the machine) or with *p50 max* (the slowest
  phase). The *p50 med* column depends on how many repetitions happened to fall in each phase, so
  two rows of equal speed can show very different medians.
* Repetitions are interleaved: each round measures every row once (or every few rounds for the
  slow rows), so all rows see the same mix of phases over the whole run.
* Kernel transports (pipe, sockets, message queue, futex wake-ups) are not affected: their cost is
  dominated by system calls and scheduler wake-ups, and their repetitions agree closely.
* The throughput table shows the median over repetitions with the slowest and fastest repetition
  beside it. Rows with a wide range (for example 8-byte messages) are in the phase-dependent regime.

**What is compared.**

* Every transport delivers each message into a buffer owned by the receiver, so the copy is
  included everywhere. The `spsc_msg_ring` rows copy out of the ring view for the same reason; the
  zero-copy API could be faster for consumers that read messages in place.
* The shared-memory rows run between two processes. The two `threads` rows (an `spsc_ring` and
  `boost::lockfree::spsc_queue`) run inside one process and are labelled as such; Boost has no
  cross-process mode.
* The ring in each throughput run is about 1 MiB, so it fits in the cache. The GB/s figures are
  cache-to-cache copy rates between two cores, not DRAM bandwidth.
* Message-ring records carry an 8-byte header that is not counted as payload.

**What the results show** (see the tables for the numbers; these statements held in every run made
on this machine).

* A busy-poll round trip through shared memory takes tens to a couple of hundred nanoseconds. A round
  trip through a pipe, a Unix socket or a POSIX message queue takes about 25 microseconds on this
  machine: more than a hundred times as long.
* The futex-based blocking ring has about the same latency as the kernel transports. That is
  expected: waking a sleeping thread goes through the same scheduler path. Blocking mode trades
  latency for not burning a core; it is not a faster way to wait.
* For small messages, shared-memory throughput is tens to hundreds of times that of kernel IPC. At
  4 KiB the gap shrinks to the memory-copy cost, but shared memory still wins several times over.
* The batch API (32 messages per publication) helps most for tiny messages: about four times the
  rate of the scalar API at 8 bytes. From 64 bytes up it does not help, because copying dominates.
* `spsc_msg_ring` matches `spsc_ring` at 8 bytes and from about 1 KiB up, and is slower at 64 and
  256 bytes, where its record header and variable length matter. `mpmc_ring` pays for a
  compare-and-swap and a sequence number per slot, and the blocking ring pays a full fence on every
  push and pop so that wake-ups cannot be lost; both costs are visible in the table.
* Against `boost::lockfree::spsc_queue`, which runs inside one process, `spsc_ring` is about equal
  at 8 bytes and at 4 KiB (the min-max ranges overlap) and ahead from 64 bytes to 1 KiB. One likely
  reason is that `spsc_ring` keeps a private copy of the other side's cursor and re-reads the shared
  cache line only when the ring looks full or empty. That explanation was not tested.
* In the single-operation costs, an `spsc_ring` push followed by a pop costs about the same as the
  Boost queue on one thread; `mpmc_ring` costs several times more.

**Not done.** No frequency governor control (WSL2 exposes none), no `isolcpus`, no hyperthread
isolation, one machine, one compiler. Latencies are taken with `std::chrono::steady_clock`, whose own
cost (tens of nanoseconds) is included in every round trip.
