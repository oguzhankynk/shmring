# shmring

Lock-free queues for passing messages between processes (and threads) on one Linux host through
POSIX shared memory. The fast path takes no locks, makes no system calls and does no heap
allocation. Written in C++17, header-only queues plus a small compiled library for segment
management and futexes.

Every number in this repository was measured; the memory-ordering reasoning is written down and
tested. See [docs/DESIGN.md](docs/DESIGN.md) for how each queue works,
[docs/DECISIONS.md](docs/DECISIONS.md) for why, and [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for the
measurements.

## The queues

| Queue | Producers / consumers | Elements | Progress guarantee |
|---|---|---|---|
| `spsc_ring<T, N>` | 1 / 1 | fixed-size, trivially copyable | wait-free |
| `spsc_msg_ring` | 1 / 1 | variable-length messages, written and read in place | wait-free |
| `mpmc_ring<T, N>` | many / many | fixed-size, trivially copyable | lock-free (not wait-free) |
| `spmc_ring<T, N>` | 1 / many (broadcast) | fixed-size, trivially copyable | wait-free; slow readers lose data |
| `blocking_spsc_ring<T, N>` | 1 / 1 | as `spsc_ring`, with optional sleeping | non-blocking calls wait-free; `push`/`pop` may sleep on a futex |

"Wait-free" means every call finishes in a bounded number of steps whatever the other side does.
"Lock-free" means the system as a whole always makes progress, but one call can retry.

Queues can live in ordinary memory (threads) or in a shared-memory segment (processes). A segment
has a header that is checked when another process attaches: magic number, layout version, queue kind,
element size and alignment, capacity (a power of two that fits in the mapped memory) and cache line
size. The segment is treated as untrusted input.

## Quick start

Build, then run the producer in one terminal and the consumer in another:

```sh
cmake --preset release
cmake --build --preset release
./build/release/examples/quickstart_producer     # terminal 1
./build/release/examples/quickstart_consumer     # terminal 2
```

The programs are in [examples/](examples/). The producer is:

```cpp
#include "shmring/shared_queue.hpp"

using Ring = shmring::shared_spsc_ring<std::uint64_t, 1024>;

auto ring = Ring::create("/shmring_quickstart");          // creates and publishes the segment
if (!ring) { /* ring.error().code, ring.error().sys_errno */ }
for (std::uint64_t i = 0; i < 1000000; ++i) {
  while (!ring.value().queue().try_push(i)) {}            // full: the consumer has not caught up
}
Ring::unlink("/shmring_quickstart");                      // removes the name when you are done
```

and the consumer:

```cpp
auto ring = Ring::open("/shmring_quickstart");            // waits until the producer has finished
std::uint64_t value;
while (!ring.value().queue().try_pop(value)) {}
```

`open` waits for the creator to publish the segment: forever by default, or up to a timeout you pass
(`0` means do not wait). It reports `not_found` at once if the name does not exist.

### Variable-length messages, written in place

```cpp
auto ring = shmring::shared_spsc_msg_ring::create("/telemetry", 1 << 20);   // capacity: a power of two

// producer
shmring::span<std::byte> room = ring.value().ring().reserve(length);       // nothing is visible yet
if (room) { /* write up to `length` bytes into room.data() */
            ring.value().ring().commit(); }                                 // publish

// consumer
shmring::span<const std::byte> message = ring.value().ring().peek();       // a view into the ring
if (message) { /* use message.data(), message.size() */
               ring.value().ring().release(); }                             // free the space
```

[examples/wireforge/](examples/wireforge/) shows frames from a
[WireForge](https://github.com/okaynak/wireforge)-generated serializer written straight into a
reservation.

### Sleeping instead of spinning

`blocking_spsc_ring` adds `push(value, timeout)` and `pop(value, timeout)`. A full or empty ring puts
the caller to sleep on a futex; the other side wakes it. A call with nobody asleep costs a fence and
a load, not a system call. `event` is the underlying building block.

## What happens when a peer dies

The surviving side never sees a torn or half-written message. A new process can attach to the same
segment and carry on. What can be lost:

| Queue | Producer killed | Consumer killed |
|---|---|---|
| `spsc_ring` | Published elements stay readable. An element that was being written, not yet published, is lost. | Unread elements stay. The element being read may be delivered again to the next consumer, or counted as consumed if the consumer died after taking it. |
| `spsc_msg_ring` | Same. A message that was `reserve`d but not `commit`ted is never visible. | Same. |
| `spmc_ring` | Readers see nothing new. A new writer continues from the head. | A reader's position is private; nothing to repair. |
| `mpmc_ring` | **Known limitation:** a producer that dies between claiming a slot and publishing it blocks consumers at that slot (`try_pop` keeps returning `false`) and eventually makes the ring look full. The library does not detect or repair this. Use one `spsc_ring` per producer where producer crashes must be survivable. | A consumer that dies mid-pop leaves the slot unreleased, which blocks producers at that slot in the same way. |

There is no liveness detection (heartbeat or epoch); the library does not take over a dead peer's
resources by itself.

## Building and testing

Requirements: Linux on a 64-bit platform (x86-64 or AArch64; 32-bit is not supported), GCC 11+ or
Clang 14+, CMake 3.25+, Ninja.

```sh
cmake --preset debug && cmake --build --preset debug && ctest --preset debug
```

| Preset | Purpose |
|---|---|
| `debug`, `release` | Plain builds with warnings as errors. |
| `asan` | AddressSanitizer + UndefinedBehaviorSanitizer. |
| `tsan` | ThreadSanitizer. |
| `coverage` | `--target coverage` runs the tests and writes a gcovr report; fails below 90 % line / 60 % branch coverage. |
| `tidy`, `cppcheck` | Build with clang-tidy / cppcheck. |

`./scripts/run_checks.sh` runs every gate: clang-format, clang-tidy, cppcheck, Doxygen coverage, the
tests, both sanitizer builds, coverage, and an install / `find_package` check.

The tests, in `tests/`:

* `unit/`: boundaries, wrap-around, counter overflow, every rejection path of the header validation,
  `reserve`/`commit` edge cases, corrupted-memory cases.
* `stress/`: millions of messages between threads and between processes, each carrying a sequence
  number and redundant fields (`ctest -L stress`), and tests that `SIGKILL` a producer or consumer
  while it works.
* `alloc/`: replaces `operator new` and checks that the fast path of every queue allocates nothing.
* `fuzz/`: libFuzzer targets for the header validation and for queues built on arbitrary bytes
  (`-DSHMRING_BUILD_FUZZERS=ON`, needs Clang). Run them as `setarch -R ./fuzz_header ...`: Clang 14's
  AddressSanitizer crashes at start-up on kernels with high ASLR entropy, and `setarch -R` turns
  address randomization off.

Options: `SHMRING_CACHE_LINE_SIZE` (64; 128 suits some AArch64 cores; every process sharing a queue
must use the same value), `SHMRING_SANITIZER`, `SHMRING_ENABLE_COVERAGE`,
`SHMRING_BUILD_BENCHMARKS`, `SHMRING_BUILD_FUZZERS`, `SHMRING_BUILD_WIREFORGE_EXAMPLE`.

## Benchmarks

`scripts/bench.sh` builds in release mode, runs everything and writes
[docs/BENCHMARKS.md](docs/BENCHMARKS.md) with the environment it ran in. It compares the queues with
pipes, Unix sockets, POSIX message queues and `boost::lockfree::spsc_queue`.

Headline numbers from one run on an Intel Core i3-12100F under WSL2, two processes pinned to two
cores (details, spread and caveats in [docs/BENCHMARKS.md](docs/BENCHMARKS.md)):

| | Round trip, 8 bytes (p50) | One way, 64 bytes | One way, 4096 bytes |
|---|---|---|---|
| `spsc_ring` (busy-poll) | 70 ns best phase, 212 ns worst phase | 276 M msg/s, 17.6 GB/s | 9.0 M msg/s, 37 GB/s |
| `spsc_ring`, batches of 32 | | 267 M msg/s, 17.1 GB/s | 5.1 M msg/s, 20.7 GB/s |
| `spsc_msg_ring` | 75 ns / 254 ns | 138 M msg/s, 8.8 GB/s | 8.8 M msg/s, 36 GB/s |
| `mpmc_ring`, one producer | 82 ns / 172 ns | 61 M msg/s, 3.9 GB/s | 8.0 M msg/s, 33 GB/s |
| `blocking_spsc_ring` (futex) | 25 us | 39 M msg/s, 2.5 GB/s | 5.3 M msg/s, 22 GB/s |
| pipe | 25 us | 5.5 M msg/s, 0.35 GB/s | 1.5 M msg/s, 6.3 GB/s |
| Unix socket, `SOCK_SEQPACKET` | 26 us | 1.9 M msg/s, 0.12 GB/s | 1.4 M msg/s, 5.7 GB/s |
| POSIX message queue | 25 us | 0.8 M msg/s, 0.05 GB/s | 1.2 M msg/s, 4.8 GB/s |
| `boost::lockfree::spsc_queue` (threads, one process) | 66 ns / 250 ns | 114 M msg/s, 7.3 GB/s | 8.3 M msg/s, 34 GB/s |

The two latency figures for busy-polling rows are the lowest and the highest p50 over 21
repetitions. They differ because WSL2 moves the virtual CPUs between host cores; the report shows
the same measurement repeated 16 times and explains how to read it. Kernel transports are stable.
The queues work between processes; the Boost row is in-process only.

## Installing

```sh
cmake --install build/release --prefix /path/to/prefix
```

```cmake
find_package(shmring CONFIG REQUIRED)
target_link_libraries(your_target PRIVATE shmring::shmring)
```

## Repository layout

```
include/shmring/  public headers (the queues are header-only)
src/              segment management and futex wrappers
tests/            unit, stress, crash and allocation tests
fuzz/             libFuzzer targets
bench/            latency, throughput and Google Benchmark programs
examples/         quick start, message ring demo, WireForge example
docs/             DESIGN.md, DECISIONS.md, BENCHMARKS.md
cmake/ scripts/   build modules, run_checks.sh, bench.sh
```

## License

MIT, see [LICENSE](LICENSE).
