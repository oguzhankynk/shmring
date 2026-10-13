# Decisions

One entry per decision: the situation, what was chosen, and what was rejected.

## 1. 64-bit platforms only

**Context.** Ring cursors are 64-bit counters that are also used directly as sizes and indices, and
cross-process atomics must be lock-free.
**Choice.** Support x86-64 and AArch64 only. A `static_assert` on `sizeof(std::size_t)` rejects
32-bit targets at compile time.
**Alternatives.** 32-bit cursors (wrap in minutes at high message rates, so correctness would rest
on wrap-around handling in every comparison); 64-bit cursors with casts for 32-bit `size_t`
(untested complexity for platforms nobody asked for).

## 2. Cursors count forever and are masked only on access

**Context.** A ring needs to tell full from empty.
**Choice.** `head` and `tail` are unmasked `uint64_t` counters. Used slots are `head - tail`; a slot
is `cursor & (N - 1)`. Because `2^64` is divisible by every power-of-two `N`, wrap-around of the
counter does not change the slot sequence, and tests start the counters near `UINT64_MAX` to prove it.
**Alternatives.** Masked indices with one slot kept empty (wastes a slot, and needs the extra
comparison to separate full from empty); a separate "full" flag (a second shared variable).

## 3. Cached copies of the other side's cursor live in the ring, on the owner's cache line

**Context.** Reading the other side's cursor on every operation moves its cache line between cores
each time.
**Choice.** The producer keeps `cached_tail` next to `head`, the consumer keeps `cached_head` next
to `tail`. Only the owner touches them. They are refreshed only when the cached value says the ring
may be full or empty. A stale value is always older than the real cursor, so the worst case is one
unnecessary refresh, including for a process that attaches later.
**Alternatives.** Separate producer and consumer handle objects holding the caches (changes the API:
the ring would no longer have `try_push` itself, and a new peer would need a handle created in the
right order); atomics for the caches (no benefit, nobody else reads them).

## 4. Payload slots are plain memory, not atomics

**Context.** Elements are copied between threads and processes.
**Choice.** Elements are moved with `memcpy`. Correctness comes from the release/acquire pair on the
cursors, which orders the copy before and after the hand-over, exactly as a mutex orders accesses to
the data it protects. `T` must be trivially copyable and needs no default constructor.
**Alternatives.** Making every element atomic (not possible for arbitrary `T`, and slower).

## 5. `seq_cst` only in the blocking layer

**Context.** The queues are one-writer-per-variable and need only release/acquire.
The blocking layer is different: the producer publishes data and then reads "is anyone waiting?",
while the consumer announces "I am waiting" and then re-reads the data. That is a store-then-load
pattern on two different variables, which release/acquire alone does not order.
**Choice.** `event::notify` and `event::wait` each execute one `memory_order_seq_cst` fence between
those two steps. Everything else is release/acquire or relaxed with a stated reason.
**Alternatives.** `seq_cst` on every queue operation (costs a full barrier on the fast path);
always calling `FUTEX_WAKE` (a system call per message).

## 6. Blocking layer is a separate object, not part of `spsc_ring`

**Context.** The requirements ask for a waiting layer outside the lock-free fast path.
**Choice.** `event` is a standalone eventcount (sequence word plus waiter count). `blocking_spsc_ring`
composes an `spsc_ring` with two events. The plain `spsc_ring` has no knowledge of waiting, so its
fast path stays free of fences and flags.
**Alternatives.** A waiter flag inside `spsc_ring` (adds a shared variable to the lock-free path
for users who never block).

## 7. `event` wakes with a changed sequence word, not by waking blindly

**Context.** A waiter that checks the condition and then sleeps can miss a wake-up in between.
**Choice.** The waiter reads the sequence word before re-checking its condition and passes the value
to `FUTEX_WAIT`, which sleeps only if the word is unchanged. `notify` increments the word before
`FUTEX_WAKE`. `FUTEX_WAIT` (not `FUTEX_WAIT_PRIVATE`) is used because the word may be in shared
memory.
**Alternatives.** A mutex and condition variable (not usable lock-free, and process-shared
condition variables do not survive a killed peer); a futex on the ring's cursor itself (couples the
ring layout to the waiting protocol).

## 8. Message ring: 8-byte record header, skip record, half-capacity limit

**Context.** Variable-length records must not wrap and must start on 8-byte boundaries.
**Choice.** Each record is an 8-byte header (length plus reserved word) followed by the payload,
padded to 8 bytes. When a record does not fit before the end of the buffer, the producer writes a
header with a reserved length value that tells the consumer to continue at offset 0. The largest
message is `capacity / 2 - 8` bytes: with that limit a message always fits in an empty ring,
because the skipped tail is shorter than the message that triggered it.
**Alternatives.** Allowing messages up to `capacity - 8` (a message could then never fit when the
write position is mid-buffer, even in an empty ring); a mirrored double mapping of the buffer
(removes the skip record but needs `memfd` or `mmap` tricks and doubles the address space use).

## 9. Message ring capacity must be a power of two

**Context.** The capacity is chosen at run time.
**Choice.** Require a power of two of at least 64 bytes, so wrap-around is a mask instead of a
division on every operation, and header validation treats all queue kinds the same way.
**Alternatives.** Any capacity with `%` (an integer division per call on the fast path).

## 10. `reserve` touches no shared state

**Context.** A producer that stops between `reserve` and `commit` must leave nothing visible.
**Choice.** `reserve` only does bookkeeping in the caller's handle. The skip header, the record
header and the cursor update all happen in `commit`. A message that was reserved but not committed
is indistinguishable from one that was never reserved.
**Alternatives.** Writing the header at `reserve` time with a "not ready" marker (leaves a marker the
consumer has to understand and a dead record that blocks it forever).

## 11. `segment::open` waits for the creator by default

**Context.** An opener can run before the creator has finished building the queue.
**Choice.** `open` takes an optional timeout; the default waits forever, zero does not wait. It spins
briefly, then sleeps with a backoff from 50 microseconds to 10 milliseconds. A file smaller than
the header counts as "not ready". A name that does not exist is `not_found` immediately.
**Alternatives.** Reject immediately (makes every caller write the same retry loop); wait for the
file to appear too (hides typos in names behind an infinite wait).

## 12. Segments are never unlinked implicitly

**Context.** Several processes attach to one name.
**Choice.** Destroying a `segment` only unmaps it. `segment::unlink` removes the name. The creator
is not special at destruction time.
**Alternatives.** A creation flag that unlinks in the creator's destructor (a creator that exits
early would pull the name out from under processes that have not opened it yet).

## 13. Header values are checked, not used

**Context.** The segment is untrusted input.
**Choice.** Opening compares every header field with what the caller's template parameters say, and
the capacity used for addressing is the compile-time `N` (or, for the runtime-sized message ring, a
copy of the validated value made once at open time). No offset is ever read from shared memory.
**Alternatives.** Trusting header fields for addressing after one validation (a peer can change
them afterwards).

## 14. The cache line size is a build-time constant stored in the header

**Context.** `std::hardware_destructive_interference_size` can differ between compiler versions
and warns as ABI-unstable; the layout must match in every process that shares a segment.
**Choice.** `SHMRING_CACHE_LINE` (default 64, CMake option `SHMRING_CACHE_LINE_SIZE`), recorded in
the segment header so a mismatch is rejected.
**Alternatives.** The standard constant; detecting the line size at run time (layouts would not be
compile-time constants).

## 15. MPMC slots are not padded to a cache line

**Context.** Vyukov's queue puts a sequence number next to each element.
**Choice.** A slot is the 8-byte sequence plus the element, rounded to the element's alignment. For
small elements several slots share a cache line, which keeps the ring small and lets neighbouring
operations share cache misses.
**Alternatives.** One cache line per slot (eliminates false sharing between neighbouring slots at the
cost of 8x the memory for 8-byte elements).

## 16. Benchmarks report medians and ranges over repetitions

**Context.** The development machine is WSL2. The same code on the same pair of CPUs gives a
median round trip of about 70 ns in one process run and about 200 ns in the next, because the
hypervisor decides which host core each virtual CPU uses.
**Choice.** Every measurement is repeated (5 times for latency, 3 for throughput). Tables show the
median of each percentile and the lowest and highest p50 seen, so the spread is part of the result.
`BENCHMARKS.md` explains the effect instead of hiding it.
**Alternatives.** Reporting the best run (overstates what a user will see); a single long run
(would silently pick one of the two modes).

## 17. Broadcast readers keep their position privately and the writer never waits

**Context.** A broadcast queue can either track every reader (so the writer can wait for the slowest)
or let slow readers lose data.
**Choice.** Readers own their position; the writer overwrites the oldest element regardless. A reader
that was lapped is told (`read_status::overrun`), skips forward and counts what it lost. This keeps
`publish` wait-free and makes readers free to join, leave or crash.
**Alternatives.** A registry of reader cursors in shared memory (needs slots to claim and release,
and a crashed reader would stall the writer or leak a slot); making the writer wait for the slowest
reader (turns a stopped reader into a stopped writer).

## 18. Seqlock payload is copied with atomic words

**Context.** A reader may copy a slot while the writer rewrites it. Discarding the copy afterwards
does not make the concurrent plain access legal.
**Choice.** The slot payload is an array of relaxed `atomic<uint64_t>`. The writer and readers
access it with atomic loads and stores, and the stamp check decides whether the copy is consistent.
**Alternatives.** `memcpy` of plain bytes with a stamp check (a data race by the C++ rules; the
sanitizers report it); `volatile` (not a synchronization tool).

## 19. The analyzers see the header-only queues through an explicit instantiation file

**Context.** The queues are templates in headers. clang-tidy and cppcheck only analyze code that a
compiled target includes, and the template bodies are only checked when they are instantiated, so
the library target alone would have left the queues unanalyzed.
**Choice.** `src/instantiations.cpp` includes every public header and instantiates every template
once. It is built as a separate object target that is never linked or run and is not instrumented
for coverage. clang-tidy's header filter was also corrected, because the template's pattern did not
match absolute paths and so silenced all header diagnostics.
**Alternatives.** Running the analyzers on the test sources (mixes test code into the findings);
relying on the library target (analyzes none of the queue code).

## 20. Naming follows the standard-library style the API uses

**Context.** The API in the requirements uses `spsc_ring`, `result`, `segment::create`. The
template's clang-tidy naming rules asked for CamelCase types and `k`-prefixed constants.
**Choice.** The naming checks stay on, with the rules changed to match the API: lower_case types,
enums, functions, variables and constants; CamelCase template parameters.
**Alternatives.** Renaming the public API to CamelCase (contradicts the requested names); turning
the naming check off (loses the consistency check).

## 21. A few targeted `NOLINT` directives, each with its reason

**Context.** The core-guidelines checks forbid pointer arithmetic, C arrays, `reinterpret_cast` and
varargs. A ring buffer over raw memory, a seqlock slot, a futex system call and an alignment check on
an address cannot avoid them.
**Choice.** The checks stay enabled for the whole project. The unavoidable uses are confined to small
helpers (`slot`, `copy_in`/`copy_out`, `byte_at`/`header_at`, `cell_at` with the word copy
functions, `futex_wait`/`futex_wake`) and each carries a `NOLINT` naming the check, with the reason
in the line above. Everything else was fixed in the code.
**Alternatives.** Disabling the checks in `.clang-tidy` (hides future real findings); wrapping the
raw memory in a bounds-checked abstraction (adds a branch to the fast path).

## 22. Coverage counts source lines, not template instantiations

**Context.** The queues are templates, and the tests instantiate them with many element types and
capacities. gcovr counts each instantiation's copy of a line separately by default, which turns
"this error path ran" into "this error path ran for this element type", and the percentage fell
from 98 % to 85 % without any code going untested.
**Choice.** `--merge-lines`: a source line is covered when any instantiation executed it. The
thresholds (90 % lines, 60 % branches) are unchanged. The stress tests also execute small functions
billions of times, which gcovr's default sanity limit on hit counts rejects, so the limit is raised
to match.
**Alternatives.** Lowering the thresholds (rejected); requiring every error path for every
instantiation (would need a test per element type for code that does not depend on the type).
