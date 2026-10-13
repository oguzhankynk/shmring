# shmring — Design

This document explains how each queue works, which side owns which variable, which memory order
every atomic operation uses and what it pairs with, what progress each operation guarantees, and
what happens when a peer dies. Why a choice was made over its alternatives is in
[DECISIONS.md](DECISIONS.md).

## 0. Conventions used throughout this document

- **Index type.** All ring cursors (`head`, `tail`, `write_cursor`, `read_cursor`, …) are
  monotonically increasing, *unmasked* `uint64_t` counters — they count every element/byte ever
  produced or consumed, not a position modulo capacity. The physical slot/offset is computed only
  at the point of access: `data[cursor & (N - 1)]`.
  - This is safe across unsigned wraparound: for a power-of-two `N`, `2^64 mod N == 0`, so wrapping
    the counter from `UINT64_MAX` to `0` and then masking gives the same slot sequence as if the
    counter had never wrapped. No special-casing is needed near the overflow boundary — which is
    exactly the case the unit tests exercise by seeding counters near `UINT64_MAX`.
  - "Is it full / how many in flight" is computed as `producer_cursor - consumer_cursor` using
    wrapping unsigned subtraction. This is correct as long as the true in-flight count never
    exceeds `2^63` (true for any `N` we will ever use), the same trick TCP uses for sequence
    numbers.
- **No pointers in shared memory.** Every reference to another part of the segment is a `uint64_t`
  index or byte offset, resolved against each process's own base address *after* `mmap`. No field
  ever stores a `T*`/`void*` that was valid in one process's address space.
- **Non-atomic payload, atomic handshake.** Per the C++ memory model, two conflicting accesses
  (one of them a write) by different threads are only undefined behavior if they are *not* ordered
  by a happens-before edge — the accesses themselves don't need to be `std::atomic`. The ring's
  payload slots are plain (non-atomic) reads/writes; correctness comes entirely from the
  release/acquire pair on the cursors, which is exactly how a mutex protects plain data without the
  protected data itself being atomic. This matters for performance: no atomic RMW, no fences, on
  the payload.
- **Memory order policy.** `memory_order_seq_cst` is not used in §§2-4. Every
  synchronization point below is a single producer/consumer release-acquire pair, which is
  sufficient because each queue has at most one writer per cursor. The one exception is the blocking
  layer in §5, which needs sequentially consistent fences and explains why there.
- **Cache line size.** `constexpr std::size_t cache_line = SHMRING_CACHE_LINE` (default 64,
  CMake option to override, e.g. 128 for some AArch64 cores). We do not use
  `std::hardware_destructive_interference_size` — GCC documents its value as not ABI-stable, and a
  value baked into one process's binary must match what every other process attached to the same
  segment assumes, so it has to be a project-controlled constant, not a standard-library constant
  that could change between compiler versions.

## 1. Shared-memory segment header

Every segment (regardless of queue kind) starts with a fixed-layout header at offset 0:

```
offset  field                               written by        when
------  -----                               ---------          ----
0x00    uint64_t magic                      creator            create()
0x08    uint32_t layout_version              creator            create()
0x0C    uint32_t queue_kind                  creator            create()
0x10    uint32_t elem_size                   creator            create()
0x14    uint32_t elem_align                  creator            create()
0x18    uint64_t capacity                    creator            create()
0x20    uint32_t creator_pid                 creator            create()
0x24    atomic<uint32_t> init_state          creator            create() -> attach
0x28    uint32_t cache_line_size             creator            create()
0x2C    [padding to cache_line]
        -- end of header, starts at a cache_line boundary --
        queue-kind-specific control block (e.g. §2's control_block)
        queue-kind-specific data region
```

`init_state` is `UNINIT(0) -> INITIALIZING(1) -> READY(2)`:

1. `create()` maps the segment, writes every header field above except `init_state` with plain
   stores (no other process can see this memory yet — it was just created), zeroes the control
   block and data region, then does `init_state.store(READY, memory_order_release)`.
2. `open()` maps the segment and does `init_state.load(memory_order_acquire)`. The acquire here is
   what makes every plain-store field written in step 1 visible: it pairs with the release in step
   1, so once `open()` observes `READY` it is guaranteed to see the fully-written header, not a
   partial one.
3. `open()` **validates before trusting anything else**: magic, version, kind, `elem_size`,
   `elem_align`, and that `capacity` is a power of two and fits inside the actual mapped size
   (`fstat` the fd, don't trust a `capacity` field larger than what was actually mapped). Any
   failure returns `result<segment, error>` with a specific error code — it never asserts or
   crashes on attacker-controlled bytes. This is also exactly the validation the libFuzzer target
   exercises directly, without going through `shm_open`.
4. If `init_state` is `UNINIT` or `INITIALIZING` when `open()` loads it: `open()` takes an optional
   `timeout` parameter (`std::optional<std::chrono::nanoseconds>` or equivalent). **Default: wait
   forever** (no deadline) — the common case is attaching to a segment another process is actively
   creating, and giving up early would just turn into the caller's own retry loop anyway. An
   explicit timeout of `0` means "reject immediately without polling"; any positive value is the
   deadline.
   - Poll strategy (process-local, no shared state involved): spin re-checking
     `init_state.load(memory_order_acquire)` for a bounded number of iterations first (a few dozen),
     since the gap between `INITIALIZING` and `READY` is normally a handful of plain stores plus one
     atomic store — microseconds, not worth a syscall. If still not `READY`, fall back to sleeping
     with exponential backoff starting at ~50µs, doubling up to a ~10ms cap, re-checking the deadline
     (via `CLOCK_MONOTONIC`) before each sleep. This bounds both wasted CPU (no busy-spin past the
     first few checks) and wake-up latency (never sleeps longer than 10ms past the point `READY`
     actually lands).
   - If the deadline passes while still not `READY`, return `result<segment, error>` with a
     dedicated `error::timed_out` (or `error::not_ready` for the `timeout == 0` case) — never block
     past the caller's stated deadline, never silently proceed with an unready segment.

The opener can find the file before the creator has sized it, so `open()` treats "file smaller
than the header" the same as "not ready yet" and waits for it under the same timeout.
`cache_line_size` is recorded because the queue layout depends on it; a mismatch is rejected
(`bad_cache_line`). Header values are only compared against what the opener expects (compile-time
`N`, `sizeof(T)`); they are never used to compute addresses, so a peer that changes them after
validation cannot steer an access out of bounds.

Rejected indices are masked (`idx & (capacity - 1)`) on every access that derives from the header's
`capacity`, so even a corrupted-but-passed-validation capacity can't be used to compute an
out-of-bounds offset once we've confirmed it's a power of two no larger than the mapped region.

## 2. SPSC typed ring — `spsc_ring<T, N>`

### 2.1 Memory layout

```
                     cache_line 0           cache_line 1          cache_line 2 ..
                  +------------------+   +------------------+   +------------------------+
control_block:    | atomic<uint64_t> |   | atomic<uint64_t> |   |    T data[N]           |
                  |   head           |   |   tail           |   |  (starts cache-aligned)|
                  | cached_tail      |   | cached_head      |   |                        |
                  | (producer-owned) |   | (consumer-owned) |   |                        |
                  | pad to line size |   | pad to line size |   |                        |
                  +------------------+   +------------------+   +------------------------+
```

- `head`: the number of elements ever pushed. **Written only by the producer.** Published with
  `release`; the consumer reads it with `acquire`.
- `tail`: the number of elements ever popped. **Written only by the consumer.** Published with
  `release`; the producer reads it with `acquire`.
- `head` and `tail` are each alone on their own cache line (padded), so the producer writing
  `head` every push never false-shares with the consumer writing `tail` every pop.
- `data[N]` is `T`, `N = capacity()`, power-of-two (`static_assert`), starts at a fresh cache line
  after `tail`'s padding.
- Each side additionally keeps a **cached copy** of the other side's cursor (`cached_tail` for the
  producer, `cached_head` for the consumer). These are plain `uint64_t` members stored on the
  owner's own cache line, next to `head` / `tail` respectively, so only the owning side ever reads
  or writes them and they cost no extra cache traffic. They are only refreshed (with an acquire
  load of the real atomic) when the cached value says the ring might be full/empty. A stale cache
  can only be too old, never ahead of the real cursor, so a process that attaches later and finds
  old values there just refreshes on its first operation.
- `T` need not be `std::atomic` — see the "non-atomic payload" convention in §0. `T` must be
  trivially copyable (`static_assert`), since slots are moved with plain assignment/`memcpy`, not
  constructed/destructed per the usual object lifetime rules.
- `spsc_ring<T, N>` is a single object holding the control block and `data[N]`. It can live on
  the stack, on the heap or inside a mapped segment (placement-constructed); the algorithm is the
  same in all cases, so its API never takes a `segment&`. Elements are stored as raw bytes and moved
  with `memcpy`, so `T` needs no default constructor.
- The constructor takes an optional start value for both counters. The default is 0; tests pass a
  value near `UINT64_MAX` to exercise wrap-around.

### 2.2 Operations, atomics, and memory order

Both `head` and `tail` below are the real shared atomics; `h`/`t` with no `atomic` qualifier are the
caller's local unmasked counters (kept in the handle object, one per side).

**`try_push(const T& v)`** (producer side; `h` = producer's local copy of `head`'s value, always
up to date since the producer is the only writer):

| step | action | memory order | pairs with |
|---|---|---|---|
| 1 | if `h - cached_tail < N`, skip to 3 (room already known) | — | — |
| 2 | `cached_tail = tail.load(...)` | `acquire` | step 6's `tail.store(release)` from some *earlier* pop — establishes that the consumer finished reading the slot we're about to overwrite before we overwrite it |
| 2a | if still `h - cached_tail >= N`, return `false` (full) | — | — |
| 3 | `data[h & (N-1)] = v` (plain store) | none (plain) | made visible to the consumer by step 4's release |
| 4 | `head.store(h + 1, ...)` | `release` | consumer's step 2 `head.load(acquire)` below |
| 5 | `h += 1` (local) | — | — |

**`try_pop(T& out)`** (consumer side; `t` = consumer's local copy of `tail`'s value):

| step | action | memory order | pairs with |
|---|---|---|---|
| 1 | if `cached_head != t`, skip to 3 (data already known present) | — | — |
| 2 | `cached_head = head.load(...)` | `acquire` | producer's step 4 `head.store(release)` above — makes the plain write to `data[]` in step 3 visible before we read it |
| 2a | if still `cached_head == t`, return `false` (empty) | — | — |
| 3 | `out = data[t & (N-1)]` (plain load) | none (plain) | happens-after producer's step 3 write via the step-2 acquire |
| 4 | `tail.store(t + 1, ...)` | `release` | producer's step 2 `tail.load(acquire)` above, for a *future* push into this same slot |
| 5 | `t += 1` (local) | — | — |

No `seq_cst`: there are only two threads touching these atomics, and each atomic has exactly one
writer, so acquire/release gives a complete happens-before chain without needing a global total
order.

**`try_push_n` / `try_pop_n`** (batch): identical reload/check logic, but compute
`available = N - (h - cached_tail)` (or symmetric for pop) once, copy
`min(n, available)` elements with at most two `memcpy`s (split at the physical wrap point if the
run crosses it), and issue a **single** `head.store(release)` / `tail.store(release)` for the whole
batch rather than one per element. This keeps the fast path wait-free (bounded work per call,
proportional to elements moved, no retry loop) while amortizing the one unavoidable atomic
store/load pair over many elements — the actual throughput win over the scalar API.

**`size_approx()`**: `head.load(memory_order_relaxed) - tail.load(memory_order_relaxed)`. Relaxed
is correct and intentional — the name documents that the result can be stale the instant it's
returned; it's a monitoring/diagnostic read, not something the ring's own correctness depends on.

**`capacity()`**: returns `N`, a compile-time constant — no atomic access.

### 2.3 Progress guarantee

**Wait-free.** Every operation (`try_push`, `try_pop`, including the `_n` batch forms) completes in
a bounded number of steps that does not depend on what the other thread is doing: there is no
CAS-retry loop, because each atomic has exactly one writer (no contention to retry against) — the
only "failure" outcome is a clean `false`/empty-span return when the ring is genuinely full or
empty, which is itself a bounded, single-pass check.

### 2.4 Crash semantics

| scenario | outcome |
|---|---|
| Producer crashes after writing `data[h & (N-1)]` (step 3) but before `head.store` (step 4) | Consumer never sees it: its acquire-load of `head` in its own step 2 cannot observe a value the producer never stored. The write is simply lost — this is the "in-flight message can be lost" case, and must be stated as such in the README. |
| Producer crashes mid-`head.store` | The store is a single aligned 8-byte atomic store; it is indivisible — either it happened (and the consumer can see the new element) or it didn't (nothing to see). No torn index is observable. |
| A new producer process attaches after the old one died | It continues from the `head` value left in shared memory. No special recovery step is needed for the ring itself; detecting *that* the old producer is gone is a separate liveness concern (heartbeat/epoch), out of scope for the ring: the library has no liveness detection. |
| Consumer crashes after `data[t & (N-1)]` read (step 3) but before `tail.store` (step 4) | The slot is still occupied from the producer's point of view (`tail` never advanced), so a new consumer that attaches reads that same element again. Nothing is lost, but the element can be **delivered twice**. |
| Consumer crashes after `tail.store` (step 4), while the application is still processing the element | The ring considers the element consumed. A new consumer does not see it again. From the application's point of view it is **lost**. |
| Consumer crashes mid-`tail.store` | Same indivisibility argument as the producer's `head.store` — no torn value observable by the producer. |

Delivery is therefore at-least-once at the ring level, and at-most-once as seen by an
application that crashes after `try_pop` returns. The README states both cases.

A peer that attaches loads its local counter from the shared atomic first (`h = head.load(acquire)`
or `t = tail.load(acquire)`) and starts with an empty cached copy of the other side's index, so the
first operation always refreshes it.

In all cases: **no torn or partially-written payload is ever observable**, because a slot only
becomes visible to the other side through the one release/acquire-guarded cursor update, and that
update is a single atomic store of an 8-byte value.

## 3. SPSC variable-length message ring — `spsc_msg_ring`

### 3.1 Memory layout

```
 cache line 0                 cache line 1                 then
+---------------------+     +---------------------+     +-----------------------------+
| atomic write_cursor |     | atomic read_cursor  |     | byte buffer[capacity]       |
| cached_read         |     | cached_write        |     | records, 8-byte aligned     |
| (producer-owned)    |     | (consumer-owned)    |     | (capacity is a power of 2)  |
+---------------------+     +---------------------+     +-----------------------------+
```

The cursors are unmasked `uint64_t` byte counters, as in §2. `spsc_msg_ring` itself is a small handle
(pointer to the control block, pointer to the buffer, capacity, plus the producer's and consumer's
local bookkeeping); it is created with `init` by the creator and with `attach` by everyone else. The
shared memory contains no pointers. The capacity comes from the segment header for the
runtime-sized case; the opener copies the validated value once.

A record is `[atomic<uint32_t> length][uint32_t reserved][payload][padding to 8]`. Its total size is
`8 + round_up(length, 8)`. A header whose length is `0xFFFFFFFF` is a skip record: the rest of the
buffer up to the physical end is unused and the next record starts at offset 0. Messages are limited
to `capacity / 2 - 8` bytes (see DECISIONS.md, entry 8); with that limit a record always fits in an
empty ring, because the space skipped at the end is smaller than the record that caused the skip.

### 3.2 Operations

**`reserve(len)`** (producer): purely local. It reads its own `write_cursor`, computes where the
record goes (and, if the record would cross the end of the buffer, how many bytes to skip), checks
free space against `cached_read`, and refreshes `cached_read` with an acquire load of `read_cursor`
only if the space looks insufficient. It returns a view of the payload area and writes nothing to
shared memory. A producer that stops after `reserve` has changed nothing anyone can see.

| step | action | memory order | pairs with |
|---|---|---|---|
| 1 | `w = write_cursor.load` | relaxed (own variable) | — |
| 2 | if free space `< skip + total`: `cached_read = read_cursor.load` | acquire | consumer's `release()` store: the consumer finished reading everything below the cursor, so the space may be overwritten |
| 3 | still not enough: return empty view | — | — |

**`commit()`** (producer): all stores that make the record visible happen here.

| step | action | memory order | pairs with |
|---|---|---|---|
| 1 | if skipping: skip header `length.store(0xFFFFFFFF)` | relaxed | published by step 3 |
| 2 | record header `length.store(len)` | relaxed | published by step 3 |
| 3 | `write_cursor.store(record_end)` | release | consumer's acquire load of `write_cursor` in `peek()` |

The payload was written through the reserved view before `commit`; the header stores are typed
atomic only to avoid a data race in the C++ sense. They can be relaxed because the release store
of the cursor, which follows them in program order, is what the consumer synchronizes with.

**`peek()`** (consumer): idempotent until `release`.

| step | action | memory order | pairs with |
|---|---|---|---|
| 1 | `r = read_cursor.load` | relaxed (own variable) | — |
| 2 | if `cached_write == r`: `cached_write = write_cursor.load` | acquire | `commit()` step 3: header and payload become visible |
| 3 | still equal: return empty view | — | — |
| 4 | read header `length` | relaxed (ordered by step 2) | — |
| 5 | skip marker: advance `r` to the buffer start, repeat from 2 (at most once) | — | — |
| 6 | sanity checks (cursor is a multiple of 8, length within limits, record inside the published bytes and before the buffer end), return view | — | — |

**`release()`** (consumer): `read_cursor.store(record_end)` with release, pairing with the acquire in
`reserve` step 2. It also covers any skip bytes that preceded the record.

The sanity checks in `peek` mean a consumer that attaches to memory corrupted by the other party gets
an empty view instead of a view outside the ring. `reserve` likewise refuses a write cursor that is not
a multiple of 8: a record header at a misaligned address would be an undefined atomic access. (This
case was found by the queue fuzzer.)

### 3.3 Progress and crash semantics

Wait-free: every function does a bounded number of steps and has no retry loop.

| scenario | outcome |
|---|---|
| Producer stops between `reserve` and `commit` | The consumer never sees the record: no cursor moved and no header was written. |
| Producer stops during `commit` | Either the release store of the cursor happened (the whole record is visible) or it did not (nothing is). The cursor is one aligned 8-byte store. |
| Consumer stops between `peek` and `release` | The record is still in the ring and is delivered again to the next consumer. |
| Consumer stops after `release` | The record is consumed; whatever the application had not finished doing with it is lost. |

## 4. MPMC bounded queue — `mpmc_ring<T, N>`

### 4.1 Memory layout

```
 cache line 0         cache line 1         then
+----------------+   +----------------+   +---------------------------------------------+
| atomic         |   | atomic         |   | cell[N]: { atomic<uint64_t> sequence;       |
| enqueue_pos    |   | dequeue_pos    |   |           byte data[sizeof(T)] }            |
+----------------+   +----------------+   +---------------------------------------------+
```

Cell `i` starts with `sequence = i`. A cell for position `pos` is **free for a producer** when
`sequence == pos`, **full for a consumer** when `sequence == pos + 1`, and goes back to free for the
next lap when the consumer stores `pos + N`.

### 4.2 Operations

**`try_push`**

| step | action | memory order | pairs with |
|---|---|---|---|
| 1 | `pos = enqueue_pos.load` | relaxed | — (a ticket, not a synchronization point) |
| 2 | `seq = cell[pos & mask].sequence.load` | acquire | consumer's step 5 below from the previous lap: the old element has been read |
| 3 | `seq - pos == 0`: try `enqueue_pos.compare_exchange_weak(pos, pos + 1)` | relaxed | — (claims the ticket; the hand-over is step 5) |
| 3' | `seq - pos < 0`: return `false` (full). `> 0`: reload `pos`, retry from 2 | — | — |
| 4 | copy the element into the cell | plain | published by step 5 |
| 5 | `sequence.store(pos + 1)` | release | consumer's step 2 |

**`try_pop`**

| step | action | memory order | pairs with |
|---|---|---|---|
| 1 | `pos = dequeue_pos.load` | relaxed | — |
| 2 | `seq = cell[pos & mask].sequence.load` | acquire | producer's step 5: the element is visible |
| 3 | `seq - (pos + 1) == 0`: try `dequeue_pos.compare_exchange_weak(pos, pos + 1)` | relaxed | — |
| 3' | `< 0`: return `false` (empty). `> 0`: reload `pos`, retry from 2 | — | — |
| 4 | copy the element out | plain | — |
| 5 | `sequence.store(pos + N)` | release | producer's step 2 on the next lap |

`seq - pos` is computed in unsigned arithmetic and read as signed, which stays correct across
counter wrap-around.

### 4.3 Progress and failure semantics

Lock-free, not wait-free. A failed compare-and-swap means another thread claimed that position and
made progress, but one call can retry any number of times.

A producer claims a ticket (step 3) before it publishes the element (step 5). If it is suspended or
killed in between, that cell never reaches `pos + 1`. Consumers that arrive at that position see an
unpublished cell and report the ring as empty, even if cells behind it are full; producers that
wrap around to it report the ring as full. This is inherent in the design and is documented on the
class; the library does not detect or repair it. Where producer crashes must be survivable, use one
`spsc_ring` per producer.

## 5. Blocking layer — `event` and `blocking_spsc_ring`

### 5.1 The problem

A consumer that finds the ring empty wants to sleep until the producer pushes. The producer wants
to avoid a system call per push. Two things must hold: a sleeping consumer is always woken, and a
push with nobody asleep costs a fence and a load.

### 5.2 Protocol

`event` has two 32-bit words in one cache line: `seq` (incremented on every real wake-up) and
`waiters` (number of threads that registered to sleep).

**`wait(ready, timeout)`**

| step | action | memory order |
|---|---|---|
| 0 | if `ready()`, return | — |
| 1 | `waiters += 1` | relaxed |
| 2 | `seen = seq.load` | acquire (pairs with the increment in `notify`) |
| 3 | fence | seq_cst |
| 4 | if `ready()`, skip to 6 | — |
| 5 | `futex_wait(&seq, seen, remaining)` — sleeps only if `seq == seen` | — |
| 6 | `waiters -= 1`; if `ready()` return true; if deadline passed return false; else repeat from 1 | relaxed |

**`notify()`** (after the caller made the condition true, for example by a release store of `head`)

| step | action | memory order |
|---|---|---|
| 1 | fence | seq_cst |
| 2 | if `waiters.load == 0`, return | relaxed |
| 3 | `seq += 1` | release |
| 4 | `futex_wake(&seq)` | — |

### 5.3 Why no wake-up is lost

Suppose the producer makes the condition true (call it `P`) and the consumer evaluates `ready()` in
step 4 (call it `C`).

* The producer's fence (notify step 1) comes after `P` and before its read of `waiters`. The
  consumer's fence (wait step 3) comes after its increment of `waiters` and before `C`. Two
  sequentially consistent fences are totally ordered.
* If the consumer's fence is first, the increment of `waiters` is visible to the producer's later
  read, so the producer takes steps 3 and 4 and wakes the consumer.
* If the producer's fence is first, `P` is visible to `C`, so `ready()` is true and the consumer
  never sleeps.
* If the producer does reach step 3 but after the consumer read `seen` (wait step 2), `futex_wait`
  compares `seq` with `seen` inside the kernel, atomically with going to sleep, finds them
  different, and returns immediately. If the producer's increment came before step 2, the
  acquire load pairs with it, so `P` is visible to `C`.

This is the only place in the library that needs more than release/acquire: it is a store-then-load
pattern on two different variables.

### 5.4 Other properties

* **Spurious wake-ups** (`futex_wait` returning for any reason, signals included) are handled by
  re-checking `ready()` in a loop.
* **Timeouts** are enforced against `CLOCK_MONOTONIC`; a timeout of zero only evaluates `ready()`.
* **Shared memory:** `FUTEX_WAIT`/`FUTEX_WAKE` without the `_PRIVATE` flag are used, so the words
  can live in a mapped segment shared by several processes.
* **Killed waiter:** `waiters` stays raised. The effect is that `notify` keeps making a harmless
  `futex_wake` call. No data is affected.
* `blocking_spsc_ring` pairs one `spsc_ring` with two events: `not_empty` (producer notifies after a
  push, consumer waits) and `not_full` (consumer notifies after a pop, producer waits).

## 6. Broadcast ring — `spmc_ring<T, N>`

One writer publishes; any number of readers each see every element, as long as they keep up. The
writer never waits for readers. Readers that fall behind are told so and skip forward.

### 6.1 Memory layout

```
 cache line 0         then
+----------------+   +----------------------------------------------------------+
| atomic head    |   | cell[N]: { atomic<uint64_t> stamp;                        |
| (writer-owned) |   |           atomic<uint64_t> words[ceil(sizeof(T) / 8)] }   |
+----------------+   +----------------------------------------------------------+
```

A reader's position is not in shared memory: it is a small `spmc_reader` object owned by that
reader (next position to read, count of dropped elements). Readers therefore need no registration
and cost the writer nothing. Position `p` lives in cell `p & (N - 1)`. The stamp of a cell is
`2p + 1` while the writer is rewriting it for position `p`, and `2p + 2` once position `p` is
complete; a stamp of 0 means never written.

The element is stored as 64-bit atomic words (relaxed), not as plain bytes. A reader can copy a
cell while the writer overwrites it; with plain bytes that would be a data race in the C++ sense
even though the stamp check discards the torn copy. With atomic words every shared access is
atomic and sanitizers have nothing to report.

### 6.2 Operations

**`publish(v)`** (writer)

| step | action | memory order | pairs with |
|---|---|---|---|
| 1 | `pos = head.load` | relaxed (own variable) | — |
| 2 | `cell.stamp.store(2 * pos + 1)` | relaxed | — |
| 3 | fence | release | keeps step 2 ahead of the data stores for any reader that sees them (reader step 4) |
| 4 | store the element words | relaxed | read by the reader's step 3 |
| 5 | `cell.stamp.store(2 * pos + 2)` | release | reader's step 2 |
| 6 | `head.store(pos + 1)` | release | `published()`, `join_*()` |

**`try_read(reader, out)`** (any reader), `want = reader.next`

| step | action | memory order | pairs with |
|---|---|---|---|
| 1 | `done = 2 * want + 2` | — | — |
| 2 | `before = cell[want & mask].stamp.load` | acquire | writer's step 5: the data of position `want` is visible |
| 3 | `before < done`: return `empty`. `before == done`: copy the element words | relaxed | — |
| 4 | fence, then `after = stamp.load` | acquire fence, relaxed load | writer's step 3: if the writer started rewriting this cell during the copy, `after != before` |
| 5 | `after == before`: advance `next`, return `ok` | — | — |
| 6 | otherwise (`before > done`, or `after != before`): the writer lapped this reader. Set `next = max(head - N/2, want + 1)`, add the skipped count to `dropped`, return `overrun` | acquire load of `head` | — |

After an overrun the reader resumes half a ring behind the writer, so it does not immediately
collide with the next overwrite.

### 6.3 Progress and failure semantics

`publish` and `try_read` are wait-free. A reader never blocks the writer and a stopped reader costs
nothing. If the writer stops, readers see `empty`. A new writer that attaches continues from
`head`. A reader never receives mixed data from two different elements: either the stamp check
passes, or the call reports an overrun.

## 7. How the memory-ordering reasoning is checked

* **ThreadSanitizer** runs every unit, stress and crash test. It does not depend on the hardware's
  ordering: it tracks happens-before edges as defined by the C++ memory model, so a queue whose
  release/acquire pairing is too weak shows up as a data race on the plain payload (the element
  copies are deliberately non-atomic, see §0). The seqlock payload in §6 is atomic word by word, so
  there the check is that nothing is reported at all.
* **Stress tests** push millions of messages with redundant fields through every queue between
  threads and between processes and check loss, duplication, reordering and torn copies.
* **Crash tests** `SIGKILL` a peer while it works and check the survivor and a replacement peer.
* **Not done: model checking.** The requirements allow checking the algorithms with a C11
  memory-model checker such as GenMC or CDSChecker. Neither is installed on the development machine
  (no package, and building GenMC needs a matching LLVM), so it was not done. The honest consequence is
  that behaviour on weakly ordered hardware rests on the reasoning in §§2-6, on ThreadSanitizer, and on
  the AArch64 CI job, which runs the tests under QEMU user-mode emulation. QEMU does not reorder
  memory accesses the way a real Arm core can, so that job shows that the code builds and runs there,
  not that its ordering is sufficient.
* **Fuzzing** covers the header validation and queues constructed over arbitrary bytes. The MPMC
  ring is excluded: its protocol trusts all participants, and a participant that writes
  inconsistent sequence numbers can make the others retry forever (a hang, not an out-of-bounds
  access).

## 8. Decisions

The reasoning behind the choices above, with the alternatives that were rejected, is in
[DECISIONS.md](DECISIONS.md). Three points that shaped the segment format:
`open` waits for a half-initialized segment (default: forever, optional timeout); the message ring
capacity is a power of two; and liveness detection (heartbeat or epoch counter) is not part of
the segment header.
