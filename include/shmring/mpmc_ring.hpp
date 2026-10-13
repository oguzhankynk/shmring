#ifndef SHMRING_MPMC_RING_HPP
#define SHMRING_MPMC_RING_HPP

#include "shmring/config.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace shmring {

/// @brief Bounded multi-producer multi-consumer ring of fixed-size elements.
///
/// Any number of threads or processes may push and pop concurrently. Each slot carries its own
/// sequence number, so producers and consumers only contend on a slot when they reach the same
/// one (the design is due to Dmitry Vyukov). Elements are copied byte for byte and the object
/// contains no pointers, so it can be placed in shared memory.
///
/// @par Progress
/// Lock-free, not wait-free: a failed compare-and-swap means another thread made progress, but a
/// single call can retry.
/// @par Known limitation
/// A producer claims a slot before it publishes the element. If that producer is suspended or
/// killed between the two steps, the slot never becomes available: consumers that reach it see
/// the ring as empty (`try_pop` returns `false`) although later slots may hold elements, and
/// producers that wrap around to it see the ring as full. Nothing in the library detects or
/// repairs this; use `spsc_ring` per producer where producer crashes must be survivable.
/// @tparam T Element type. Must be trivially copyable.
/// @tparam N Capacity in elements. Must be a power of two and at least 2: with one slot a full
///           slot and the next lap's free slot would carry the same sequence number.
/// @par Thread safety
/// All member functions may be called concurrently from any number of threads or processes.
template <typename T, std::size_t N>
class mpmc_ring {
  static_assert(std::is_trivially_copyable_v<T>, "mpmc_ring elements must be trivially copyable");
  static_assert(N >= 2 && (N & (N - 1)) == 0,
                "mpmc_ring capacity must be a power of two, at least 2");
  static_assert(alignof(T) <= cache_line, "element alignment must not exceed the cache line size");
  static_assert(
      sizeof(std::size_t) == sizeof(std::uint64_t),
      "counters are 64-bit and are used directly as sizes; a 64-bit platform is required");
  static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                "64-bit atomics must be lock-free to be usable across processes");

public:
  /// @brief Creates an empty ring.
  /// @param initial_index Value the position counters start from. The default of 0 is right for
  ///        normal use; a value close to `UINT64_MAX` lets tests exercise counter wrap-around.
  explicit mpmc_ring(std::uint64_t initial_index = 0) noexcept
      : enqueue_pos_(initial_index), dequeue_pos_(initial_index) {
    for (std::uint64_t i = 0; i < N; ++i) {
      cell_at(initial_index + i).sequence.store(initial_index + i, std::memory_order_relaxed);
    }
  }

  mpmc_ring(const mpmc_ring&) = delete;
  mpmc_ring& operator=(const mpmc_ring&) = delete;
  mpmc_ring(mpmc_ring&&) = delete;
  mpmc_ring& operator=(mpmc_ring&&) = delete;
  ~mpmc_ring() = default;

  /// @brief Returns the number of elements the ring can hold.
  /// @return `N`.
  static constexpr std::size_t capacity() noexcept {
    return N;
  }

  /// @brief Returns the number of elements currently in the ring, without synchronization.
  /// @return A value in `[0, capacity()]`. It may already be out of date, and it counts slots
  ///         that producers have claimed but not yet published.
  [[nodiscard]] std::size_t size_approx() const noexcept {
    // Relaxed: advisory only. Reading dequeue_pos_ first keeps enqueue >= dequeue.
    const std::uint64_t d = dequeue_pos_.load(std::memory_order_relaxed);
    const std::uint64_t e = enqueue_pos_.load(std::memory_order_relaxed);
    return std::min<std::uint64_t>(e - d, N);
  }

  /// @brief Appends one element.
  /// @param value The element to copy into the ring.
  /// @return `true` if the element was stored, `false` if the ring is full.
  bool try_push(const T& value) noexcept {
    cell* c = nullptr;
    // Relaxed: the position is only a ticket; ordering with other threads comes from the
    // slot's sequence number below.
    std::uint64_t pos = enqueue_pos_.load(std::memory_order_relaxed);
    for (;;) {
      c = &cell_at(pos);
      // Acquire: pairs with the release store of `pos + N` in try_pop. It guarantees the
      // consumer of the previous lap has finished reading this slot before we overwrite it.
      const std::uint64_t seq = c->sequence.load(std::memory_order_acquire);
      const auto diff = static_cast<std::int64_t>(seq - pos);
      if (diff == 0) {
        // Relaxed: claiming the ticket needs atomicity, not ordering; the slot is handed over
        // by the release store to its sequence below.
        if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          break;
        }
      } else if (diff < 0) {
        return false;  // The slot still holds an element from the previous lap: full.
      } else {
        pos = enqueue_pos_.load(std::memory_order_relaxed);  // Another producer got here first.
      }
    }
    std::memcpy(c->data.data(), &value, sizeof(T));
    // Release: pairs with the acquire load of the sequence in try_pop. It publishes the element
    // written above.
    c->sequence.store(pos + 1, std::memory_order_release);
    return true;
  }

  /// @brief Removes one element.
  /// @param out Receives the element when the call succeeds; unchanged otherwise.
  /// @return `true` if an element was removed, `false` if the ring is empty (or the next slot is
  ///         claimed by a producer that has not published yet; see the class notes).
  bool try_pop(T& out) noexcept {
    cell* c = nullptr;
    std::uint64_t pos = dequeue_pos_.load(std::memory_order_relaxed);  // A ticket, as above.
    for (;;) {
      c = &cell_at(pos);
      // Acquire: pairs with the release store of `pos + 1` in try_push. It makes the element
      // written by the producer visible before we read it.
      const std::uint64_t seq = c->sequence.load(std::memory_order_acquire);
      const auto diff = static_cast<std::int64_t>(seq - (pos + 1));
      if (diff == 0) {
        if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          break;
        }
      } else if (diff < 0) {
        return false;  // No published element in this slot yet: empty.
      } else {
        pos = dequeue_pos_.load(std::memory_order_relaxed);  // Another consumer got here first.
      }
    }
    std::memcpy(&out, c->data.data(), sizeof(T));
    // Release: pairs with the acquire load of the sequence in try_push. It orders our read of
    // the element before the producer's reuse of the slot on the next lap.
    c->sequence.store(pos + N, std::memory_order_release);
    return true;
  }

private:
  static constexpr std::uint64_t mask = N - 1;

  static constexpr std::size_t slot_alignment = alignof(T) > alignof(std::uint64_t)
                                                    ? alignof(T)
                                                    : alignof(std::uint64_t);

  struct alignas(slot_alignment) cell {
    std::atomic<std::uint64_t> sequence;
    std::array<std::byte, sizeof(T)> data;
  };

  cell& cell_at(std::uint64_t position) noexcept {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index): masked, always below N
    return cells_[position & mask];
  }

  alignas(cache_line) std::atomic<std::uint64_t> enqueue_pos_;
  alignas(cache_line) std::atomic<std::uint64_t> dequeue_pos_;
  alignas(cache_line) std::array<cell, N> cells_;
};

}  // namespace shmring

#endif  // SHMRING_MPMC_RING_HPP
