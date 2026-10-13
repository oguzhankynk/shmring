#ifndef SHMRING_SPSC_RING_HPP
#define SHMRING_SPSC_RING_HPP

#include "shmring/config.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace shmring {

/// @brief Wait-free single-producer single-consumer ring of fixed-size elements.
///
/// Exactly one thread (or process) may call the producer functions (`try_push`, `try_push_n`)
/// and exactly one may call the consumer functions (`try_pop`, `try_pop_n`). The two sides never
/// block each other: a full ring makes a push fail, an empty ring makes a pop fail.
///
/// The object contains no pointers, so it can be placed in shared memory and used by several
/// processes at once. Elements are copied byte for byte.
///
/// @tparam T Element type. Must be trivially copyable and no more strictly aligned than a cache
///           line.
/// @tparam N Capacity in elements. Must be a power of two.
/// @par Thread safety
/// One producer and one consumer concurrently. `size_approx` and `capacity` may be called from
/// any thread.
template <typename T, std::size_t N>
class spsc_ring {
  static_assert(std::is_trivially_copyable_v<T>, "spsc_ring elements must be trivially copyable");
  static_assert(N > 0 && (N & (N - 1)) == 0, "spsc_ring capacity must be a power of two");
  static_assert(alignof(T) <= cache_line, "element alignment must not exceed the cache line size");
  static_assert(
      sizeof(std::size_t) == sizeof(std::uint64_t),
      "counters are 64-bit and are used directly as sizes; a 64-bit platform is required");
  static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                "64-bit atomics must be lock-free to be usable across processes");

public:
  /// @brief Creates an empty ring.
  /// @param initial_index Value both counters start from. The default of 0 is right for normal
  ///        use; a value close to `UINT64_MAX` lets tests exercise counter wrap-around.
  // The element storage is intentionally left uninitialized.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
  explicit spsc_ring(std::uint64_t initial_index = 0) noexcept
      : head_(initial_index),
        cached_tail_(initial_index),
        tail_(initial_index),
        cached_head_(initial_index) {}

  spsc_ring(const spsc_ring&) = delete;
  spsc_ring& operator=(const spsc_ring&) = delete;
  spsc_ring(spsc_ring&&) = delete;
  spsc_ring& operator=(spsc_ring&&) = delete;
  ~spsc_ring() = default;

  /// @brief Returns the number of elements the ring can hold.
  /// @return `N`.
  static constexpr std::size_t capacity() noexcept {
    return N;
  }

  /// @brief Returns the number of elements currently in the ring, without synchronization.
  /// @return A value in `[0, capacity()]`. It may already be out of date when it is returned.
  /// @note Meant for monitoring. Do not use it to decide whether a push or pop will succeed.
  [[nodiscard]] std::size_t size_approx() const noexcept {
    // Relaxed: the result is advisory, nothing is published or consumed through it. Reading the
    // consumer's counter first guarantees head >= tail for the values we observe.
    const std::uint64_t t = tail_.load(std::memory_order_relaxed);
    const std::uint64_t h = head_.load(std::memory_order_relaxed);
    return std::min<std::uint64_t>(h - t, N);
  }

  /// @brief Appends one element. Producer only.
  /// @param value The element to copy into the ring.
  /// @return `true` if the element was stored, `false` if the ring is full.
  /// @par Progress
  /// Wait-free.
  bool try_push(const T& value) noexcept {
    // Relaxed: only the producer writes head_, so this reads our own last store.
    const std::uint64_t h = head_.load(std::memory_order_relaxed);
    if (h - cached_tail_ >= N) {
      // Acquire: pairs with the release store to tail_ in try_pop/try_pop_n. It guarantees the
      // consumer has finished reading every slot below the tail value we see, so we may
      // overwrite them.
      cached_tail_ = tail_.load(std::memory_order_acquire);
      if (h - cached_tail_ >= N) {
        return false;
      }
    }
    std::memcpy(slot(h), &value, sizeof(T));
    // Release: pairs with the acquire load of head_ in try_pop/try_pop_n. It makes the element
    // written above visible to the consumer before the consumer can see the new head.
    head_.store(h + 1, std::memory_order_release);
    return true;
  }

  /// @brief Appends up to `count` elements with a single publication. Producer only.
  /// @param src First of `count` elements to copy.
  /// @param count Number of elements offered.
  /// @return The number of elements stored, from 0 to `count`. Fewer than `count` means the ring
  ///         became full; the stored elements are the first ones in `src`.
  /// @par Progress
  /// Wait-free; the work is proportional to the number of elements copied.
  std::size_t try_push_n(const T* src, std::size_t count) noexcept {
    const std::uint64_t h = head_.load(std::memory_order_relaxed);  // own store, see try_push
    std::uint64_t used = h - cached_tail_;
    if (used >= N || N - used < count) {
      // Acquire: pairs with the release store to tail_ in the pop functions, as in try_push.
      cached_tail_ = tail_.load(std::memory_order_acquire);
      used = h - cached_tail_;
      if (used >= N) {
        return 0;
      }
    }
    const std::size_t n = std::min<std::size_t>(count, N - used);
    if (n == 0) {
      return 0;
    }
    copy_in(h, src, n);
    // Release: publishes all n elements at once; pairs with the acquire load of head_ in the pop
    // functions.
    head_.store(h + n, std::memory_order_release);
    return n;
  }

  /// @brief Removes the oldest element. Consumer only.
  /// @param out Receives the element when the call succeeds; unchanged otherwise.
  /// @return `true` if an element was removed, `false` if the ring is empty.
  /// @par Progress
  /// Wait-free.
  bool try_pop(T& out) noexcept {
    // Relaxed: only the consumer writes tail_, so this reads our own last store.
    const std::uint64_t t = tail_.load(std::memory_order_relaxed);
    if (cached_head_ == t) {
      // Acquire: pairs with the release store to head_ in try_push/try_push_n. It makes the
      // element the producer wrote to the slot visible before we read it.
      cached_head_ = head_.load(std::memory_order_acquire);
      if (cached_head_ == t) {
        return false;
      }
    }
    std::memcpy(&out, slot(t), sizeof(T));
    // Release: pairs with the acquire load of tail_ in try_push/try_push_n. It orders our read
    // of the slot before the producer's later reuse of it.
    tail_.store(t + 1, std::memory_order_release);
    return true;
  }

  /// @brief Removes up to `count` elements with a single publication. Consumer only.
  /// @param dst Receives the elements, oldest first. Needs room for `count` elements.
  /// @param count Maximum number of elements to remove.
  /// @return The number of elements removed, from 0 to `count`.
  /// @par Progress
  /// Wait-free; the work is proportional to the number of elements copied.
  std::size_t try_pop_n(T* dst, std::size_t count) noexcept {
    const std::uint64_t t = tail_.load(std::memory_order_relaxed);  // own store, see try_pop
    std::uint64_t avail = cached_head_ - t;
    if (avail < count) {
      // Acquire: pairs with the release store to head_ in the push functions, as in try_pop.
      cached_head_ = head_.load(std::memory_order_acquire);
      avail = cached_head_ - t;
    }
    if (avail > N) {
      return 0;  // Only reachable if the shared memory was corrupted by another party.
    }
    const std::size_t n = std::min<std::size_t>(count, avail);
    if (n == 0) {
      return 0;
    }
    copy_out(t, dst, n);
    // Release: tells the producer all n slots are free; pairs with the acquire load of tail_ in
    // the push functions.
    tail_.store(t + n, std::memory_order_release);
    return n;
  }

private:
  static constexpr std::uint64_t mask = N - 1;

  // The element storage is raw bytes addressed by a computed slot index: pointer arithmetic,
  // array-to-pointer decay and a C array are inherent to a ring buffer over raw memory.
  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic,cppcoreguidelines-pro-bounds-array-to-pointer-decay,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
  std::byte* slot(std::uint64_t index) noexcept {
    return storage_ + ((index & mask) * sizeof(T));
  }

  void copy_in(std::uint64_t index, const T* src, std::size_t n) noexcept {
    const std::size_t first = std::min<std::size_t>(n, N - (index & mask));
    std::memcpy(slot(index), src, first * sizeof(T));
    if (n > first) {
      std::memcpy(storage_, src + first, (n - first) * sizeof(T));
    }
  }

  void copy_out(std::uint64_t index, T* dst, std::size_t n) noexcept {
    const std::size_t first = std::min<std::size_t>(n, N - (index & mask));
    std::memcpy(dst, slot(index), first * sizeof(T));
    if (n > first) {
      std::memcpy(dst + first, storage_, (n - first) * sizeof(T));
    }
  }
  // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic,cppcoreguidelines-pro-bounds-array-to-pointer-decay,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

  // Producer's cache line: written only by the producer.
  alignas(cache_line) std::atomic<std::uint64_t> head_;
  std::uint64_t cached_tail_;  // producer's last observed value of tail_

  // Consumer's cache line: written only by the consumer.
  alignas(cache_line) std::atomic<std::uint64_t> tail_;
  std::uint64_t cached_head_;  // consumer's last observed value of head_

  // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays): raw element storage
  alignas(cache_line) std::byte storage_[N * sizeof(T)];
};

}  // namespace shmring

#endif  // SHMRING_SPSC_RING_HPP
