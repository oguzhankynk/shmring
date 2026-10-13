#ifndef SHMRING_BLOCKING_SPSC_RING_HPP
#define SHMRING_BLOCKING_SPSC_RING_HPP

#include "shmring/event.hpp"
#include "shmring/spsc_ring.hpp"

#include <chrono>
#include <cstddef>
#include <optional>

namespace shmring {

/// @brief An `spsc_ring` with optional blocking: a full ring can make the producer sleep and an
///        empty ring can make the consumer sleep.
///
/// The non-blocking `try_push` and `try_pop` keep the ring's own behaviour and only add a
/// notification, which costs a fence and a load unless someone is asleep. `push` and `pop` wait
/// with an optional timeout. The object contains no pointers and works in shared memory.
/// @tparam T Element type, trivially copyable.
/// @tparam N Capacity in elements, a power of two.
/// @par Thread safety
/// One producer and one consumer concurrently.
template <typename T, std::size_t N>
class blocking_spsc_ring {
public:
  /// @brief Creates an empty ring.
  blocking_spsc_ring() noexcept = default;

  /// @brief Appends one element without blocking, then wakes a sleeping consumer. Producer only.
  /// @param value The element to copy into the ring.
  /// @return `true` if stored, `false` if the ring is full.
  bool try_push(const T& value) noexcept {
    if (!ring_.try_push(value)) {
      return false;
    }
    not_empty_.notify();
    return true;
  }

  /// @brief Removes the oldest element without blocking, then wakes a sleeping producer.
  ///        Consumer only.
  /// @param out Receives the element on success.
  /// @return `true` if an element was removed, `false` if the ring is empty.
  bool try_pop(T& out) noexcept {
    if (!ring_.try_pop(out)) {
      return false;
    }
    not_full_.notify();
    return true;
  }

  /// @brief Appends one element, sleeping while the ring is full. Producer only.
  /// @param value The element to copy into the ring.
  /// @param timeout Maximum time to wait for space; empty waits without limit.
  /// @return `true` if stored, `false` if the timeout expired.
  bool push(const T& value, std::optional<std::chrono::nanoseconds> timeout = std::nullopt) {
    return blocking(
        timeout,
        not_full_,
        [&] { return try_push(value); },
        [this] { return ring_.size_approx() < N; });
  }

  /// @brief Removes the oldest element, sleeping while the ring is empty. Consumer only.
  /// @param out Receives the element on success.
  /// @param timeout Maximum time to wait for an element; empty waits without limit.
  /// @return `true` if an element was removed, `false` if the timeout expired.
  bool pop(T& out, std::optional<std::chrono::nanoseconds> timeout = std::nullopt) {
    return blocking(
        timeout,
        not_empty_,
        [&] { return try_pop(out); },
        [this] { return ring_.size_approx() > 0; });
  }

  /// @brief Returns the underlying ring, for batch operations and size queries.
  /// @return The ring. Pushing to it directly does not wake a sleeping consumer.
  spsc_ring<T, N>& ring() noexcept {
    return ring_;
  }

  /// @brief Returns the number of elements the ring can hold.
  /// @return `N`.
  static constexpr std::size_t capacity() noexcept {
    return N;
  }

private:
  template <typename Attempt, typename Ready>
  static bool blocking_impl(event& ev,
                            std::optional<std::chrono::nanoseconds> timeout,
                            Attempt attempt,
                            Ready ready) {
    using clock = std::chrono::steady_clock;
    const clock::time_point deadline = timeout ? clock::now() + *timeout : clock::time_point::max();
    for (;;) {
      if (attempt()) {
        return true;
      }
      std::optional<std::chrono::nanoseconds> remaining;
      if (timeout) {
        remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - clock::now());
      }
      if (!ev.wait(ready, remaining)) {
        return false;
      }
    }
  }

  template <typename Attempt, typename Ready>
  bool blocking(std::optional<std::chrono::nanoseconds> timeout,
                event& ev,
                Attempt attempt,
                Ready ready) {
    return blocking_impl(ev, timeout, attempt, ready);
  }

  spsc_ring<T, N> ring_;
  event not_empty_;
  event not_full_;
};

}  // namespace shmring

#endif  // SHMRING_BLOCKING_SPSC_RING_HPP
