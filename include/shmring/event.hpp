#ifndef SHMRING_EVENT_HPP
#define SHMRING_EVENT_HPP

#include "shmring/config.hpp"
#include "shmring/futex.hpp"

#include <atomic>
#include <chrono>
#include <climits>
#include <cstdint>
#include <optional>

namespace shmring {

/// @brief Lets a thread or process sleep until another one reports that a condition may hold.
///
/// This is the blocking layer: it sits next to a queue, not inside its fast path. A notifier
/// makes no system call unless some thread is actually asleep, so notifying is cheap when
/// nobody waits. The object contains no pointers and works in shared memory.
///
/// Typical use: the producer publishes data, then calls `notify`; the consumer calls `wait` with
/// a function that checks whether data is available.
/// @par Thread safety
/// Any number of threads or processes may call `notify` and `wait` concurrently.
class event {
public:
  /// @brief Creates an event with no waiters.
  event() noexcept = default;

  event(const event&) = delete;
  event& operator=(const event&) = delete;
  event(event&&) = delete;
  event& operator=(event&&) = delete;
  ~event() = default;

  /// @brief Wakes waiters, if there are any. Call after the condition has been made true.
  /// @param count Maximum number of waiters to wake; defaults to all of them.
  void notify(int count = INT_MAX) noexcept {
    // Sequentially consistent fence: pairs with the fence in wait(). The caller's change of the
    // condition and our read of waiters_ cannot both miss the waiter's registration and the
    // waiter's re-check of the condition, so a sleeping waiter is always woken. This is the one
    // place the library needs more than release/acquire: it is a store-then-load pattern on two
    // different variables.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (waiters_.load(std::memory_order_relaxed) == 0) {
      return;
    }
    // Release: a waiter that sees the new value in futex_wait's comparison also sees the change
    // the caller made before notify().
    seq_.fetch_add(1, std::memory_order_release);
    futex_wake(&seq_, count);
  }

  /// @brief Blocks until `ready()` returns true or the timeout expires.
  /// @tparam Ready Callable returning something convertible to `bool`.
  /// @param ready Checks the condition. Called on the waiting thread, possibly many times, and
  ///        must not block.
  /// @param timeout Maximum time to wait; an empty value waits without limit, zero only checks.
  /// @return `true` if `ready()` returned true, `false` if the timeout expired first.
  template <typename Ready>
  bool wait(Ready ready, std::optional<std::chrono::nanoseconds> timeout = std::nullopt) {
    using clock = std::chrono::steady_clock;
    if (ready()) {
      return true;
    }
    if (timeout && *timeout <= std::chrono::nanoseconds::zero()) {
      return false;
    }
    const clock::time_point deadline = timeout ? clock::now() + *timeout : clock::time_point::max();
    for (;;) {
      waiters_.fetch_add(1, std::memory_order_relaxed);
      // Acquire: pairs with the release increment in notify(). If we see a bumped value, the
      // condition change that preceded the notification is visible to ready() below.
      const std::uint32_t seen = seq_.load(std::memory_order_acquire);
      // Sequentially consistent fence: see notify(). Registration above is ordered before the
      // condition check below.
      std::atomic_thread_fence(std::memory_order_seq_cst);
      if (ready()) {
        waiters_.fetch_sub(1, std::memory_order_relaxed);
        return true;
      }
      std::optional<std::chrono::nanoseconds> remaining;
      if (timeout) {
        remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - clock::now());
      }
      // Sleeps only if seq_ still equals `seen`, so a notification between the load and the
      // sleep makes this return immediately.
      futex_wait(&seq_, seen, remaining);
      waiters_.fetch_sub(1, std::memory_order_relaxed);
      // Otherwise the next iteration registers again and re-checks the condition first.
      if (timeout && clock::now() >= deadline) {
        return ready();
      }
    }
  }

private:
  // A waiter that is killed while asleep leaves waiters_ raised; the only effect is that
  // notify() keeps making a (harmless) wake call.
  alignas(cache_line) std::atomic<std::uint32_t> seq_{0};
  std::atomic<std::uint32_t> waiters_{0};
};

static_assert(sizeof(event) == cache_line);

}  // namespace shmring

#endif  // SHMRING_EVENT_HPP
