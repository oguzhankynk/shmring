#ifndef SHMRING_FUTEX_HPP
#define SHMRING_FUTEX_HPP

#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>

namespace shmring {

/// @brief Outcome of `futex_wait`.
enum class futex_result {
  woken,           ///< Woken by `futex_wake` (or spuriously); re-check the condition.
  value_mismatch,  ///< The word did not hold the expected value, so the call did not sleep.
  timed_out,       ///< The timeout expired.
  interrupted,     ///< A signal arrived; re-check the condition.
  failed           ///< The system call failed for another reason, for example a bad address.
};

/// @brief Sleeps until the word is woken, if it still holds `expected`.
///
/// Uses the shared (non-private) futex operation, so the word may live in memory mapped by
/// several processes. The check of the value and the decision to sleep are atomic with respect to
/// `futex_wake`, which is what makes lost wake-ups impossible when the waker changes the word
/// before waking.
/// @param word A 32-bit word in memory the caller can access.
/// @param expected The value the word must still hold for the call to sleep.
/// @param timeout Maximum time to sleep; an empty value sleeps without limit.
/// @return Why the call returned.
/// @note Spurious returns are allowed; callers must loop on their own condition.
futex_result futex_wait(const std::atomic<std::uint32_t>* word,
                        std::uint32_t expected,
                        std::optional<std::chrono::nanoseconds> timeout = std::nullopt) noexcept;

/// @brief Wakes threads or processes sleeping in `futex_wait` on the same word.
/// @param word A 32-bit word in memory the caller can access.
/// @param count Maximum number of sleepers to wake.
/// @return The number of sleepers woken, or -1 on failure.
int futex_wake(const std::atomic<std::uint32_t>* word, int count) noexcept;

}  // namespace shmring

#endif  // SHMRING_FUTEX_HPP
