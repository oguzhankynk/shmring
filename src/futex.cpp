#include "shmring/futex.hpp"

#include <cerrno>
#include <ctime>
#include <linux/futex.h>
#include <sys/syscall.h>

#include <unistd.h>

namespace shmring {

static_assert(sizeof(std::atomic<std::uint32_t>) == sizeof(std::uint32_t));

constexpr long nanos_per_second = 1000000000L;

futex_result futex_wait(const std::atomic<std::uint32_t>* word,
                        std::uint32_t expected,
                        std::optional<std::chrono::nanoseconds> timeout) noexcept {
  timespec ts{};
  const timespec* ts_ptr = nullptr;
  if (timeout) {
    const auto ns = timeout->count() < 0 ? std::chrono::nanoseconds::zero() : *timeout;
    ts.tv_sec = ns.count() / nanos_per_second;
    ts.tv_nsec = ns.count() % nanos_per_second;
    ts_ptr = &ts;
  }
  // FUTEX_WAIT without FUTEX_PRIVATE_FLAG: the word may be shared between processes. The
  // timeout is relative and measured on CLOCK_MONOTONIC.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): futex(2) has no libc wrapper, only syscall()
  const long rc = ::syscall(SYS_futex, word, FUTEX_WAIT, expected, ts_ptr, nullptr, 0);
  if (rc == 0) {
    return futex_result::woken;
  }
  switch (errno) {
    case EAGAIN:
      return futex_result::value_mismatch;
    case ETIMEDOUT:
      return futex_result::timed_out;
    case EINTR:
      return futex_result::interrupted;
    default:
      return futex_result::failed;
  }
}

int futex_wake(const std::atomic<std::uint32_t>* word, int count) noexcept {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): futex(2) has no libc wrapper, only syscall()
  return static_cast<int>(::syscall(SYS_futex, word, FUTEX_WAKE, count, nullptr, nullptr, 0));
}

}  // namespace shmring
