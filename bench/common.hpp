// Helpers shared by the benchmark tools: timing, CPU pinning and a fixed-size histogram.
#ifndef SHMRING_BENCH_COMMON_HPP
#define SHMRING_BENCH_COMMON_HPP

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <sys/wait.h>

#include <sched.h>
#include <unistd.h>

namespace bench {

inline std::uint64_t now_ns() noexcept {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count());
}

inline void cpu_relax() noexcept {
#if defined(__x86_64__)
  __builtin_ia32_pause();
#elif defined(__aarch64__)
  asm volatile("yield" ::: "memory");
#endif
}

// Pins the calling thread to one CPU. A negative cpu leaves the affinity unchanged.
inline bool pin_to_cpu(int cpu) noexcept {
  if (cpu < 0) {
    return true;
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(static_cast<std::size_t>(cpu), &set);
  return sched_setaffinity(0, sizeof(set), &set) == 0;
}

inline int env_int(const char* name, int fallback) noexcept {
  const char* v = std::getenv(name);
  return v != nullptr ? std::atoi(v) : fallback;
}

// Records values (nanoseconds) into fixed buckets: exact below 64, then 64 buckets per power of
// two (about 1.6% relative error). Recording never allocates.
class histogram {
public:
  void record(std::uint64_t value) noexcept {
    ++buckets_[index_of(value)];
    ++count_;
    if (value > max_) {
      max_ = value;
    }
  }

  std::uint64_t count() const noexcept {
    return count_;
  }
  std::uint64_t max() const noexcept {
    return max_;
  }

  // Smallest bucket lower bound such that at least `fraction` of the samples are <= its upper edge.
  std::uint64_t percentile(double fraction) const noexcept {
    const auto target = static_cast<std::uint64_t>(fraction * static_cast<double>(count_));
    std::uint64_t seen = 0;
    for (std::size_t i = 0; i < buckets_.size(); ++i) {
      seen += buckets_[i];
      if (seen > 0 && seen >= (target == 0 ? 1 : target)) {
        return value_of(i);
      }
    }
    return max_;
  }

private:
  static constexpr int sub_bits = 6;
  static constexpr std::size_t sub_count = std::size_t{1} << sub_bits;

  static std::size_t index_of(std::uint64_t v) noexcept {
    if (v < sub_count) {
      return v;
    }
    const int msb = 63 - __builtin_clzll(v);
    const int shift = msb - sub_bits;
    const std::uint64_t mantissa = v >> shift;
    return static_cast<std::size_t>(shift + 1) * sub_count + (mantissa - sub_count);
  }

  static std::uint64_t value_of(std::size_t index) noexcept {
    if (index < sub_count) {
      return index;
    }
    const std::size_t shift = index / sub_count - 1;
    const std::uint64_t mantissa = sub_count + index % sub_count;
    return mantissa << shift;
  }

  std::array<std::uint64_t, (64 - sub_bits + 1) * sub_count> buckets_{};
  std::uint64_t count_ = 0;
  std::uint64_t max_ = 0;
};

// Runs `child` in a forked process and returns its pid. The child exits with status 0 afterwards.
template <typename Fn>
pid_t spawn(Fn child) {
  const pid_t pid = ::fork();
  if (pid == 0) {
    child();
    ::_exit(0);
  }
  return pid;
}

inline int wait_for(pid_t pid) {
  int status = 0;
  ::waitpid(pid, &status, 0);
  return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

inline std::string unique_name(const char* tag) {
  static int counter = 0;
  return std::string("/shmring_bench_") + tag + "_" + std::to_string(::getpid()) + "_" +
         std::to_string(counter++);
}

}  // namespace bench

#endif  // SHMRING_BENCH_COMMON_HPP
