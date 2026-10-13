#ifndef SHMRING_CONFIG_HPP
#define SHMRING_CONFIG_HPP

#include <cstddef>

/// @brief Cache line size, in bytes, used to separate data written by different threads.
/// @note Defaults to 64. Set the `SHMRING_CACHE_LINE_SIZE` CMake option (for example to 128 for
/// some AArch64 cores) to change it. Every process that shares a queue must be built with the
/// same value, because it determines the memory layout of the queue.
#ifndef SHMRING_CACHE_LINE
#define SHMRING_CACHE_LINE 64
#endif

namespace shmring {

/// @brief Cache line size in bytes, taken from `SHMRING_CACHE_LINE`.
inline constexpr std::size_t cache_line = SHMRING_CACHE_LINE;

/// @brief Smallest accepted cache line size, in bytes.
inline constexpr std::size_t min_cache_line = 16;

/// @brief Smallest capacity of an `spsc_msg_ring`, in bytes.
inline constexpr std::size_t min_message_ring_capacity = 64;

static_assert(cache_line >= min_cache_line && (cache_line & (cache_line - 1)) == 0,
              "SHMRING_CACHE_LINE must be a power of two of at least 16");

}  // namespace shmring

#endif  // SHMRING_CONFIG_HPP
