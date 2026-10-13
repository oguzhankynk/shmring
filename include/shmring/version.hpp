#ifndef SHMRING_VERSION_HPP
#define SHMRING_VERSION_HPP

/// @brief Lock-free inter-process queues over POSIX shared memory.
namespace shmring {

/// @brief Returns the library version as "MAJOR.MINOR.PATCH".
/// @return A pointer to a static, null-terminated string. Never null.
/// @par Thread safety
/// Safe to call from any thread; the returned string is immutable.
const char* version() noexcept;

}  // namespace shmring

#endif  // SHMRING_VERSION_HPP
