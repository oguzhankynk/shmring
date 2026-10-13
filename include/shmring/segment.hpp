#ifndef SHMRING_SEGMENT_HPP
#define SHMRING_SEGMENT_HPP

#include "shmring/error.hpp"
#include "shmring/segment_header.hpp"

#include <chrono>
#include <cstddef>
#include <optional>
#include <string>
#include <sys/types.h>

namespace shmring {

/// @brief Default permission bits of a new segment: read and write for the owner only.
inline constexpr mode_t default_segment_mode = 0600;

/// @brief A POSIX shared-memory segment mapped into this process.
///
/// A segment starts with a `segment_header`, followed by the queue it carries. The creating
/// process builds the queue in `payload()` and then calls `publish()`; other processes attach with
/// `open`, which waits for publication and validates the header.
///
/// Destroying the object unmaps the memory but never removes the segment from the system. Removal
/// is explicit: call `unlink`.
/// @par Thread safety
/// Not thread-safe; use one object per thread or synchronize externally.
class segment {
public:
  /// @brief Creates a new segment and maps it. The segment is not ready until `publish()`.
  /// @param name System-wide name, starting with '/' (see `shm_open`).
  /// @param layout Queue kind, element type and capacity to record in the header. The capacity
  ///        must be a power of two.
  /// @param mode Permission bits of the new segment, subject to the process umask.
  /// @return The mapped segment, or `already_exists`, `invalid_argument`, `bad_capacity`,
  ///         `permission_denied` or `system_error`.
  static result<segment> create(const std::string& name,
                                const segment_expectation& layout,
                                mode_t mode = default_segment_mode);

  /// @brief Attaches to an existing segment and validates it.
  ///
  /// If the creator has not published the segment yet, waits for it: first by spinning briefly,
  /// then by sleeping with a backoff that grows from 50 microseconds to 10 milliseconds.
  /// @param name System-wide name of the segment.
  /// @param layout What the segment is expected to contain.
  /// @param timeout How long to wait for the creator. An empty value (the default) waits forever;
  ///        zero does not wait at all.
  /// @return The mapped segment, `not_found` if no such segment exists, `not_ready` (zero timeout)
  ///         or `timed_out` if it did not become ready in time, one of the validation errors if
  ///         the contents do not match `layout`, or a system error.
  static result<segment> open(const std::string& name,
                              const segment_expectation& layout,
                              std::optional<std::chrono::nanoseconds> timeout = std::nullopt);

  /// @brief Removes the name of a segment from the system.
  ///
  /// Processes that already mapped the segment keep working; the memory is freed after the last
  /// one unmaps it.
  /// @param name System-wide name of the segment.
  /// @return Success, `not_found` if the name does not exist, or a system error.
  static result<unit> unlink(const std::string& name);

  segment(const segment&) = delete;
  segment& operator=(const segment&) = delete;

  /// @brief Takes over the mapping of another object, which becomes empty.
  /// @param other The object to move from.
  segment(segment&& other) noexcept;

  /// @brief Releases the current mapping and takes over the mapping of another object.
  /// @param other The object to move from.
  /// @return `*this`.
  segment& operator=(segment&& other) noexcept;

  /// @brief Unmaps the segment.
  ~segment();

  /// @brief Returns the start of the mapping.
  /// @return The address of the header.
  void* data() noexcept {
    return base_;
  }

  /// @brief Returns the size of the mapping.
  /// @return Size in bytes, including the header.
  [[nodiscard]] std::size_t size() const noexcept {
    return size_;
  }

  /// @brief Returns the segment header.
  /// @return A reference to the header inside the mapping.
  segment_header& header() noexcept {
    return *static_cast<segment_header*>(base_);
  }

  /// @brief Returns the start of the queue area, directly after the header.
  /// @return An address aligned to `cache_line`.
  void* payload() noexcept {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic): the queue follows the header
    return static_cast<std::byte*>(base_) + sizeof(segment_header);
  }

  /// @brief Returns the header values that were validated (or written, for a created segment).
  /// @return Kind, element layout and capacity. Unlike the live header, these cannot change.
  [[nodiscard]] const segment_info& info() const noexcept {
    return info_;
  }

  /// @brief Marks the segment ready so that `open` calls in other processes succeed.
  /// @note Call once, after the queue in `payload()` is fully constructed. Creator only.
  void publish() noexcept;

private:
  segment(void* base, std::size_t size, const segment_info& info) noexcept
      : base_(base), size_(size), info_(info) {}

  void* base_;
  std::size_t size_;
  segment_info info_;
};

}  // namespace shmring

#endif  // SHMRING_SEGMENT_HPP
