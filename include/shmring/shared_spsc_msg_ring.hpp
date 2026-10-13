#ifndef SHMRING_SHARED_SPSC_MSG_RING_HPP
#define SHMRING_SHARED_SPSC_MSG_RING_HPP

#include "shmring/segment.hpp"
#include "shmring/spsc_msg_ring.hpp"

#include <chrono>
#include <cstddef>
#include <optional>
#include <string>
#include <utility>

namespace shmring {

/// @brief An `spsc_msg_ring` living in a shared-memory segment.
///
/// One process calls `create`, others call `open`. Afterwards one process uses the producer
/// functions of `ring()` and one uses the consumer functions.
/// @par Thread safety
/// Same as `spsc_msg_ring`.
class shared_spsc_msg_ring {
public:
  /// @brief Creates the segment, builds an empty ring in it and makes it available to `open`.
  /// @param name System-wide segment name, starting with '/'.
  /// @param capacity Ring capacity in bytes; a power of two of at least
  ///        `spsc_msg_ring::min_capacity`.
  /// @param mode Permission bits of the segment, subject to the process umask.
  /// @return The ring handle, or the error from `segment::create`.
  static result<shared_spsc_msg_ring> create(const std::string& name,
                                             std::size_t capacity,
                                             mode_t mode = default_segment_mode) {
    result<segment> seg =
        segment::create(name, segment_expectation{queue_kind::spsc_msg_ring, 1, 1, capacity}, mode);
    if (!seg) {
      return seg.error();
    }
    segment mapped = std::move(seg).value();
    const spsc_msg_ring built = spsc_msg_ring::init(mapped.payload(), capacity);
    mapped.publish();
    return shared_spsc_msg_ring(std::move(mapped), built);
  }

  /// @brief Attaches to a ring created by another process.
  /// @param name System-wide segment name.
  /// @param timeout How long to wait for the creator to finish; see `segment::open`.
  /// @return The ring handle, or the error from `segment::open`.
  static result<shared_spsc_msg_ring> open(
      const std::string& name, std::optional<std::chrono::nanoseconds> timeout = std::nullopt) {
    result<segment> seg =
        segment::open(name, segment_expectation{queue_kind::spsc_msg_ring, 1, 1, 0}, timeout);
    if (!seg) {
      return seg.error();
    }
    segment mapped = std::move(seg).value();
    const spsc_msg_ring attached =
        spsc_msg_ring::attach(mapped.payload(), static_cast<std::size_t>(mapped.info().capacity));
    return shared_spsc_msg_ring(std::move(mapped), attached);
  }

  /// @brief Removes the segment name from the system; see `segment::unlink`.
  /// @param name System-wide segment name.
  /// @return Success or the error from `segment::unlink`.
  static result<unit> unlink(const std::string& name) {
    return segment::unlink(name);
  }

  /// @brief Returns this process's handle onto the ring.
  /// @return A reference valid as long as this object is alive.
  spsc_msg_ring& ring() noexcept {
    return ring_;
  }

private:
  shared_spsc_msg_ring(segment seg, const spsc_msg_ring& ring) noexcept
      : segment_(std::move(seg)), ring_(ring) {}

  segment segment_;
  spsc_msg_ring ring_;
};

}  // namespace shmring

#endif  // SHMRING_SHARED_SPSC_MSG_RING_HPP
