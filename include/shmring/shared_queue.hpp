#ifndef SHMRING_SHARED_QUEUE_HPP
#define SHMRING_SHARED_QUEUE_HPP

#include "shmring/blocking_spsc_ring.hpp"
#include "shmring/mpmc_ring.hpp"
#include "shmring/segment.hpp"
#include "shmring/spmc_ring.hpp"
#include "shmring/spsc_ring.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <new>
#include <optional>
#include <string>
#include <utility>

namespace shmring {

/// @brief Describes how a fixed-capacity queue type is recorded in a segment header.
///
/// Specialized for `spsc_ring`, `blocking_spsc_ring`, `mpmc_ring` and `spmc_ring`; each
/// specialization provides `kind`, `elem_size`, `elem_align` and `capacity`.
/// @tparam Queue The queue type.
template <typename Queue>
struct queue_traits;

/// @brief Traits of `spsc_ring<T, N>`.
template <typename T, std::size_t N>
struct queue_traits<spsc_ring<T, N>> {
  static constexpr queue_kind kind = queue_kind::spsc_ring;  ///< Header kind.
  static constexpr std::size_t elem_size = sizeof(T);        ///< Header element size.
  static constexpr std::size_t elem_align = alignof(T);      ///< Header element alignment.
  static constexpr std::size_t capacity = N;                 ///< Header capacity.
};

/// @brief Traits of `blocking_spsc_ring<T, N>`.
template <typename T, std::size_t N>
struct queue_traits<blocking_spsc_ring<T, N>> {
  static constexpr queue_kind kind = queue_kind::blocking_spsc_ring;  ///< Header kind.
  static constexpr std::size_t elem_size = sizeof(T);                 ///< Header element size.
  static constexpr std::size_t elem_align = alignof(T);               ///< Header element alignment.
  static constexpr std::size_t capacity = N;                          ///< Header capacity.
};

/// @brief A fixed-capacity queue living in a shared-memory segment.
///
/// One process calls `create`, others call `open`. Afterwards the queue is used exactly as an
/// in-process one.
/// @tparam Queue A queue type with a `queue_traits` specialization and a default constructor.
/// @par Thread safety
/// Same as `Queue`; the handle itself is not safe to move or destroy concurrently.
template <typename Queue>
class shared_queue {
public:
  /// @brief Creates the segment, builds an empty queue in it and makes it available to `open`.
  /// @param name System-wide segment name, starting with '/'.
  /// @param mode Permission bits of the segment, subject to the process umask.
  /// @return The queue handle, or the error from `segment::create`.
  static result<shared_queue> create(const std::string& name, mode_t mode = default_segment_mode) {
    result<segment> seg = segment::create(name, expectation(), mode);
    if (!seg) {
      return seg.error();
    }
    segment mapped = std::move(seg).value();
    new (mapped.payload()) Queue();
    mapped.publish();
    return shared_queue(std::move(mapped));
  }

  /// @brief Attaches to a queue created by another process.
  /// @param name System-wide segment name.
  /// @param timeout How long to wait for the creator to finish; see `segment::open`.
  /// @return The queue handle, or the error from `segment::open`.
  static result<shared_queue> open(const std::string& name,
                                   std::optional<std::chrono::nanoseconds> timeout = std::nullopt) {
    result<segment> seg = segment::open(name, expectation(), timeout);
    if (!seg) {
      return seg.error();
    }
    return shared_queue(std::move(seg).value());
  }

  /// @brief Removes the segment name from the system; see `segment::unlink`.
  /// @param name System-wide segment name.
  /// @return Success or the error from `segment::unlink`.
  static result<unit> unlink(const std::string& name) {
    return segment::unlink(name);
  }

  /// @brief Returns the queue inside the segment.
  /// @return A reference valid as long as this handle is alive.
  Queue& queue() noexcept {
    return *queue_;
  }

private:
  using traits = queue_traits<Queue>;

  static_assert(sizeof(Queue) == payload_bytes(traits::kind,
                                               traits::elem_size,
                                               traits::capacity,
                                               traits::elem_align)
                                     .value_or(0),
                "segment validation and the queue layout disagree about the queue size");

  static segment_expectation expectation() noexcept {
    return {traits::kind,
            static_cast<std::uint32_t>(traits::elem_size),
            static_cast<std::uint32_t>(traits::elem_align),
            traits::capacity};
  }

  explicit shared_queue(segment seg) noexcept
      : segment_(std::move(seg)), queue_(std::launder(static_cast<Queue*>(segment_.payload()))) {}

  segment segment_;
  Queue* queue_;
};

/// @brief An `spsc_ring<T, N>` in a shared-memory segment.
/// @tparam T Element type, trivially copyable.
/// @tparam N Capacity in elements, a power of two.
template <typename T, std::size_t N>
using shared_spsc_ring = shared_queue<spsc_ring<T, N>>;

/// @brief Traits of `mpmc_ring<T, N>`.
template <typename T, std::size_t N>
struct queue_traits<mpmc_ring<T, N>> {
  static constexpr queue_kind kind = queue_kind::mpmc_ring;  ///< Header kind.
  static constexpr std::size_t elem_size = sizeof(T);        ///< Header element size.
  static constexpr std::size_t elem_align = alignof(T);      ///< Header element alignment.
  static constexpr std::size_t capacity = N;                 ///< Header capacity.
};

/// @brief Traits of `spmc_ring<T, N>`.
template <typename T, std::size_t N>
struct queue_traits<spmc_ring<T, N>> {
  static constexpr queue_kind kind = queue_kind::spmc_ring;  ///< Header kind.
  static constexpr std::size_t elem_size = sizeof(T);        ///< Header element size.
  static constexpr std::size_t elem_align = alignof(T);      ///< Header element alignment.
  static constexpr std::size_t capacity = N;                 ///< Header capacity.
};

/// @brief A `blocking_spsc_ring<T, N>` in a shared-memory segment.
/// @tparam T Element type, trivially copyable.
/// @tparam N Capacity in elements, a power of two.
template <typename T, std::size_t N>
using shared_blocking_spsc_ring = shared_queue<blocking_spsc_ring<T, N>>;

/// @brief An `mpmc_ring<T, N>` in a shared-memory segment.
/// @tparam T Element type, trivially copyable.
/// @tparam N Capacity in elements, a power of two.
template <typename T, std::size_t N>
using shared_mpmc_ring = shared_queue<mpmc_ring<T, N>>;

/// @brief An `spmc_ring<T, N>` in a shared-memory segment.
/// @tparam T Element type, trivially copyable.
/// @tparam N Capacity in elements, a power of two.
template <typename T, std::size_t N>
using shared_spmc_ring = shared_queue<spmc_ring<T, N>>;

}  // namespace shmring

#endif  // SHMRING_SHARED_QUEUE_HPP
