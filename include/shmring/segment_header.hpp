#ifndef SHMRING_SEGMENT_HEADER_HPP
#define SHMRING_SEGMENT_HEADER_HPP

#include "shmring/config.hpp"
#include "shmring/error.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>

static_assert(sizeof(std::size_t) == sizeof(std::uint64_t), "a 64-bit platform is required");

namespace shmring {

/// @brief Kind of queue stored in a segment.
enum class queue_kind : std::uint32_t {
  spsc_ring = 1,          ///< `spsc_ring<T, N>`.
  spsc_msg_ring = 2,      ///< Variable-length message ring.
  mpmc_ring = 3,          ///< Bounded multi-producer multi-consumer ring.
  spmc_ring = 4,          ///< Broadcast ring.
  blocking_spsc_ring = 5  ///< `blocking_spsc_ring<T, N>`.
};

/// @brief Initialization state stored in the segment header.
enum class init_state : std::uint32_t {
  uninitialized = 0,  ///< Nothing has been written yet.
  initializing = 1,   ///< The creator is still building the queue.
  ready = 2           ///< The queue is complete and may be used.
};

/// @brief Identifies a shmring segment.
inline constexpr std::uint64_t segment_magic = 0x00474E49524D4853ULL;  // "SHMRING\0"

/// @brief Version of the segment memory layout.
inline constexpr std::uint32_t segment_layout_version = 1;

/// @brief What an opener expects a segment to contain.
struct segment_expectation {
  queue_kind kind;           ///< Required queue kind.
  std::uint32_t elem_size;   ///< Required element size in bytes.
  std::uint32_t elem_align;  ///< Required element alignment in bytes.
  std::uint64_t capacity;    ///< Required capacity in elements; 0 accepts any power of two.
};

/// @brief Fixed-layout header at offset 0 of every segment.
///
/// All fields except `state` are written by the creator before the segment becomes ready and are
/// never changed afterwards. Nothing in the header is a pointer.
struct alignas(cache_line) segment_header {
  std::uint64_t magic;               ///< `segment_magic`.
  std::uint32_t layout_version;      ///< `segment_layout_version`.
  std::uint32_t kind;                ///< A `queue_kind` value.
  std::uint32_t elem_size;           ///< `sizeof` the queue's element type.
  std::uint32_t elem_align;          ///< `alignof` the queue's element type.
  std::uint64_t capacity;            ///< Queue capacity in elements.
  std::uint32_t creator_pid;         ///< Process id of the creator.
  std::atomic<std::uint32_t> state;  ///< An `init_state` value; published with release.
  std::uint32_t cache_line_size;     ///< `cache_line` of the creator's build.

  /// @brief Fills in the header for a segment that is still being initialized.
  /// @param layout Queue kind, element layout and capacity to record.
  /// @param pid Process id of the creator.
  segment_header(const segment_expectation& layout, std::uint32_t pid) noexcept
      : magic(segment_magic),
        layout_version(segment_layout_version),
        kind(static_cast<std::uint32_t>(layout.kind)),
        elem_size(layout.elem_size),
        elem_align(layout.elem_align),
        capacity(layout.capacity),
        creator_pid(pid),
        state(static_cast<std::uint32_t>(init_state::initializing)),
        cache_line_size(static_cast<std::uint32_t>(cache_line)) {}
};

static_assert(std::atomic<std::uint32_t>::is_always_lock_free,
              "the state word must be lock-free to be usable across processes");

/// @brief Values read from a validated segment header.
struct segment_info {
  queue_kind kind;            ///< Queue kind.
  std::uint32_t elem_size;    ///< Element size in bytes.
  std::uint32_t elem_align;   ///< Element alignment in bytes.
  std::uint64_t capacity;     ///< Capacity in elements.
  std::uint32_t creator_pid;  ///< Process id of the creator.
};

/// @brief Computes how many bytes the queue behind the header occupies.
/// @param kind Queue kind.
/// @param elem_size Element size in bytes (1 for the message ring).
/// @param capacity Capacity in elements (bytes for the message ring).
/// @param elem_align Element alignment in bytes; only the MPMC ring's slot size depends on it.
/// @return The size in bytes, or an empty value if the combination is not valid (unsupported
///         kind, zero or oversized capacity, zero element size, bad alignment, or arithmetic
///         overflow).
constexpr std::optional<std::size_t> payload_bytes(queue_kind kind,
                                                   std::uint64_t elem_size,
                                                   std::uint64_t capacity,
                                                   std::uint64_t elem_align = 1) noexcept {
  constexpr std::uint64_t max_capacity = std::uint64_t{1} << 40;
  constexpr std::uint64_t max_data_bytes = std::uint64_t{1} << 46;
  if (capacity == 0 || capacity > max_capacity || elem_size == 0 || elem_align == 0 ||
      (elem_align & (elem_align - 1)) != 0 || elem_align > cache_line ||
      elem_size > max_data_bytes / capacity / 2) {
    return std::nullopt;
  }
  const auto round_up = [](std::uint64_t value, std::uint64_t unit) {
    return (value + unit - 1) / unit * unit;
  };
  switch (kind) {
    case queue_kind::spsc_ring:
      return 2 * cache_line + round_up(capacity * elem_size, cache_line);
    case queue_kind::blocking_spsc_ring:
      return 4 * cache_line + round_up(capacity * elem_size, cache_line);
    case queue_kind::mpmc_ring: {
      const std::uint64_t slot_align = elem_align > 8 ? elem_align : 8;
      const std::uint64_t slot = round_up(8 + elem_size, slot_align);
      return 2 * cache_line + round_up(capacity * slot, cache_line);
    }
    case queue_kind::spmc_ring: {
      if (capacity < 2) {
        return std::nullopt;
      }
      const std::uint64_t slot = 8 + round_up(elem_size, 8);
      return cache_line + round_up(capacity * slot, cache_line);
    }
    case queue_kind::spsc_msg_ring:
      if (elem_size != 1 || capacity < min_message_ring_capacity) {
        return std::nullopt;
      }
      return 2 * cache_line + capacity;
    default:
      return std::nullopt;
  }
}

/// @brief Checks a mapped segment against an expectation, treating its contents as untrusted.
///
/// Nothing in the memory is trusted: every field is compared against `want`, the capacity must be
/// a power of two, and the whole queue must fit inside `mapped_size`. The function only reads
/// memory inside `[base, base + mapped_size)` and never follows an offset taken from it.
/// @param base Start of the mapping. Must be aligned to `cache_line`.
/// @param mapped_size Size of the mapping in bytes.
/// @param want What the caller expects to find.
/// @return The header's values on success, otherwise the first check that failed.
inline result<segment_info> validate_header(const void* base,
                                            std::size_t mapped_size,
                                            const segment_expectation& want) noexcept {
  // The address is only checked for alignment.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (base == nullptr || reinterpret_cast<std::uintptr_t>(base) % cache_line != 0) {
    return error{errc::invalid_argument};
  }
  if (mapped_size < sizeof(segment_header)) {
    return error{errc::too_small};
  }
  const auto* h = static_cast<const segment_header*>(base);
  // Acquire: pairs with the release store of `ready` in segment::publish. Once we see `ready`,
  // every header field the creator wrote before that store is visible to us.
  if (h->state.load(std::memory_order_acquire) != static_cast<std::uint32_t>(init_state::ready)) {
    return error{errc::not_ready};
  }
  if (h->magic != segment_magic) {
    return error{errc::bad_magic};
  }
  if (h->layout_version != segment_layout_version) {
    return error{errc::bad_version};
  }
  if (h->cache_line_size != cache_line) {
    return error{errc::bad_cache_line};
  }
  if (h->kind != static_cast<std::uint32_t>(want.kind)) {
    return error{errc::bad_kind};
  }
  if (h->elem_size != want.elem_size) {
    return error{errc::bad_element_size};
  }
  if (h->elem_align != want.elem_align) {
    return error{errc::bad_alignment};
  }
  const std::uint64_t capacity = h->capacity;
  if (capacity == 0 || (capacity & (capacity - 1)) != 0 ||
      (want.capacity != 0 && capacity != want.capacity)) {
    return error{errc::bad_capacity};
  }
  const std::optional<std::size_t> payload =
      payload_bytes(want.kind, h->elem_size, capacity, h->elem_align);
  if (!payload) {
    return error{errc::bad_capacity};
  }
  if (*payload > mapped_size - sizeof(segment_header)) {
    return error{errc::too_small};
  }
  return segment_info{want.kind, h->elem_size, h->elem_align, capacity, h->creator_pid};
}

}  // namespace shmring

#endif  // SHMRING_SEGMENT_HEADER_HPP
