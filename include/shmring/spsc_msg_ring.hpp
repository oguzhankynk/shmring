#ifndef SHMRING_SPSC_MSG_RING_HPP
#define SHMRING_SPSC_MSG_RING_HPP

#include "shmring/config.hpp"
#include "shmring/span.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>

namespace shmring {

/// @brief Wait-free single-producer single-consumer ring of variable-length messages.
///
/// Messages are stored as records in a byte ring. The producer writes a message in place
/// (`reserve`, then `commit`) and the consumer reads it in place (`peek`, then `release`), so no
/// copy is needed on either side. A message never wraps around the end of the buffer; space left
/// at the end is skipped. Every record starts on an 8-byte boundary, so message payloads are
/// 8-byte aligned.
///
/// The ring lives in memory the caller provides (an ordinary buffer or a shared-memory segment) and
/// the `spsc_msg_ring` object is a small handle onto it. Each thread or process creates its own
/// handle with `init` (once, by the creator) or `attach` (everyone else). The memory contains no
/// pointers.
/// @par Thread safety
/// One producer and one consumer concurrently, each with its own handle or sharing one handle as
/// long as each side's functions are called from one thread only.
class spsc_msg_ring {
public:
  /// @brief Smallest supported capacity in bytes.
  static constexpr std::size_t min_capacity = min_message_ring_capacity;

  /// @brief Size in bytes of the record header that precedes every message.
  static constexpr std::size_t record_header_size = 8;

  /// @brief Computes how much memory a ring of the given capacity needs.
  /// @param capacity Ring capacity in bytes; a power of two of at least `min_capacity`.
  /// @return The required size in bytes, or 0 if `capacity` is not supported.
  static constexpr std::size_t required_bytes(std::size_t capacity) noexcept {
    return valid_capacity(capacity) ? 2 * cache_line + capacity : 0;
  }

  /// @brief Builds an empty ring in `memory` and returns a handle to it.
  /// @param memory Start of at least `required_bytes(capacity)` bytes, aligned to `cache_line`.
  ///        Its previous contents are overwritten.
  /// @param capacity Ring capacity in bytes; a power of two of at least `min_capacity`.
  /// @return A handle onto the new ring.
  /// @note Call once, before any other handle is attached.
  static spsc_msg_ring init(void* memory, std::size_t capacity) noexcept {
    // The control block lives in the caller-provided memory, not on the heap.
    // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
    auto* ctl = new (memory) control();
    std::byte* buffer = buffer_of(memory);
    std::memset(buffer, 0, capacity);
    return {ctl, buffer, capacity};
  }

  /// @brief Returns a handle onto a ring that was built by `init`, possibly in another process.
  /// @param memory The same memory that was passed to `init`.
  /// @param capacity The same capacity that was passed to `init`.
  /// @return A handle onto the ring.
  static spsc_msg_ring attach(void* memory, std::size_t capacity) noexcept {
    return {static_cast<control*>(memory), buffer_of(memory), capacity};
  }

  /// @brief Returns the ring capacity.
  /// @return Capacity in bytes, including record headers and padding.
  [[nodiscard]] std::size_t capacity() const noexcept {
    return capacity_;
  }

  /// @brief Returns the largest message `reserve` accepts.
  /// @return Size in bytes; half the capacity minus one record header.
  [[nodiscard]] std::size_t max_message_size() const noexcept {
    return capacity_ / 2 - record_header_size;
  }

  /// @brief Returns the number of ring bytes in use, without synchronization.
  /// @return A value in `[0, capacity()]` that may already be out of date.
  [[nodiscard]] std::size_t size_approx() const noexcept {
    // Relaxed: advisory only. Reading the consumer's cursor first keeps write >= read.
    const std::uint64_t r = ctl_->read_cursor.load(std::memory_order_relaxed);
    const std::uint64_t w = ctl_->write_cursor.load(std::memory_order_relaxed);
    return std::min<std::uint64_t>(w - r, capacity_);
  }

  /// @brief Reserves room for a message and returns a writable view of it. Producer only.
  ///
  /// Nothing is visible to the consumer until `commit`. If the producer stops after `reserve`,
  /// the consumer never sees the message. Calling `reserve` again before `commit` abandons the
  /// previous reservation.
  /// @param length Message size in bytes, at most `max_message_size()`.
  /// @return A view of `length` writable bytes, 8-byte aligned, or an empty view (converts to
  ///         `false`) if the ring does not have room or `length` is too large.
  /// @par Progress
  /// Wait-free.
  span<std::byte> reserve(std::size_t length) noexcept {
    pending_ = false;
    if (length > max_message_size()) {
      return {};
    }
    // Relaxed: only the producer writes write_cursor, so this reads our own last store.
    const std::uint64_t w = ctl_->write_cursor.load(std::memory_order_relaxed);
    if (!is_record_aligned(w)) {
      return {};  // Only reachable if the shared memory was corrupted by another party.
    }
    const std::uint64_t total = record_size(length);
    const std::uint64_t to_end = capacity_ - (w & mask());
    const std::uint64_t skip = total > to_end ? to_end : 0;
    const std::uint64_t need = skip + total;
    std::uint64_t used = w - ctl_->cached_read;
    if (used > capacity_ || capacity_ - used < need) {
      // Acquire: pairs with the release store to read_cursor in release(). The consumer has
      // finished reading everything below the cursor value we see, so that space is reusable.
      ctl_->cached_read = ctl_->read_cursor.load(std::memory_order_acquire);
      used = w - ctl_->cached_read;
      if (used > capacity_ || capacity_ - used < need) {
        return {};
      }
    }
    pending_ = true;
    pending_start_ = w;
    pending_skip_ = skip;
    pending_length_ = length;
    return {byte_at(w + skip + record_header_size), length};
  }

  /// @brief Publishes the reserved message with its reserved length. Producer only.
  /// @note Does nothing if there is no reservation.
  void commit() noexcept {
    commit(pending_length_);
  }

  /// @brief Publishes the reserved message, using only the first `length` bytes of it.
  /// @param length Number of bytes actually written; at most the reserved length.
  /// @note Does nothing if there is no reservation.
  void commit(std::size_t length) noexcept {
    if (!pending_) {
      return;
    }
    pending_ = false;
    length = std::min(length, pending_length_);
    const std::uint64_t record = pending_start_ + pending_skip_;
    if (pending_skip_ != 0) {
      header_at(pending_start_)->length.store(skip_marker, std::memory_order_relaxed);
    }
    // Relaxed: the release store to write_cursor below orders this store before the consumer's
    // acquire load of write_cursor, which the consumer performs before reading the header.
    header_at(record)->length.store(static_cast<std::uint32_t>(length), std::memory_order_relaxed);
    // Release: pairs with the acquire load of write_cursor in peek(). It publishes the record
    // header and the payload written through the reserved view.
    ctl_->write_cursor.store(record + record_size(length), std::memory_order_release);
  }

  /// @brief Copies a message into the ring in one step. Producer only.
  /// @param data Start of the message.
  /// @param length Message size in bytes, at most `max_message_size()`.
  /// @return `true` if the message was stored, `false` if the ring is full or `length` is too
  ///         large.
  bool try_push(const void* data, std::size_t length) noexcept {
    const span<std::byte> dst = reserve(length);
    if (!dst) {
      return false;
    }
    if (length != 0) {
      std::memcpy(dst.data(), data, length);
    }
    commit();
    return true;
  }

  /// @brief Returns the oldest message without removing it. Consumer only.
  ///
  /// Calling `peek` again before `release` returns the same message.
  /// @return A read-only view of the message, 8-byte aligned, valid until `release`; or an empty
  ///         view (converts to `false`) if the ring is empty. A zero-length message gives a
  ///         non-empty view with `size() == 0`.
  /// @par Progress
  /// Wait-free.
  span<const std::byte> peek() noexcept {
    peeked_ = false;
    // Relaxed: only the consumer writes read_cursor, so this reads our own last store.
    std::uint64_t r = ctl_->read_cursor.load(std::memory_order_relaxed);
    if (!is_record_aligned(r)) {
      return {};  // Only reachable if the shared memory was corrupted by another party.
    }
    for (int attempt = 0; attempt < 2; ++attempt) {
      if (ctl_->cached_write == r) {
        // Acquire: pairs with the release store to write_cursor in commit(). It makes the
        // record header and payload visible before we read them.
        ctl_->cached_write = ctl_->write_cursor.load(std::memory_order_acquire);
        if (ctl_->cached_write == r) {
          return {};
        }
      }
      const std::uint64_t available = ctl_->cached_write - r;
      if (available > capacity_) {
        return {};  // Only reachable if the shared memory was corrupted by another party.
      }
      const std::uint64_t to_end = capacity_ - (r & mask());
      const std::uint32_t length = header_at(r)->length.load(std::memory_order_relaxed);
      if (length == skip_marker) {
        r += to_end;
        continue;
      }
      const std::uint64_t total = record_size(length);
      if (length > max_message_size() || total > available || total > to_end) {
        return {};  // Corrupted memory; refuse to hand out a view that is not inside the ring.
      }
      peeked_ = true;
      peeked_end_ = r + total;
      return {byte_at(r + record_header_size), length};
    }
    return {};
  }

  /// @brief Removes the message last returned by `peek` and frees its space. Consumer only.
  /// @note Does nothing if the last `peek` returned an empty view.
  void release() noexcept {
    if (!peeked_) {
      return;
    }
    peeked_ = false;
    // Release: pairs with the acquire load of read_cursor in reserve(). It orders our reads of
    // the message before the producer's reuse of its space.
    ctl_->read_cursor.store(peeked_end_, std::memory_order_release);
  }

private:
  static constexpr std::uint32_t skip_marker = 0xFFFFFFFFU;

  struct record_header {
    std::atomic<std::uint32_t> length;
    std::uint32_t reserved;
  };
  static_assert(sizeof(record_header) == record_header_size);

  struct control {
    alignas(cache_line) std::atomic<std::uint64_t> write_cursor{0};
    std::uint64_t cached_read{0};  // producer's last observed read_cursor
    alignas(cache_line) std::atomic<std::uint64_t> read_cursor{0};
    std::uint64_t cached_write{0};  // consumer's last observed write_cursor
  };
  static_assert(sizeof(control) == 2 * cache_line);
  static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
  static_assert(std::atomic<std::uint32_t>::is_always_lock_free);

  static constexpr std::size_t record_alignment = 8;
  static constexpr std::size_t max_capacity = std::size_t{1} << 40;

  static constexpr bool valid_capacity(std::size_t capacity) noexcept {
    return capacity >= min_capacity && (capacity & (capacity - 1)) == 0 && capacity <= max_capacity;
  }

  // Records start on 8-byte boundaries, so a cursor that is not a multiple of 8 can only come from
  // corrupted memory; using it would access a record header at a misaligned address.
  static constexpr bool is_record_aligned(std::uint64_t cursor) noexcept {
    return (cursor & (record_alignment - 1)) == 0;
  }

  static constexpr std::uint64_t record_size(std::uint64_t length) noexcept {
    return record_header_size + ((length + (record_alignment - 1)) & ~(record_alignment - 1));
  }

  // The buffer is raw bytes addressed by a cursor, so a few places need pointer arithmetic or a
  // cast; they are all in these helpers.
  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic,cppcoreguidelines-pro-type-reinterpret-cast)
  static std::byte* buffer_of(void* memory) noexcept {
    return static_cast<std::byte*>(memory) + sizeof(control);
  }

  std::byte* byte_at(std::uint64_t cursor) noexcept {
    return buffer_ + (cursor & mask());
  }

  record_header* header_at(std::uint64_t cursor) noexcept {
    return std::launder(reinterpret_cast<record_header*>(byte_at(cursor)));
  }
  // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic,cppcoreguidelines-pro-type-reinterpret-cast)

  spsc_msg_ring(control* ctl, std::byte* buffer, std::size_t capacity) noexcept
      : ctl_(ctl), buffer_(buffer), capacity_(capacity) {}

  [[nodiscard]] std::uint64_t mask() const noexcept {
    return capacity_ - 1;
  }

  control* ctl_;
  std::byte* buffer_;
  std::size_t capacity_;

  // Producer-side state of the reservation in progress; never shared.
  bool pending_ = false;
  std::uint64_t pending_start_ = 0;
  std::uint64_t pending_skip_ = 0;
  std::size_t pending_length_ = 0;

  // Consumer-side state of the message last peeked; never shared.
  bool peeked_ = false;
  std::uint64_t peeked_end_ = 0;
};

}  // namespace shmring

#endif  // SHMRING_SPSC_MSG_RING_HPP
