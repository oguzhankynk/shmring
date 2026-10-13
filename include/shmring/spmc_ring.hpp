#ifndef SHMRING_SPMC_RING_HPP
#define SHMRING_SPMC_RING_HPP

#include "shmring/config.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace shmring {

/// @brief Result of `spmc_ring::try_read`.
enum class read_status {
  ok,      ///< An element was read.
  empty,   ///< The reader has seen everything the writer has published.
  overrun  ///< The writer got too far ahead; the reader skipped forward and lost elements.
};

/// @brief Broadcast ring: one writer publishes elements and any number of readers each see all
///        of them.
///
/// The writer never waits for readers. Each reader keeps its own position in a `spmc_reader`
/// object, so readers do not affect each other or the writer. A reader that falls more than the
/// capacity behind is told so (`read_status::overrun`) and skips forward to recent elements; it
/// never receives torn or mixed-up data. Slots are protected by sequence stamps (a seqlock), and
/// every shared access is an atomic one, so the code has no data race.
///
/// The object contains no pointers and works in shared memory. A reader that stops costs nothing,
/// and a new reader can join at any time.
/// @tparam T Element type. Must be trivially copyable.
/// @tparam N Capacity in elements. Must be a power of two.
/// @par Progress
/// `publish` is wait-free. `try_read` is wait-free.
/// @par Thread safety
/// One thread or process may call `publish`; any number may call `try_read`, each with its own
/// `spmc_reader`.
template <typename T, std::size_t N>
class spmc_ring;

/// @brief A reader's private position in a broadcast ring.
///
/// Create it with `spmc_ring::join_latest` or `spmc_ring::join_oldest`. It is plain data owned by
/// one reader; do not share one between threads.
class spmc_reader {
public:
  /// @brief Returns how many elements this reader has missed because of overruns.
  /// @return The count of skipped elements.
  [[nodiscard]] std::uint64_t dropped() const noexcept {
    return dropped_;
  }

private:
  template <typename T, std::size_t N>
  friend class spmc_ring;

  std::uint64_t next_ = 0;
  std::uint64_t dropped_ = 0;
};

template <typename T, std::size_t N>
class spmc_ring {
  static_assert(std::is_trivially_copyable_v<T>, "spmc_ring elements must be trivially copyable");
  static_assert(N >= 2 && (N & (N - 1)) == 0, "spmc_ring capacity must be a power of two");
  static_assert(
      sizeof(std::size_t) == sizeof(std::uint64_t),
      "counters are 64-bit and are used directly as sizes; a 64-bit platform is required");
  static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                "64-bit atomics must be lock-free to be usable across processes");

public:
  /// @brief Creates an empty ring.
  spmc_ring() noexcept = default;

  spmc_ring(const spmc_ring&) = delete;
  spmc_ring& operator=(const spmc_ring&) = delete;
  spmc_ring(spmc_ring&&) = delete;
  spmc_ring& operator=(spmc_ring&&) = delete;
  ~spmc_ring() = default;

  /// @brief Returns the number of elements the ring keeps for slow readers.
  /// @return `N`.
  static constexpr std::size_t capacity() noexcept {
    return N;
  }

  /// @brief Returns how many elements have been published so far.
  /// @return The total count since the ring was created.
  [[nodiscard]] std::uint64_t published() const noexcept {
    return head_.load(std::memory_order_acquire);
  }

  /// @brief Publishes an element, overwriting the oldest one if the ring is full. Writer only.
  /// @param value The element to copy into the ring.
  /// @par Progress
  /// Wait-free.
  void publish(const T& value) noexcept {
    // Relaxed: only the writer writes head_, so this reads our own last store.
    const std::uint64_t pos = head_.load(std::memory_order_relaxed);
    cell& c = cell_at(pos);
    // Relaxed, followed by a release fence: marks the slot as being rewritten. The fence keeps the
    // marker ahead of the data stores below, so a reader that sees any new data also sees that the
    // slot is no longer the one it expects.
    c.stamp.store(2 * pos + 1, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_release);
    store_words(c, value);
    // Release: pairs with the acquire load of the stamp in try_read. Everything stored above is
    // visible to a reader that sees this stamp.
    c.stamp.store(2 * pos + 2, std::memory_order_release);
    // Release: pairs with the acquire load of head_ in published() and join_*(); publishes the
    // new count.
    head_.store(pos + 1, std::memory_order_release);
  }

  /// @brief Creates a reader that will see only elements published from now on.
  /// @return A reader positioned at the current end of the ring.
  [[nodiscard]] spmc_reader join_latest() const noexcept {
    spmc_reader r;
    r.next_ = head_.load(std::memory_order_acquire);
    return r;
  }

  /// @brief Creates a reader that starts with the oldest element still in the ring.
  /// @return A reader positioned at the oldest element that is not about to be overwritten.
  [[nodiscard]] spmc_reader join_oldest() const noexcept {
    spmc_reader r;
    const std::uint64_t head = head_.load(std::memory_order_acquire);
    r.next_ = head > N / 2 ? head - N / 2 : 0;
    return r;
  }

  /// @brief Reads the reader's next element.
  /// @param reader The reader's position; advanced when an element is read or after an overrun.
  /// @param out Receives the element when the result is `read_status::ok`.
  /// @return `ok` if an element was read, `empty` if there is nothing new, or `overrun` if the
  ///         writer overtook the reader. After `overrun` the reader has been moved to a recent
  ///         element, `reader.dropped()` has grown by the number of skipped elements, and the next
  ///         call continues from there. `out` is unchanged unless the result is `ok`.
  /// @par Progress
  /// Wait-free.
  read_status try_read(spmc_reader& reader, T& out) const noexcept {
    const std::uint64_t want = reader.next_;
    const cell& c = cell_at(want);
    // Acquire: pairs with the release store of the stamp in publish. If the stamp says position
    // `want` is complete, the data stores before it are visible.
    const std::uint64_t before = c.stamp.load(std::memory_order_acquire);
    const std::uint64_t done = 2 * want + 2;
    if (before < done) {
      return read_status::empty;
    }
    if (before == done) {
      std::array<std::uint64_t, word_count> words{};
      load_words(c, words);
      // Acquire fence, then re-read the stamp: if it is unchanged, the writer did not start
      // rewriting the slot while we copied, so the words form one consistent element.
      std::atomic_thread_fence(std::memory_order_acquire);
      if (c.stamp.load(std::memory_order_relaxed) == before) {
        std::memcpy(&out, words.data(), sizeof(T));
        reader.next_ = want + 1;
        return read_status::ok;
      }
    }
    // The writer is at least one lap ahead of this reader. Jump to a position that is not about
    // to be overwritten and account for the elements skipped.
    const std::uint64_t head = head_.load(std::memory_order_acquire);
    const std::uint64_t target = head > N / 2 ? head - N / 2 : 0;
    const std::uint64_t resume = std::max(target, want + 1);
    reader.dropped_ += resume - want;
    reader.next_ = resume;
    return read_status::overrun;
  }

private:
  static constexpr std::uint64_t mask = N - 1;
  static constexpr std::size_t word_count =
      (sizeof(T) + sizeof(std::uint64_t) - 1) / sizeof(std::uint64_t);

  struct cell {
    std::atomic<std::uint64_t> stamp{0};
    std::array<std::atomic<std::uint64_t>, word_count> words{};
  };

  // The slot index is masked, so it is always below N.
  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-constant-array-index)
  [[nodiscard]] cell& cell_at(std::uint64_t position) noexcept {
    return cells_[position & mask];
  }
  [[nodiscard]] const cell& cell_at(std::uint64_t position) const noexcept {
    return cells_[position & mask];
  }

  // The payload is moved as 64-bit words, the same way in and out of the slot; the word index is
  // the loop counter over the fixed word count.
  static void store_words(cell& c, const T& value) noexcept {
    std::array<std::uint64_t, word_count> words{};
    std::memcpy(words.data(), &value, sizeof(T));
    for (std::size_t i = 0; i < word_count; ++i) {
      c.words[i].store(words[i], std::memory_order_relaxed);
    }
  }

  static void load_words(const cell& c, std::array<std::uint64_t, word_count>& words) noexcept {
    for (std::size_t i = 0; i < word_count; ++i) {
      words[i] = c.words[i].load(std::memory_order_relaxed);
    }
  }
  // NOLINTEND(cppcoreguidelines-pro-bounds-constant-array-index)

  alignas(cache_line) std::atomic<std::uint64_t> head_{0};
  alignas(cache_line) std::array<cell, N> cells_;
};

}  // namespace shmring

#endif  // SHMRING_SPMC_RING_HPP
