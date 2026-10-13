#ifndef SHMRING_SPAN_HPP
#define SHMRING_SPAN_HPP

#include <cstddef>

namespace shmring {

/// @brief A non-owning view of `size()` consecutive objects, like C++20 `std::span`.
/// @tparam T Element type; use `const std::byte` for read-only views.
template <typename T>
class span {
public:
  /// @brief Creates an empty view.
  constexpr span() noexcept = default;

  /// @brief Creates a view of `size` elements starting at `data`.
  /// @param data First element, or null for an empty view.
  /// @param size Number of elements.
  constexpr span(T* data, std::size_t size) noexcept : data_(data), size_(size) {}

  /// @brief Returns the first element.
  /// @return A pointer, or null if the view is empty because an operation failed.
  [[nodiscard]] constexpr T* data() const noexcept {
    return data_;
  }

  /// @brief Returns the number of elements.
  /// @return The element count, possibly zero for a valid zero-length view.
  [[nodiscard]] constexpr std::size_t size() const noexcept {
    return size_;
  }

  /// @brief Tells whether the operation that produced the view succeeded.
  /// @return `true` if `data()` is not null. A successful zero-length view is also `true`.
  [[nodiscard]] constexpr explicit operator bool() const noexcept {
    return data_ != nullptr;
  }

  /// @brief Returns an iterator to the first element.
  /// @return `data()`.
  [[nodiscard]] constexpr T* begin() const noexcept {
    return data_;
  }

  /// @brief Returns an iterator past the last element.
  /// @return `data() + size()`.
  [[nodiscard]] constexpr T* end() const noexcept {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic): one past the end of the view
    return data_ + size_;
  }

private:
  T* data_ = nullptr;
  std::size_t size_ = 0;
};

}  // namespace shmring

#endif  // SHMRING_SPAN_HPP
