#ifndef SHMRING_ERROR_HPP
#define SHMRING_ERROR_HPP

#include <cstdlib>
#include <utility>
#include <variant>

namespace shmring {

/// @brief Reason a setup operation (create, open, validate) failed.
enum class errc {
  invalid_argument = 1,  ///< A parameter was malformed, for example a name without a leading '/'.
  already_exists,        ///< `segment::create` found a segment with that name.
  not_found,             ///< `segment::open` found no segment with that name.
  permission_denied,     ///< The operating system refused access.
  system_error,          ///< Another system call failed; `error::sys_errno` has the reason.
  not_ready,             ///< The segment is not fully initialized yet.
  timed_out,             ///< The segment did not become ready within the timeout.
  too_small,             ///< The mapped memory is smaller than the segment layout requires.
  bad_magic,             ///< The memory is not a shmring segment.
  bad_version,           ///< The segment uses a different layout version.
  bad_kind,              ///< The segment holds a different kind of queue.
  bad_element_size,      ///< The segment's element size differs from the expected one.
  bad_alignment,         ///< The segment's element alignment differs from the expected one.
  bad_capacity,          ///< The capacity is zero, not a power of two, or not the expected one.
  bad_cache_line         ///< The segment was built with a different cache line size.
};

/// @brief Returns a short name for an error code.
/// @param code The code to describe.
/// @return A static, null-terminated string. Never null.
inline const char* to_string(errc code) noexcept {
  switch (code) {
    case errc::invalid_argument:
      return "invalid_argument";
    case errc::already_exists:
      return "already_exists";
    case errc::not_found:
      return "not_found";
    case errc::permission_denied:
      return "permission_denied";
    case errc::system_error:
      return "system_error";
    case errc::not_ready:
      return "not_ready";
    case errc::timed_out:
      return "timed_out";
    case errc::too_small:
      return "too_small";
    case errc::bad_magic:
      return "bad_magic";
    case errc::bad_version:
      return "bad_version";
    case errc::bad_kind:
      return "bad_kind";
    case errc::bad_element_size:
      return "bad_element_size";
    case errc::bad_alignment:
      return "bad_alignment";
    case errc::bad_capacity:
      return "bad_capacity";
    case errc::bad_cache_line:
      return "bad_cache_line";
  }
  return "unknown";
}

/// @brief Describes why a setup operation failed.
struct error {
  errc code;         ///< What went wrong.
  int sys_errno{0};  ///< The `errno` value of the failed system call, or 0 if none applies.
};

/// @brief Value type for operations that succeed without producing a value.
struct unit {};

/// @brief Holds either a value of type `T` or an error of type `E`.
///
/// C++17 has no `std::expected`; this is the small subset the library needs.
/// @tparam T The type of the success value.
/// @tparam E The type of the error. Defaults to `shmring::error`.
template <typename T, typename E = error>
class result {
public:
  /// @brief Creates a successful result.
  /// @param value The success value.
  // Implicit on purpose: `return value;` and `return error{...};` read like std::expected.
  // cppcheck-suppress noExplicitConstructor
  result(T value) : storage_(std::in_place_index<0>, std::move(value)) {}

  /// @brief Creates a failed result.
  /// @param err The error.
  // cppcheck-suppress noExplicitConstructor
  result(E err) : storage_(std::in_place_index<1>, std::move(err)) {}

  /// @brief Tells whether the result holds a value.
  /// @return `true` for success, `false` for an error.
  [[nodiscard]] bool has_value() const noexcept {
    return storage_.index() == 0;
  }

  /// @brief Same as `has_value()`.
  [[nodiscard]] explicit operator bool() const noexcept {
    return has_value();
  }

  /// @brief Returns the success value.
  /// @return A reference to the value.
  /// @note Calling this on an error result terminates the program.
  [[nodiscard]] T& value() & noexcept {
    return *checked_value();
  }
  /// @copydoc value()
  [[nodiscard]] const T& value() const& noexcept {
    return *checked_value();
  }
  /// @copydoc value()
  [[nodiscard]] T&& value() && noexcept {
    return std::move(*checked_value());
  }

  /// @brief Returns the error.
  /// @return A reference to the error.
  /// @note Calling this on a successful result terminates the program.
  [[nodiscard]] const E& error() const noexcept {
    const E* err = std::get_if<1>(&storage_);
    if (err == nullptr) {
      std::abort();
    }
    return *err;
  }

private:
  [[nodiscard]] T* checked_value() noexcept {
    T* v = std::get_if<0>(&storage_);
    if (v == nullptr) {
      std::abort();
    }
    return v;
  }
  [[nodiscard]] const T* checked_value() const noexcept {
    const T* v = std::get_if<0>(&storage_);
    if (v == nullptr) {
      std::abort();
    }
    return v;
  }

  std::variant<T, E> storage_;
};

}  // namespace shmring

#endif  // SHMRING_ERROR_HPP
