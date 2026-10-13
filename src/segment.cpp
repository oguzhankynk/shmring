#include "shmring/segment.hpp"

#include <algorithm>
#include <cerrno>
#include <new>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

namespace shmring {
namespace {

using std::chrono::microseconds;
using std::chrono::milliseconds;
using std::chrono::nanoseconds;

error from_errno(int err) noexcept {
  switch (err) {
    case EEXIST:
      return error{errc::already_exists, err};
    case ENOENT:
      return error{errc::not_found, err};
    case EACCES:
    case EPERM:
      return error{errc::permission_denied, err};
    case EINVAL:
    case ENAMETOOLONG:
      return error{errc::invalid_argument, err};
    default:
      return error{errc::system_error, err};
  }
}

// Number of immediate re-checks before the waiter starts sleeping.
constexpr int spin_attempts = 64;
constexpr microseconds first_nap{50};
constexpr milliseconds longest_nap{10};

bool valid_name(const std::string& name) noexcept {
  return name.size() > 1 && name.front() == '/';
}

// Calls `ready` until it returns true or the timeout passes. Returns whether it became true.
// Spins briefly first because publication normally follows creation within microseconds.
template <typename Ready>
bool wait_until_ready(Ready ready, std::optional<nanoseconds> timeout) {
  using clock = std::chrono::steady_clock;
  if (ready()) {
    return true;
  }
  if (timeout && *timeout <= nanoseconds::zero()) {
    return false;
  }
  const clock::time_point deadline = timeout ? clock::now() + *timeout : clock::time_point::max();
  for (int i = 0; i < spin_attempts; ++i) {
    if (ready()) {
      return true;
    }
  }
  microseconds nap = first_nap;
  for (;;) {
    if (ready()) {
      return true;
    }
    const clock::time_point now = clock::now();
    if (timeout && now >= deadline) {
      return false;
    }
    microseconds this_nap = nap;
    if (timeout) {
      this_nap =
          std::min(nap, std::chrono::duration_cast<microseconds>(deadline - now) + microseconds{1});
    }
    std::this_thread::sleep_for(this_nap);
    nap = std::min<microseconds>(nap * 2, longest_nap);
  }
}

struct fd_guard {
  int fd;
  ~fd_guard() {
    if (fd >= 0) {
      ::close(fd);
    }
  }
  fd_guard(const fd_guard&) = delete;
  fd_guard& operator=(const fd_guard&) = delete;
  fd_guard(fd_guard&&) = delete;
  fd_guard& operator=(fd_guard&&) = delete;
  explicit fd_guard(int f) noexcept : fd(f) {}
};

}  // namespace

result<segment> segment::create(const std::string& name,
                                const segment_expectation& layout,
                                mode_t mode) {
  if (!valid_name(name)) {
    return error{errc::invalid_argument};
  }
  if (layout.capacity == 0 || (layout.capacity & (layout.capacity - 1)) != 0) {
    return error{errc::bad_capacity};
  }
  const std::optional<std::size_t> payload =
      payload_bytes(layout.kind, layout.elem_size, layout.capacity, layout.elem_align);
  if (!payload) {
    return error{errc::bad_capacity};
  }
  const std::size_t total = sizeof(segment_header) + *payload;

  fd_guard fd(::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, mode));
  if (fd.fd < 0) {
    return from_errno(errno);
  }
  if (::ftruncate(fd.fd, static_cast<off_t>(total)) != 0) {
    const int err = errno;
    ::shm_unlink(name.c_str());
    return from_errno(err);
  }
  void* base = ::mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd.fd, 0);
  if (base == MAP_FAILED) {
    const int err = errno;
    ::shm_unlink(name.c_str());
    return from_errno(err);
  }
  const auto pid = static_cast<std::uint32_t>(::getpid());
  new (base)
      segment_header(layout, pid);  // NOLINT(cppcoreguidelines-owning-memory): lives in the mapping
  return segment(
      base,
      total,
      segment_info{layout.kind, layout.elem_size, layout.elem_align, layout.capacity, pid});
}

result<segment> segment::open(const std::string& name,
                              const segment_expectation& layout,
                              std::optional<nanoseconds> timeout) {
  if (!valid_name(name)) {
    return error{errc::invalid_argument};
  }
  fd_guard fd(::shm_open(name.c_str(), O_RDWR, 0));
  if (fd.fd < 0) {
    return from_errno(errno);
  }
  const errc not_ready_code =
      (timeout && *timeout <= nanoseconds::zero()) ? errc::not_ready : errc::timed_out;

  // The creator sizes the file right after creating it, so the file can briefly be too small to
  // hold even the header.
  struct stat st {};
  const bool sized = wait_until_ready(
      [&] {
        return ::fstat(fd.fd, &st) == 0 &&
               static_cast<std::size_t>(st.st_size) >= sizeof(segment_header);
      },
      timeout);
  if (!sized) {
    return error{not_ready_code};
  }
  const auto size = static_cast<std::size_t>(st.st_size);
  void* base = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd.fd, 0);
  if (base == MAP_FAILED) {
    return from_errno(errno);
  }
  segment seg(base, size, segment_info{});  // unmaps on every early return below

  const bool published = wait_until_ready(
      [&] {
        // Acquire: pairs with the release store in publish().
        return seg.header().state.load(std::memory_order_acquire) ==
               static_cast<std::uint32_t>(init_state::ready);
      },
      timeout);
  if (!published) {
    return error{not_ready_code};
  }
  const result<segment_info> checked = validate_header(base, size, layout);
  if (!checked) {
    return checked.error();
  }
  seg.info_ = checked.value();
  return seg;
}

result<unit> segment::unlink(const std::string& name) {
  if (!valid_name(name)) {
    return error{errc::invalid_argument};
  }
  if (::shm_unlink(name.c_str()) != 0) {
    return from_errno(errno);
  }
  return unit{};
}

segment::segment(segment&& other) noexcept
    : base_(std::exchange(other.base_, nullptr)),
      size_(std::exchange(other.size_, 0)),
      info_(other.info_) {}

segment& segment::operator=(segment&& other) noexcept {
  if (this != &other) {
    if (base_ != nullptr) {
      ::munmap(base_, size_);
    }
    base_ = std::exchange(other.base_, nullptr);
    size_ = std::exchange(other.size_, 0);
    info_ = other.info_;
  }
  return *this;
}

segment::~segment() {
  if (base_ != nullptr) {
    ::munmap(base_, size_);
  }
}

void segment::publish() noexcept {
  // Release: pairs with the acquire loads of the state in open() and validate_header(). It makes
  // the header fields and the queue built in payload() visible before `ready` is.
  header().state.store(static_cast<std::uint32_t>(init_state::ready), std::memory_order_release);
}

}  // namespace shmring
