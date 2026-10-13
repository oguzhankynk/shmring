// Helpers shared by the test executables.
#ifndef SHMRING_TESTS_SUPPORT_HPP
#define SHMRING_TESTS_SUPPORT_HPP

#include "shmring/config.hpp"
#include "shmring/segment.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <utility>

#include <unistd.h>

namespace shmring::testing_support {

// A name for a segment that no other test or process uses.
inline std::string unique_name(const char* tag) {
  static std::atomic<int> counter{0};
  return std::string("/shmring_test_") + tag + "_" + std::to_string(::getpid()) + "_" +
         std::to_string(counter++);
}

// Removes a segment name when the test ends, whatever happens.
class name_cleanup {
public:
  explicit name_cleanup(std::string name) : name_(std::move(name)) {}
  name_cleanup(const name_cleanup&) = delete;
  name_cleanup& operator=(const name_cleanup&) = delete;
  name_cleanup(name_cleanup&&) = delete;
  name_cleanup& operator=(name_cleanup&&) = delete;
  ~name_cleanup() {
    (void)segment::unlink(name_);
  }
  const std::string& name() const {
    return name_;
  }

private:
  std::string name_;
};

// Memory aligned to the cache line, freed on destruction.
class aligned_buffer {
public:
  explicit aligned_buffer(std::size_t bytes)
      : ptr_(std::aligned_alloc(cache_line, (bytes + cache_line - 1) / cache_line * cache_line)) {}
  aligned_buffer(const aligned_buffer&) = delete;
  aligned_buffer& operator=(const aligned_buffer&) = delete;
  aligned_buffer(aligned_buffer&&) = delete;
  aligned_buffer& operator=(aligned_buffer&&) = delete;
  ~aligned_buffer() {
    std::free(ptr_);
  }
  void* get() const {
    return ptr_;
  }

private:
  void* ptr_;
};

// Overwrites a 64-bit word inside an object. Used to simulate memory corrupted by another party.
template <typename Object>
void poke_u64(Object& object, std::size_t byte_offset, std::uint64_t value) {
  std::memcpy(reinterpret_cast<unsigned char*>(&object) + byte_offset, &value, sizeof(value));
}

// Same for a 32-bit word.
template <typename Object>
void poke_u32(Object& object, std::size_t byte_offset, std::uint32_t value) {
  std::memcpy(reinterpret_cast<unsigned char*>(&object) + byte_offset, &value, sizeof(value));
}

inline std::uint64_t checksum_of(std::uint64_t seq) {
  return seq * 0x9E3779B97F4A7C15ULL + 0x1234567ULL;
}

}  // namespace shmring::testing_support

#endif  // SHMRING_TESTS_SUPPORT_HPP
