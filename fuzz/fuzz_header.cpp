// libFuzzer target: random segment bytes must never make header validation crash or read outside
// the mapping.
#include "shmring/segment_header.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>

namespace {

struct aligned_free {
  void operator()(void* p) const noexcept {
    ::operator delete (p, std::align_val_t{shmring::cache_line});
  }
};

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  // Allocate exactly `size` bytes so AddressSanitizer flags any read past the end.
  std::unique_ptr<void, aligned_free> buffer(
      ::operator new (size == 0 ? 1 : size, std::align_val_t{shmring::cache_line}));
  if (size != 0) {
    std::memcpy(buffer.get(), data, size);
  }

  using shmring::queue_kind;
  using shmring::segment_expectation;
  const segment_expectation expectations[] = {
      {queue_kind::spsc_ring, 8, 8, 0},
      {queue_kind::spsc_ring, 8, 8, 1024},
      {queue_kind::spsc_ring, 64, 8, 0},
      {queue_kind::spsc_msg_ring, 1, 1, 0},
  };
  for (const segment_expectation& want : expectations) {
    (void)shmring::validate_header(buffer.get(), size, want);
  }
  return 0;
}
