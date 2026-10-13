// Attaches to the ring made by quickstart_producer and checks the numbers it receives.
#include "shmring/shared_queue.hpp"

#include <cstdint>
#include <cstdio>

int main() {
  constexpr std::uint64_t count = 1000000;
  using Ring = shmring::shared_spsc_ring<std::uint64_t, 1024>;

  // With no timeout, open() waits for the producer to finish creating the ring, but reports
  // not_found right away if the producer has not been started at all.
  auto ring = Ring::open("/shmring_quickstart");
  if (!ring) {
    std::fprintf(stderr,
                 "open failed: %s (start quickstart_producer first)\n",
                 shmring::to_string(ring.error().code));
    return 1;
  }

  std::uint64_t expected = 0;
  while (expected < count) {
    std::uint64_t value = 0;
    if (!ring.value().queue().try_pop(value)) {
      continue;
    }
    if (value != expected) {
      std::fprintf(stderr,
                   "consumer: expected %llu, got %llu\n",
                   static_cast<unsigned long long>(expected),
                   static_cast<unsigned long long>(value));
      return 1;
    }
    ++expected;
  }
  std::printf("consumer: received %llu numbers in order\n", static_cast<unsigned long long>(count));
  return 0;
}
