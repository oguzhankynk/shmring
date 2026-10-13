// Creates a shared-memory ring and sends the numbers 0..N-1 to quickstart_consumer.
//
//   ./quickstart_producer          (terminal 1)
//   ./quickstart_consumer          (terminal 2)
#include "shmring/shared_queue.hpp"

#include <cstdint>
#include <cstdio>

int main() {
  constexpr std::uint64_t count = 1000000;
  using Ring = shmring::shared_spsc_ring<std::uint64_t, 1024>;

  auto ring = Ring::create("/shmring_quickstart");
  if (!ring) {
    std::fprintf(stderr,
                 "create failed: %s (errno %d)\n",
                 shmring::to_string(ring.error().code),
                 ring.error().sys_errno);
    return 1;
  }
  std::printf("producer: ring created, waiting for the consumer to start\n");

  for (std::uint64_t i = 0; i < count; ++i) {
    while (!ring.value().queue().try_push(i)) {
      // The ring is full: the consumer has not caught up yet.
    }
  }
  std::printf("producer: sent %llu numbers\n", static_cast<unsigned long long>(count));

  // Give the consumer time to drain the ring before the name disappears.
  while (ring.value().queue().size_approx() != 0) {
  }
  (void)Ring::unlink("/shmring_quickstart");
  return 0;
}
