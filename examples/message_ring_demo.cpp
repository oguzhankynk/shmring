// Sends variable-length text messages from a child process to its parent through a message ring,
// writing and reading them in place without intermediate copies.
#include "shmring/shared_spsc_msg_ring.hpp"

#include <cstdio>
#include <cstring>
#include <string>
#include <sys/wait.h>

#include <unistd.h>

int main() {
  const std::string name = "/shmring_message_demo_" + std::to_string(::getpid());
  auto parent = shmring::shared_spsc_msg_ring::create(name, 4096);
  if (!parent) {
    std::fprintf(stderr, "create failed: %s\n", shmring::to_string(parent.error().code));
    return 1;
  }

  const pid_t child = ::fork();
  if (child == 0) {
    auto ring = shmring::shared_spsc_msg_ring::open(name);
    if (!ring) {
      ::_exit(1);
    }
    for (int i = 0; i < 5; ++i) {
      char text[64];
      const int length = std::snprintf(text, sizeof(text), "message %d of %d", i + 1, 5);
      // reserve() hands out room inside the ring; nothing is visible until commit().
      shmring::span<std::byte> room = ring.value().ring().reserve(static_cast<std::size_t>(length));
      while (!room) {
        room = ring.value().ring().reserve(static_cast<std::size_t>(length));
      }
      std::memcpy(room.data(), text, static_cast<std::size_t>(length));
      ring.value().ring().commit();
    }
    ::_exit(0);
  }

  for (int received = 0; received < 5;) {
    shmring::span<const std::byte> message = parent.value().ring().peek();
    if (!message) {
      continue;
    }
    std::printf("parent got: %.*s\n",
                static_cast<int>(message.size()),
                reinterpret_cast<const char*>(message.data()));
    parent.value().ring().release();
    ++received;
  }
  int status = 0;
  ::waitpid(child, &status, 0);
  (void)shmring::shared_spsc_msg_ring::unlink(name);
  return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 1;
}
