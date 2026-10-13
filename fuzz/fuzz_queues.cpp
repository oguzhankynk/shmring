// libFuzzer target: a queue built on arbitrary bytes (as if another process had scribbled over the
// shared memory) must never read or write outside its own memory, whatever operations follow.
//
// The mpmc_ring is not fuzzed: its protocol trusts every participant, and a participant that
// writes inconsistent sequence numbers can make the others retry forever. That is a hang, not an
// out-of-bounds access.
#include "shmring/spmc_ring.hpp"
#include "shmring/spsc_msg_ring.hpp"
#include "shmring/spsc_ring.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>

namespace {

using namespace shmring;

struct aligned_free {
  void operator()(void* p) const noexcept {
    ::operator delete (p, std::align_val_t{cache_line});
  }
};

// Memory of exactly `bytes` bytes, so that AddressSanitizer flags any access beyond it.
std::unique_ptr<void, aligned_free> make_image(std::size_t bytes,
                                               const std::uint8_t* src,
                                               std::size_t available) {
  std::unique_ptr<void, aligned_free> image(::operator new (bytes, std::align_val_t{cache_line}));
  std::memset(image.get(), 0, bytes);
  std::memcpy(image.get(), src, available < bytes ? available : bytes);
  return image;
}

constexpr std::size_t kProgramBytes = 32;

void run_spsc(const std::uint8_t* program, const std::uint8_t* rest, std::size_t rest_size) {
  using ring_type = spsc_ring<std::uint64_t, 16>;
  auto image = make_image(sizeof(ring_type), rest, rest_size);
  auto& ring = *static_cast<ring_type*>(image.get());
  std::uint64_t buffer[32] = {};
  for (std::size_t i = 0; i < kProgramBytes; ++i) {
    const std::size_t n = program[i] >> 3;
    switch (program[i] & 7) {
      case 0:
        ring.try_push(program[i]);
        break;
      case 1:
        ring.try_pop(buffer[0]);
        break;
      case 2:
        ring.try_push_n(buffer, n);
        break;
      case 3:
        ring.try_pop_n(buffer, n);
        break;
      default:
        (void)ring.size_approx();
        break;
    }
  }
}

void run_message_ring(const std::uint8_t* program,
                      const std::uint8_t* rest,
                      std::size_t rest_size) {
  const std::size_t capacity = std::size_t{64} << (program[0] & 3);  // 64, 128, 256 or 512
  auto image = make_image(spsc_msg_ring::required_bytes(capacity), rest, rest_size);
  spsc_msg_ring producer = spsc_msg_ring::attach(image.get(), capacity);
  spsc_msg_ring consumer = spsc_msg_ring::attach(image.get(), capacity);
  unsigned char payload[512] = {};
  for (std::size_t i = 0; i < kProgramBytes; ++i) {
    const std::size_t n = static_cast<std::size_t>(program[i] >> 3) * 4;
    switch (program[i] & 7) {
      case 0: {
        auto room = producer.reserve(n);
        if (room && room.size() != 0)
          std::memset(room.data(), 0xAB, room.size());
        break;
      }
      case 1:
        producer.commit();
        break;
      case 2:
        producer.commit(n);
        break;
      case 3:
        producer.try_push(payload, n);
        break;
      case 4: {
        auto message = consumer.peek();
        // Touch every byte of the view: it must lie inside the ring.
        unsigned sum = 0;
        for (std::size_t b = 0; b < message.size(); ++b)
          sum += static_cast<unsigned char>(message.data()[b]);
        payload[0] = static_cast<unsigned char>(sum);
        break;
      }
      case 5:
        consumer.release();
        break;
      default:
        (void)producer.size_approx();
        break;
    }
  }
}

void run_broadcast(const std::uint8_t* program, const std::uint8_t* rest, std::size_t rest_size) {
  using ring_type = spmc_ring<std::array<std::uint64_t, 3>, 8>;
  auto image = make_image(sizeof(ring_type), rest, rest_size);
  auto& ring = *static_cast<ring_type*>(image.get());
  spmc_reader a = ring.join_latest();
  spmc_reader b = ring.join_oldest();
  std::array<std::uint64_t, 3> value{};
  for (std::size_t i = 0; i < kProgramBytes; ++i) {
    switch (program[i] & 3) {
      case 0:
        ring.publish(value);
        break;
      case 1:
        ring.try_read(a, value);
        break;
      case 2:
        ring.try_read(b, value);
        break;
      default:
        (void)ring.published();
        break;
    }
  }
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size < kProgramBytes + 1) {
    return 0;
  }
  const std::uint8_t selector = data[0];
  const std::uint8_t* program = data + 1;
  const std::uint8_t* rest = data + 1 + kProgramBytes;
  const std::size_t rest_size = size - 1 - kProgramBytes;
  switch (selector % 3) {
    case 0:
      run_spsc(program, rest, rest_size);
      break;
    case 1:
      run_message_ring(program, rest, rest_size);
      break;
    default:
      run_broadcast(program, rest, rest_size);
      break;
  }
  return 0;
}
