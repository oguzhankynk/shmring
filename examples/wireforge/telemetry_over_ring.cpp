// Sends WireForge-generated telemetry frames from a child process to its parent through an
// spsc_msg_ring. The sender serializes each frame directly into the ring's memory (no staging
// buffer); the receiver feeds the ring's messages to the generated frame decoder.
#include "shmring/shared_spsc_msg_ring.hpp"

#include <cstdint>
#include <cstdio>
#include <string>
#include <sys/wait.h>

#include <unistd.h>

#include "basic_telemetry/interfaces/sensor_to_us.hpp"
#include "basic_telemetry/model.hpp"
#include "wireforge/span.hpp"
#include "wireforge/status.hpp"

namespace {

namespace telemetry = demo::telemetry;
namespace link = telemetry::sensor_to_us;

constexpr std::uint8_t kFrames = 20;

// WireForge reads bytes through a transport with send() and receive(). This one hands over the
// next message of the ring; sending is not used by this example.
class RingReceiver {
public:
  explicit RingReceiver(shmring::spsc_msg_ring& ring) noexcept : ring_(ring) {}

  std::size_t send(wireforge::Span<const std::uint8_t>) noexcept {
    return 0;
  }

  std::size_t receive(wireforge::Span<std::uint8_t> buffer) noexcept {
    const shmring::span<const std::byte> message = ring_.peek();
    if (!message || message.size() > buffer.size()) {
      return 0;
    }
    for (std::size_t i = 0; i < message.size(); ++i) {
      buffer[i] = static_cast<std::uint8_t>(message.data()[i]);
    }
    const std::size_t size = message.size();
    ring_.release();
    return size;
  }

private:
  shmring::spsc_msg_ring& ring_;
};

class Application final : public link::Handler {
public:
  void onSensorReadingReceived(const telemetry::SensorReading& reading) noexcept override {
    std::printf("receiver: sensor %u, %.1f degC, %zu samples\n",
                static_cast<unsigned>(reading.sensorId),
                static_cast<double>(reading.temperature),
                reading.samples.size());
    ++received_;
  }
  unsigned received() const noexcept {
    return received_;
  }

private:
  unsigned received_ = 0;
};

int send_frames(const std::string& name) {
  auto opened = shmring::shared_spsc_msg_ring::open(name);
  if (!opened) {
    return 1;
  }
  shmring::spsc_msg_ring& ring = opened.value().ring();
  for (std::uint8_t i = 0; i < kFrames; ++i) {
    telemetry::SensorReading reading{};
    reading.sensorId = static_cast<std::uint8_t>(1 + i % 32);
    reading.temperature = 20.0F + static_cast<float>(i);
    for (std::uint8_t s = 0; s < i % 5 && s < 4; ++s) {
      static_cast<void>(reading.samples.push_back(static_cast<std::uint16_t>(100 * s)));
    }
    // Reserve room for the largest frame, serialize into it, and commit only the bytes used.
    shmring::span<std::byte> room = ring.reserve(link::kMaxFrameSize);
    while (!room) {
      room = ring.reserve(link::kMaxFrameSize);
    }
    std::size_t size = 0;
    const wireforge::Status status = link::encode_frame(
        reading,
        wireforge::Span<std::uint8_t>(reinterpret_cast<std::uint8_t*>(room.data()), room.size()),
        size);
    if (status != wireforge::Status::Ok) {
      return 2;
    }
    ring.commit(size);
  }
  return 0;
}

}  // namespace

int main() {
  const std::string name = "/shmring_wireforge_demo_" + std::to_string(::getpid());
  auto created = shmring::shared_spsc_msg_ring::create(name, 4096);
  if (!created) {
    std::fprintf(stderr, "create failed: %s\n", shmring::to_string(created.error().code));
    return 1;
  }

  const pid_t child = ::fork();
  if (child == 0) {
    ::_exit(send_frames(name));
  }

  Application application;
  link::Endpoint endpoint(application);
  RingReceiver transport(created.value().ring());
  link::Link<RingReceiver> channel(transport, endpoint.decoder(), endpoint.dispatcher());
  while (application.received() < kFrames) {
    channel.poll();
  }
  int status = 0;
  ::waitpid(child, &status, 0);
  (void)shmring::shared_spsc_msg_ring::unlink(name);
  const bool ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;
  std::printf(ok ? "all %u frames received\n" : "sender failed\n", application.received());
  return ok ? 0 : 1;
}
