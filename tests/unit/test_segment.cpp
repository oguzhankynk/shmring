#include "shmring/segment.hpp"
#include "shmring/shared_queue.hpp"
#include "shmring/spsc_msg_ring.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <thread>

#include <fcntl.h>
#include <unistd.h>

#include "support.hpp"

namespace shmring {
namespace {

using std::chrono::milliseconds;
using testing_support::name_cleanup;
using testing_support::unique_name;

constexpr segment_expectation kExpect{queue_kind::spsc_ring, 8, 8, 16};

// A header image in aligned memory, for exercising validation without a real segment.
struct header_image {
  alignas(cache_line) std::byte bytes[sizeof(segment_header) + 2 * cache_line + 128]{};

  header_image() {
    auto* h = new (bytes) segment_header(kExpect, 1);
    h->state.store(static_cast<std::uint32_t>(init_state::ready));
  }
  segment_header& header() {
    return *reinterpret_cast<segment_header*>(bytes);
  }
  result<segment_info> check(std::size_t size = sizeof(bytes), segment_expectation e = kExpect) {
    return validate_header(bytes, size, e);
  }
};

TEST(SegmentValidationTest, AcceptsAWellFormedHeader) {
  header_image img;
  const auto r = img.check();
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r.value().capacity, 16U);
  EXPECT_EQ(r.value().creator_pid, 1U);
  EXPECT_EQ(r.value().kind, queue_kind::spsc_ring);
  EXPECT_EQ(r.value().elem_size, 8U);
  EXPECT_EQ(r.value().elem_align, 8U);
}

TEST(SegmentValidationTest, AnyCapacityIsAcceptedWhenNoneIsExpected) {
  header_image img;
  EXPECT_TRUE(img.check(sizeof(img.bytes), {queue_kind::spsc_ring, 8, 8, 0}).has_value());
}

TEST(SegmentValidationTest, RejectsEachBadField) {
  {
    header_image img;
    img.header().magic ^= 1;
    EXPECT_EQ(img.check().error().code, errc::bad_magic);
  }
  {
    header_image img;
    img.header().layout_version += 1;
    EXPECT_EQ(img.check().error().code, errc::bad_version);
  }
  {
    header_image img;
    img.header().kind = 99;
    EXPECT_EQ(img.check().error().code, errc::bad_kind);
  }
  {
    header_image img;
    img.header().elem_size = 4;
    EXPECT_EQ(img.check().error().code, errc::bad_element_size);
  }
  {
    header_image img;
    img.header().elem_align = 4;
    EXPECT_EQ(img.check().error().code, errc::bad_alignment);
  }
  {
    header_image img;
    img.header().capacity = 12;  // not a power of two
    EXPECT_EQ(img.check().error().code, errc::bad_capacity);
  }
  {
    header_image img;
    img.header().capacity = 0;
    EXPECT_EQ(img.check().error().code, errc::bad_capacity);
  }
  {
    header_image img;
    img.header().capacity = 32;  // a power of two, but not the expected 16
    EXPECT_EQ(img.check().error().code, errc::bad_capacity);
  }
  {
    header_image img;
    img.header().cache_line_size = 32;
    EXPECT_EQ(img.check().error().code, errc::bad_cache_line);
  }
  {
    header_image img;
    img.header().state.store(static_cast<std::uint32_t>(init_state::initializing));
    EXPECT_EQ(img.check().error().code, errc::not_ready);
  }
  {
    header_image img;
    img.header().state.store(static_cast<std::uint32_t>(init_state::uninitialized));
    EXPECT_EQ(img.check().error().code, errc::not_ready);
  }
  {
    header_image img;
    img.header().state.store(0xDEADBEEF);  // not a defined state at all
    EXPECT_EQ(img.check().error().code, errc::not_ready);
  }
}

TEST(SegmentValidationTest, RejectsCapacitiesThatDoNotFitTheMapping) {
  header_image img;
  img.header().capacity = std::uint64_t{1} << 62;
  EXPECT_EQ(img.check(sizeof(img.bytes), {queue_kind::spsc_ring, 8, 8, 0}).error().code,
            errc::bad_capacity);
  img.header().capacity = 1024;
  EXPECT_EQ(img.check(sizeof(img.bytes), {queue_kind::spsc_ring, 8, 8, 0}).error().code,
            errc::too_small);
  EXPECT_EQ(img.check(sizeof(segment_header) - 1).error().code, errc::too_small);
  EXPECT_EQ(img.check(0).error().code, errc::too_small);
}

TEST(SegmentValidationTest, RejectsAMissingOrMisalignedMapping) {
  EXPECT_EQ(validate_header(nullptr, 4096, kExpect).error().code, errc::invalid_argument);
  header_image img;
  EXPECT_EQ(validate_header(img.bytes + 8, sizeof(img.bytes) - 8, kExpect).error().code,
            errc::invalid_argument);
}

TEST(SegmentValidationTest, RejectsAKindWhoseLayoutIsImpossible) {
  header_image img;
  img.header().kind = static_cast<std::uint32_t>(queue_kind::spsc_msg_ring);
  // The header agrees with the expectation, but a message ring cannot have 8-byte elements.
  EXPECT_EQ(img.check(sizeof(img.bytes), {queue_kind::spsc_msg_ring, 8, 8, 16}).error().code,
            errc::bad_capacity);
  // The expectation (1-byte elements) disagrees with the header (8-byte elements).
  EXPECT_EQ(img.check(sizeof(img.bytes), {queue_kind::spsc_msg_ring, 1, 8, 16}).error().code,
            errc::bad_element_size);
}

TEST(PayloadBytesTest, MatchesTheSizeOfEveryQueueType) {
  EXPECT_EQ(payload_bytes(queue_kind::spsc_ring, 8, 64, 8).value(),
            sizeof(spsc_ring<std::uint64_t, 64>));
  EXPECT_EQ(payload_bytes(queue_kind::spsc_ring, 3, 128, 1).value(),
            sizeof(spsc_ring<std::array<char, 3>, 128>));
  EXPECT_EQ(payload_bytes(queue_kind::blocking_spsc_ring, 16, 32, 8).value(),
            (sizeof(blocking_spsc_ring<std::array<std::uint64_t, 2>, 32>)));
  EXPECT_EQ(payload_bytes(queue_kind::mpmc_ring, 16, 32, 8).value(),
            (sizeof(mpmc_ring<std::array<std::uint64_t, 2>, 32>)));
  EXPECT_EQ(payload_bytes(queue_kind::mpmc_ring, 3, 16, 1).value(),
            (sizeof(mpmc_ring<std::array<char, 3>, 16>)));
  EXPECT_EQ(payload_bytes(queue_kind::spmc_ring, 16, 32, 8).value(),
            (sizeof(spmc_ring<std::array<std::uint64_t, 2>, 32>)));
  EXPECT_EQ(payload_bytes(queue_kind::spmc_ring, 3, 16, 1).value(),
            (sizeof(spmc_ring<std::array<char, 3>, 16>)));
  EXPECT_EQ(payload_bytes(queue_kind::spsc_msg_ring, 1, 256, 1).value(),
            spsc_msg_ring::required_bytes(256));
}

TEST(PayloadBytesTest, RejectsImpossibleLayouts) {
  EXPECT_FALSE(payload_bytes(queue_kind::spsc_ring, 8, 0, 8));   // no capacity
  EXPECT_FALSE(payload_bytes(queue_kind::spsc_ring, 0, 16, 1));  // empty elements
  EXPECT_FALSE(payload_bytes(queue_kind::spsc_ring, 8, 16, 0));  // zero alignment
  EXPECT_FALSE(payload_bytes(queue_kind::spsc_ring, 8, 16, 3));  // alignment not a power of two
  EXPECT_FALSE(payload_bytes(queue_kind::spsc_ring, 8, 16, cache_line * 2));  // over-aligned
  EXPECT_FALSE(payload_bytes(queue_kind::spsc_ring, 8, std::uint64_t{1} << 41, 8));
  EXPECT_FALSE(
      payload_bytes(queue_kind::spsc_ring, std::uint64_t{1} << 40, std::uint64_t{1} << 40, 8));
  EXPECT_FALSE(payload_bytes(static_cast<queue_kind>(77), 8, 16, 8));
  EXPECT_FALSE(
      payload_bytes(queue_kind::spsc_msg_ring, 2, 256, 1));  // message ring elements are bytes
  EXPECT_FALSE(payload_bytes(queue_kind::spsc_msg_ring, 1, 32, 1));  // below the minimum capacity
  EXPECT_FALSE(payload_bytes(queue_kind::spmc_ring, 8, 1, 8));       // needs at least two slots
}

TEST(SegmentTest, CreateOpenRoundTripAndUnlink) {
  const name_cleanup name(unique_name("rt"));
  auto created = shared_spsc_ring<std::uint64_t, 16>::create(name.name());
  ASSERT_TRUE(created.has_value());
  auto opened = shared_spsc_ring<std::uint64_t, 16>::open(name.name());
  ASSERT_TRUE(opened.has_value());

  EXPECT_TRUE(created.value().queue().try_push(42));
  std::uint64_t v = 0;
  ASSERT_TRUE(opened.value().queue().try_pop(v));
  EXPECT_EQ(v, 42U);

  EXPECT_EQ((shared_spsc_ring<std::uint64_t, 16>::create(name.name()).error().code),
            errc::already_exists);
  EXPECT_EQ((shared_spsc_ring<std::uint64_t, 32>::open(name.name()).error().code),
            errc::bad_capacity);
  EXPECT_EQ((shared_spsc_ring<std::uint32_t, 16>::open(name.name()).error().code),
            errc::bad_element_size);
  EXPECT_EQ((shared_mpmc_ring<std::uint64_t, 16>::open(name.name()).error().code), errc::bad_kind);

  ASSERT_TRUE(segment::unlink(name.name()).has_value());
  EXPECT_EQ((shared_spsc_ring<std::uint64_t, 16>::open(name.name()).error().code), errc::not_found);
  EXPECT_EQ(segment::unlink(name.name()).error().code, errc::not_found);
}

TEST(SegmentTest, RejectsBadNamesAndLayouts) {
  EXPECT_EQ(segment::create("no_slash", kExpect).error().code, errc::invalid_argument);
  EXPECT_EQ(segment::create("/", kExpect).error().code, errc::invalid_argument);
  EXPECT_EQ(segment::open("", kExpect).error().code, errc::invalid_argument);
  EXPECT_EQ(segment::unlink("nope").error().code, errc::invalid_argument);
  EXPECT_EQ(segment::create("/" + std::string(300, 'a'), kExpect).error().code,
            errc::invalid_argument);

  const name_cleanup name(unique_name("layout"));
  EXPECT_EQ(segment::create(name.name(), {queue_kind::spsc_ring, 8, 8, 12}).error().code,
            errc::bad_capacity);
  EXPECT_EQ(segment::create(name.name(), {queue_kind::spsc_ring, 8, 8, 0}).error().code,
            errc::bad_capacity);
  EXPECT_EQ(segment::create(name.name(), {static_cast<queue_kind>(77), 8, 8, 16}).error().code,
            errc::bad_capacity);
  EXPECT_EQ(segment::unlink(name.name()).error().code, errc::not_found);  // nothing was left behind
}

TEST(SegmentTest, TheCreatorsPermissionsApply) {
  if (::geteuid() == 0) {
    GTEST_SKIP() << "root ignores permission bits";
  }
  const name_cleanup name(unique_name("perm"));
  auto created = segment::create(name.name(), kExpect, 0);
  ASSERT_TRUE(created.has_value());
  EXPECT_EQ(segment::open(name.name(), kExpect, milliseconds(0)).error().code,
            errc::permission_denied);
}

TEST(SegmentTest, OpenTimesOutOnAnUnpublishedSegment) {
  const name_cleanup name(unique_name("unpub"));
  auto created = segment::create(name.name(), kExpect);
  ASSERT_TRUE(created.has_value());

  EXPECT_EQ(segment::open(name.name(), kExpect, std::chrono::nanoseconds::zero()).error().code,
            errc::not_ready);
  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(segment::open(name.name(), kExpect, milliseconds(50)).error().code, errc::timed_out);
  EXPECT_GE(std::chrono::steady_clock::now() - start, milliseconds(50));

  created.value().publish();
  EXPECT_TRUE(segment::open(name.name(), kExpect, milliseconds(50)).has_value());
}

TEST(SegmentTest, OpenWaitsForTheCreatorToPublish) {
  const name_cleanup name(unique_name("wait"));
  auto created = segment::create(name.name(), kExpect);
  ASSERT_TRUE(created.has_value());
  std::thread publisher([&] {
    std::this_thread::sleep_for(milliseconds(60));
    created.value().publish();
  });
  const auto start = std::chrono::steady_clock::now();
  auto opened = segment::open(name.name(), kExpect);  // default: wait without limit
  const auto waited = std::chrono::steady_clock::now() - start;
  publisher.join();
  ASSERT_TRUE(opened.has_value());
  EXPECT_GE(waited, milliseconds(50));
}

TEST(SegmentTest, OpenTreatsAFileSmallerThanTheHeaderAsNotReady) {
  const name_cleanup name(unique_name("tiny"));
  const int fd = ::shm_open(name.name().c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::ftruncate(fd, 10), 0);
  EXPECT_EQ(segment::open(name.name(), kExpect, std::chrono::nanoseconds::zero()).error().code,
            errc::not_ready);
  EXPECT_EQ(segment::open(name.name(), kExpect, milliseconds(20)).error().code, errc::timed_out);
  ::close(fd);
}

TEST(SegmentTest, OpenReportsEveryValidationError) {
  const name_cleanup name(unique_name("bad"));
  auto created = segment::create(name.name(), kExpect);
  ASSERT_TRUE(created.has_value());
  created.value().publish();
  segment_header& h = created.value().header();
  const auto open_code = [&] {
    return segment::open(name.name(), kExpect, milliseconds(0)).error().code;
  };

  h.magic ^= 1;
  EXPECT_EQ(open_code(), errc::bad_magic);
  h.magic ^= 1;
  h.layout_version = 9;
  EXPECT_EQ(open_code(), errc::bad_version);
  h.layout_version = segment_layout_version;
  h.kind = 3;
  EXPECT_EQ(open_code(), errc::bad_kind);
  h.kind = static_cast<std::uint32_t>(queue_kind::spsc_ring);
  EXPECT_TRUE(segment::open(name.name(), kExpect, milliseconds(0)).has_value());
}

TEST(SegmentTest, ExposesItsMappingAndValidatedInfo) {
  const name_cleanup name(unique_name("info"));
  auto created = segment::create(name.name(), kExpect);
  ASSERT_TRUE(created.has_value());
  segment& seg = created.value();
  EXPECT_EQ(seg.size(),
            sizeof(segment_header) + payload_bytes(queue_kind::spsc_ring, 8, 16, 8).value());
  EXPECT_NE(seg.data(), nullptr);
  EXPECT_EQ(static_cast<std::byte*>(seg.payload()) - static_cast<std::byte*>(seg.data()),
            static_cast<std::ptrdiff_t>(sizeof(segment_header)));
  EXPECT_EQ(seg.info().capacity, 16U);
  EXPECT_EQ(seg.info().creator_pid, static_cast<std::uint32_t>(::getpid()));
  EXPECT_EQ(seg.header().state.load(), static_cast<std::uint32_t>(init_state::initializing));
  seg.publish();
  EXPECT_EQ(seg.header().state.load(), static_cast<std::uint32_t>(init_state::ready));
}

TEST(SegmentTest, MovingTransfersTheMapping) {
  const name_cleanup first_name(unique_name("mv1"));
  const name_cleanup second_name(unique_name("mv2"));
  auto first = segment::create(first_name.name(), kExpect);
  auto second = segment::create(second_name.name(), {queue_kind::spsc_ring, 8, 8, 32});
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());

  void* first_base = first.value().data();
  segment moved(std::move(first).value());
  EXPECT_EQ(moved.data(), first_base);
  EXPECT_EQ(moved.info().capacity, 16U);

  void* second_base = second.value().data();
  moved = std::move(second).value();  // releases the first mapping
  EXPECT_EQ(moved.data(), second_base);
  EXPECT_EQ(moved.info().capacity, 32U);

  segment& alias = moved;
  moved = std::move(alias);  // self-move leaves the mapping alone
  EXPECT_EQ(moved.data(), second_base);
}

struct Message {
  std::uint64_t seq;
  std::uint64_t checksum;
};

TEST(SegmentTest, ChildProcessTransfersMessagesThroughSharedMemory) {
  constexpr std::uint64_t total = 100000;
  const name_cleanup name(unique_name("fork"));
  auto created = shared_spsc_ring<Message, 256>::create(name.name());
  ASSERT_TRUE(created.has_value());

  const pid_t pid = ::fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    auto producer = shared_spsc_ring<Message, 256>::open(name.name());
    if (!producer) {
      ::_exit(2);
    }
    for (std::uint64_t i = 0; i < total; ++i) {
      const Message m{i, ~i};
      while (!producer.value().queue().try_push(m)) {
      }
    }
    ::_exit(0);
  }

  std::uint64_t bad = 0;
  for (std::uint64_t expected = 0; expected < total;) {
    Message m{};
    if (!created.value().queue().try_pop(m)) {
      continue;
    }
    if (m.seq != expected || m.checksum != ~expected) {
      ++bad;
    }
    ++expected;
  }
  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  EXPECT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
  EXPECT_EQ(bad, 0U);
}

// Runs `body` in a child process and returns its exit code. Used to change resource limits without
// affecting the rest of the test run.
int run_in_child(const std::function<int()>& body) {
  const pid_t pid = ::fork();
  if (pid < 0) {
    return -1;
  }
  if (pid == 0) {
    ::_exit(body());
  }
  int status = 0;
  ::waitpid(pid, &status, 0);
  return WIFEXITED(status) ? WEXITSTATUS(status) : -2;
}

void limit(int resource, rlim_t value) {
  const rlimit lim{value, value};
  ::setrlimit(resource, &lim);
}

// Allows this process only `headroom` more bytes of address space than it uses now. A fixed small
// limit would also starve the allocators of the sanitizers, which reserve a lot of address space.
void limit_address_space_to_current_plus(std::size_t headroom) {
  std::ifstream status("/proc/self/status");
  std::string line;
  while (std::getline(status, line)) {
    if (line.rfind("VmSize:", 0) == 0) {
      const std::size_t used_kib = std::stoul(line.substr(7));
      limit(RLIMIT_AS, static_cast<rlim_t>(used_kib) * 1024 + headroom);
      return;
    }
  }
}

TEST(SegmentFailureTest, CreateReportsAFileThatCannotBeSized) {
  const name_cleanup name(unique_name("fsize"));
  const int code = run_in_child([&] {
    ::signal(SIGXFSZ, SIG_IGN);
    limit(RLIMIT_FSIZE, 4096);
    auto created = segment::create(name.name(), {queue_kind::spsc_ring, 8, 8, 1024});
    if (created.has_value() || created.error().code != errc::system_error) {
      return 1;
    }
    return segment::unlink(name.name()).has_value() ? 2
                                                    : 0;  // the failed create left nothing behind
  });
  EXPECT_EQ(code, 0);
}

TEST(SegmentFailureTest, CreateReportsAMappingThatCannotBeMade) {
  const name_cleanup name(unique_name("map"));
  const int code = run_in_child([&] {
    limit_address_space_to_current_plus(32ULL << 20);
    auto created =
        segment::create(name.name(), {queue_kind::spsc_ring, 8, 8, std::uint64_t{1} << 24});
    if (created.has_value() || created.error().code != errc::system_error) {
      return 1;
    }
    return segment::unlink(name.name()).has_value() ? 2 : 0;
  });
  EXPECT_EQ(code, 0);
}

TEST(SegmentFailureTest, OpenReportsAMappingThatCannotBeMade) {
  const name_cleanup name(unique_name("openmap"));
  const segment_expectation big{queue_kind::spsc_ring, 8, 8, std::uint64_t{1} << 24};
  auto created = segment::create(name.name(), big);
  ASSERT_TRUE(created.has_value());
  created.value().publish();
  const int code = run_in_child([&] {
    limit_address_space_to_current_plus(32ULL << 20);
    auto opened = segment::open(name.name(), big);
    return (!opened.has_value() && opened.error().code == errc::system_error) ? 0 : 1;
  });
  EXPECT_EQ(code, 0);
}

TEST(SegmentFailureTest, OtherSystemErrorsAreReportedAsSystemError) {
  const name_cleanup name(unique_name("nofile"));
  const int code = run_in_child([&] {
    limit(RLIMIT_NOFILE, 0);  // no new file descriptors
    auto created = segment::create(name.name(), kExpect);
    if (created.has_value() || created.error().code != errc::system_error ||
        created.error().sys_errno != EMFILE) {
      return 1;
    }
    return 0;
  });
  EXPECT_EQ(code, 0);
}

}  // namespace
}  // namespace shmring
