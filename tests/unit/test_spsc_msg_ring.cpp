#include "shmring/shared_queue.hpp"
#include "shmring/shared_spsc_msg_ring.hpp"
#include "shmring/spsc_msg_ring.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <sys/wait.h>
#include <vector>

#include <unistd.h>

#include "support.hpp"

namespace shmring {
namespace {

using testing_support::aligned_buffer;

std::string text(span<const std::byte> s) {
  return std::string(reinterpret_cast<const char*>(s.data()), s.size());
}

TEST(SpscMsgRingTest, RoundTripsMessagesInPlace) {
  aligned_buffer mem(spsc_msg_ring::required_bytes(256));
  spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), 256);
  EXPECT_FALSE(ring.peek());
  ASSERT_TRUE(ring.try_push("hello", 5));
  ASSERT_TRUE(ring.try_push("", 0));
  ASSERT_TRUE(ring.try_push("world!!", 7));

  auto m = ring.peek();
  ASSERT_TRUE(m);
  EXPECT_EQ(text(m), "hello");
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m.data()) % 8, 0U);
  EXPECT_EQ(text(ring.peek()), "hello");  // peek again returns the same message
  ring.release();
  m = ring.peek();
  ASSERT_TRUE(m);
  EXPECT_EQ(m.size(), 0U);
  ring.release();
  EXPECT_EQ(text(ring.peek()), "world!!");
  ring.release();
  EXPECT_FALSE(ring.peek());
}

TEST(SpscMsgRingTest, ReserveIsInvisibleUntilCommit) {
  aligned_buffer mem(spsc_msg_ring::required_bytes(128));
  spsc_msg_ring producer = spsc_msg_ring::init(mem.get(), 128);
  spsc_msg_ring consumer = spsc_msg_ring::attach(mem.get(), 128);
  auto w = producer.reserve(10);
  ASSERT_TRUE(w);
  std::memcpy(w.data(), "0123456789", 10);
  EXPECT_FALSE(consumer.peek());
  producer.commit(4);  // publish only the first four bytes
  EXPECT_EQ(text(consumer.peek()), "0123");
}

TEST(SpscMsgRingTest, RejectsOversizedAndFullWithoutLoss) {
  aligned_buffer mem(spsc_msg_ring::required_bytes(64));
  spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), 64);
  EXPECT_EQ(ring.max_message_size(), 24U);
  EXPECT_FALSE(ring.reserve(25));
  const char payload[24] = "abcdefghijklmnopqrstuvw";
  EXPECT_TRUE(ring.try_push(payload, 24));  // 32 bytes
  EXPECT_TRUE(ring.try_push(payload, 24));  // 32 bytes, now full
  EXPECT_FALSE(ring.try_push(payload, 1));
  ring.peek();
  ring.release();
  EXPECT_TRUE(ring.try_push(payload, 1));
}

TEST(SpscMsgRingTest, RecordsNeverSplitAcrossTheWrapPoint) {
  aligned_buffer mem(spsc_msg_ring::required_bytes(128));
  spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), 128);
  std::uint64_t next_in = 0;
  std::uint64_t next_out = 0;
  for (int round = 0; round < 2000; ++round) {
    const std::size_t len = 1 + static_cast<std::size_t>(round * 7 % 40);
    std::string msg(len, static_cast<char>('a' + next_in % 26));
    while (!ring.try_push(msg.data(), len)) {
      auto m = ring.peek();
      ASSERT_TRUE(m);
      const std::size_t got = m.size();
      EXPECT_EQ(text(m), std::string(got, static_cast<char>('a' + next_out % 26)));
      EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m.data()) % 8, 0U);
      ring.release();
      ++next_out;
    }
    ++next_in;
  }
}

TEST(SpscMsgRingTest, RequiredBytesRejectsUnsupportedCapacities) {
  EXPECT_EQ(spsc_msg_ring::required_bytes(100), 0U);
  EXPECT_EQ(spsc_msg_ring::required_bytes(32), 0U);
  EXPECT_EQ(spsc_msg_ring::required_bytes(64), 2 * cache_line + 64);
}

TEST(SpscMsgRingTest, ChildProcessSendsVariableLengthMessagesThroughASegment) {
  constexpr std::uint32_t total = 20000;
  const std::string name = "/shmring_test_msg_" + std::to_string(::getpid());
  auto created = shared_spsc_msg_ring::create(name, 1024);
  ASSERT_TRUE(created.has_value());

  const pid_t pid = ::fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    auto producer = shared_spsc_msg_ring::open(name);
    if (!producer) {
      ::_exit(2);
    }
    for (std::uint32_t i = 0; i < total; ++i) {
      const std::size_t len = 4 + (i % 50);
      for (;;) {
        auto w = producer.value().ring().reserve(len);
        if (!w) {
          continue;
        }
        std::memset(w.data(), static_cast<int>(i & 0xFF), len);
        std::memcpy(w.data(), &i, sizeof(i));
        producer.value().ring().commit();
        break;
      }
    }
    ::_exit(0);
  }

  std::uint32_t bad = 0;
  for (std::uint32_t expected = 0; expected < total;) {
    auto m = created.value().ring().peek();
    if (!m) {
      continue;
    }
    std::uint32_t seq = 0;
    std::memcpy(&seq, m.data(), sizeof(seq));
    bool ok = seq == expected && m.size() == 4 + (expected % 50);
    for (std::size_t i = 4; i < m.size(); ++i) {
      ok = ok && static_cast<std::uint8_t>(m.data()[i]) == (expected & 0xFF);
    }
    bad += ok ? 0U : 1U;
    created.value().ring().release();
    ++expected;
  }
  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  EXPECT_EQ(WEXITSTATUS(status), 0);
  EXPECT_EQ(bad, 0U);
  (void)shared_spsc_msg_ring::unlink(name);
}

TEST(SpscMsgRingTest, TheLargestMessageFitsAtEveryWritePosition) {
  for (const std::size_t capacity : {std::size_t{64}, std::size_t{128}, std::size_t{256}}) {
    for (std::size_t position = 0; position < capacity; position += 8) {
      aligned_buffer mem(spsc_msg_ring::required_bytes(capacity));
      spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), capacity);
      // Move the cursors to `position` with empty messages (8 bytes each).
      for (std::size_t moved = 0; moved < position; moved += 8) {
        ASSERT_TRUE(ring.try_push("", 0));
        ASSERT_TRUE(ring.peek());
        ring.release();
      }
      const std::size_t length = ring.max_message_size();
      auto room = ring.reserve(length);
      ASSERT_TRUE(room) << "capacity " << capacity << " position " << position;
      std::memset(room.data(), 0x5A, length);
      ring.commit();
      auto msg = ring.peek();
      ASSERT_TRUE(msg);
      EXPECT_EQ(msg.size(), length);
      EXPECT_EQ(reinterpret_cast<std::uintptr_t>(msg.data()) % 8, 0U);
      EXPECT_EQ(static_cast<unsigned char>(msg.data()[length - 1]), 0x5A);
      ring.release();
      EXPECT_FALSE(ring.peek());
    }
  }
}

TEST(SpscMsgRingTest, ARecordMayEndExactlyAtTheEndOfTheBuffer) {
  aligned_buffer mem(spsc_msg_ring::required_bytes(64));
  spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), 64);
  const char data[24] = "0123456789012345678901";
  for (int lap = 0; lap < 5; ++lap) {
    ASSERT_TRUE(ring.try_push(data, 24));  // 32 bytes: first half
    ASSERT_TRUE(ring.try_push(data, 24));  // 32 bytes: ends exactly at the buffer end
    EXPECT_FALSE(ring.try_push(data, 1));
    for (int i = 0; i < 2; ++i) {
      auto msg = ring.peek();
      ASSERT_TRUE(msg);
      EXPECT_EQ(msg.size(), 24U);
      ring.release();
    }
  }
}

TEST(SpscMsgRingTest, ASkipRecordCanBeAsSmallAsOneHeader) {
  aligned_buffer mem(spsc_msg_ring::required_bytes(64));
  spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), 64);
  // Seven empty messages leave exactly one header's worth (8 bytes) before the end.
  for (int i = 0; i < 7; ++i) {
    ASSERT_TRUE(ring.try_push("", 0));
    ASSERT_TRUE(ring.peek());
    ring.release();
  }
  ASSERT_TRUE(ring.try_push("abc", 3));  // needs 16 bytes: an 8-byte skip record, then offset 0
  auto msg = ring.peek();
  ASSERT_TRUE(msg);
  EXPECT_EQ(text(msg), "abc");
  ring.release();
}

TEST(SpscMsgRingTest, MisuseOfReserveCommitPeekReleaseIsHarmless) {
  aligned_buffer mem(spsc_msg_ring::required_bytes(128));
  spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), 128);

  ring.commit();   // nothing reserved
  ring.release();  // nothing peeked
  EXPECT_FALSE(ring.peek());

  auto first = ring.reserve(10);
  ASSERT_TRUE(first);
  auto second = ring.reserve(20);  // abandons the first reservation
  ASSERT_TRUE(second);
  std::memset(second.data(), 'q', 20);
  ring.commit();
  ring.commit();  // already committed
  auto msg = ring.peek();
  ASSERT_TRUE(msg);
  EXPECT_EQ(msg.size(), 20U);
  ring.release();
  ring.release();  // already released
  EXPECT_FALSE(ring.peek());

  EXPECT_FALSE(ring.reserve(ring.max_message_size() + 1));
  ring.commit();  // the failed reserve left no reservation behind
  EXPECT_FALSE(ring.peek());

  auto room = ring.reserve(8);
  ASSERT_TRUE(room);
  ring.commit(1000);  // longer than reserved: clamped
  msg = ring.peek();
  ASSERT_TRUE(msg);
  EXPECT_EQ(msg.size(), 8U);
}

TEST(SpscMsgRingTest, ReportsAccurateSizes) {
  aligned_buffer mem(spsc_msg_ring::required_bytes(256));
  spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), 256);
  EXPECT_EQ(ring.capacity(), 256U);
  EXPECT_EQ(ring.max_message_size(), 120U);
  EXPECT_EQ(ring.size_approx(), 0U);
  ASSERT_TRUE(ring.try_push("hello", 5));
  EXPECT_EQ(ring.size_approx(), 16U);  // 8-byte header + 5 bytes rounded up to 8
}

TEST(SpscMsgRingTest, CorruptedMemoryNeverYieldsAViewOutsideTheRing) {
  constexpr std::size_t capacity = 128;
  // Offsets inside the memory block: write cursor, read cursor, and the first record header.
  constexpr std::size_t write_cursor = 0;
  constexpr std::size_t first_header = 2 * cache_line;
  {
    aligned_buffer mem(spsc_msg_ring::required_bytes(capacity));
    spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), capacity);
    ASSERT_TRUE(ring.try_push("ok", 2));
    testing_support::poke_u32(
        *static_cast<std::byte*>(mem.get()), first_header, 0x7FFFFFFF);  // huge length
    EXPECT_FALSE(ring.peek());
  }
  {
    aligned_buffer mem(spsc_msg_ring::required_bytes(capacity));
    spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), capacity);
    ASSERT_TRUE(ring.try_push("ok", 2));
    testing_support::poke_u64(
        *static_cast<std::byte*>(mem.get()), write_cursor, 1000000);  // more bytes than fit
    EXPECT_FALSE(ring.peek());
    EXPECT_FALSE(ring.reserve(8));  // also looks full, not like free space
  }
  {
    aligned_buffer mem(spsc_msg_ring::required_bytes(capacity));
    spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), capacity);
    ASSERT_TRUE(ring.try_push("ok", 2));
    testing_support::poke_u32(
        *static_cast<std::byte*>(mem.get()), first_header, 0xFFFFFFFF);  // skip marker
    // A skip marker followed by nothing else published: the ring looks empty, not corrupted.
    EXPECT_FALSE(ring.peek());
  }
}

TEST(SpscMsgRingTest, MisalignedCursorsAreTreatedAsCorruption) {
  constexpr std::size_t capacity = 128;
  constexpr std::size_t write_cursor = 0;
  constexpr std::size_t read_cursor = cache_line;
  {
    aligned_buffer mem(spsc_msg_ring::required_bytes(capacity));
    spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), capacity);
    ASSERT_TRUE(ring.try_push("ok", 2));
    testing_support::poke_u64(*static_cast<std::byte*>(mem.get()), read_cursor, 1);
    EXPECT_FALSE(ring.peek());
  }
  {
    aligned_buffer mem(spsc_msg_ring::required_bytes(capacity));
    spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), capacity);
    testing_support::poke_u64(*static_cast<std::byte*>(mem.get()), write_cursor, 3);
    EXPECT_FALSE(ring.reserve(8));
  }
}

TEST(SpscMsgRingTest, TwoSkipMarkersInARowAreTreatedAsCorruption) {
  constexpr std::size_t capacity = 128;
  aligned_buffer mem(spsc_msg_ring::required_bytes(capacity));
  spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), capacity);
  auto& block = *static_cast<std::byte*>(mem.get());
  const std::size_t buffer = 2 * cache_line;
  // The reader is at offset 8, where a skip marker sends it to the start of the buffer, where
  // another skip marker sits. Eight bytes are published past the wrap.
  testing_support::poke_u64(block, cache_line, 8);    // read cursor
  testing_support::poke_u64(block, 0, capacity + 8);  // write cursor
  testing_support::poke_u32(block, buffer + 8, 0xFFFFFFFF);
  testing_support::poke_u32(block, buffer + 0, 0xFFFFFFFF);
  EXPECT_FALSE(ring.peek());
}

TEST(SharedMessageRingTest, ReportsSetupFailures) {
  const testing_support::name_cleanup name(testing_support::unique_name("msg_err"));
  EXPECT_EQ(shared_spsc_msg_ring::create(name.name(), 100).error().code, errc::bad_capacity);
  EXPECT_EQ(shared_spsc_msg_ring::create(name.name(), 32).error().code, errc::bad_capacity);
  EXPECT_EQ(shared_spsc_msg_ring::open(name.name(), std::chrono::nanoseconds::zero()).error().code,
            errc::not_found);

  auto created = shared_spsc_msg_ring::create(name.name(), 256);
  ASSERT_TRUE(created.has_value());
  EXPECT_EQ(shared_spsc_msg_ring::create(name.name(), 256).error().code, errc::already_exists);
  auto opened = shared_spsc_msg_ring::open(name.name());
  ASSERT_TRUE(opened.has_value());
  EXPECT_EQ(opened.value().ring().capacity(), 256U);
  EXPECT_EQ((shared_spsc_ring<std::uint64_t, 16>::open(name.name()).error().code), errc::bad_kind);
  ASSERT_TRUE(shared_spsc_msg_ring::unlink(name.name()).has_value());
  EXPECT_EQ(shared_spsc_msg_ring::unlink(name.name()).error().code, errc::not_found);
}

}  // namespace
}  // namespace shmring
