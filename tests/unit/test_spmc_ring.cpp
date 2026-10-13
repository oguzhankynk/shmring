#include "shmring/shared_queue.hpp"
#include "shmring/spmc_ring.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <vector>

#include <unistd.h>

#include "support.hpp"

namespace shmring {
namespace {

struct Wide {
  std::uint64_t seq;
  std::uint64_t parts[7];
};

Wide make_wide(std::uint64_t seq) {
  Wide w{};
  w.seq = seq;
  for (std::uint64_t i = 0; i < 7; ++i) {
    w.parts[i] = seq * (i + 3);
  }
  return w;
}

bool consistent(const Wide& w) {
  for (std::uint64_t i = 0; i < 7; ++i) {
    if (w.parts[i] != w.seq * (i + 3)) {
      return false;
    }
  }
  return true;
}

TEST(SpmcRingTest, EveryReaderSeesEveryElement) {
  spmc_ring<int, 8> ring;
  auto a = ring.join_latest();
  auto b = ring.join_latest();
  int v = 0;
  EXPECT_EQ(ring.try_read(a, v), read_status::empty);
  for (int i = 0; i < 5; ++i) {
    ring.publish(i);
  }
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(ring.try_read(a, v), read_status::ok);
    EXPECT_EQ(v, i);
  }
  EXPECT_EQ(ring.try_read(a, v), read_status::empty);
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(ring.try_read(b, v), read_status::ok);
    EXPECT_EQ(v, i);
  }
}

TEST(SpmcRingTest, LateJoinersStartWhereTheyAsked) {
  spmc_ring<int, 8> ring;
  for (int i = 0; i < 20; ++i) {
    ring.publish(i);
  }
  int v = -1;
  auto latest = ring.join_latest();
  EXPECT_EQ(ring.try_read(latest, v), read_status::empty);
  auto oldest = ring.join_oldest();
  ASSERT_EQ(ring.try_read(oldest, v), read_status::ok);
  EXPECT_EQ(v, 16);  // half a ring behind the writer
}

TEST(SpmcRingTest, SlowReaderIsToldItWasOverrun) {
  spmc_ring<int, 8> ring;
  auto slow = ring.join_latest();
  for (int i = 0; i < 100; ++i) {
    ring.publish(i);
  }
  int v = -1;
  EXPECT_EQ(ring.try_read(slow, v), read_status::overrun);
  EXPECT_GT(slow.dropped(), 0U);
  ASSERT_EQ(ring.try_read(slow, v), read_status::ok);
  EXPECT_EQ(v, 96);
}

TEST(SpmcRingTest, ReadersNeverSeeTornElementsWhileTheWriterRacesAhead) {
  constexpr std::uint64_t total = 1000000;
  constexpr int readers = 3;
  auto ring = std::make_unique<spmc_ring<Wide, 16>>();
  std::atomic<bool> go{false};
  std::atomic<std::uint64_t> torn{0};
  std::atomic<std::uint64_t> reordered{0};
  std::atomic<std::uint64_t> delivered{0};

  std::vector<std::thread> threads;
  for (int r = 0; r < readers; ++r) {
    threads.emplace_back([&] {
      auto reader = ring->join_latest();
      go.store(true);
      std::uint64_t last = 0;
      bool any = false;
      Wide w{};
      for (;;) {
        const read_status st = ring->try_read(reader, w);
        if (st == read_status::ok) {
          if (!consistent(w))
            torn.fetch_add(1);
          if (any && w.seq <= last)
            reordered.fetch_add(1);
          any = true;
          last = w.seq;
          delivered.fetch_add(1, std::memory_order_relaxed);
          if (w.seq + 1 == total)
            break;
        } else if (st == read_status::empty && ring->published() >= total && any &&
                   last + 1 >= total) {
          break;
        }
      }
    });
  }
  while (!go.load()) {
  }
  for (std::uint64_t i = 0; i < total; ++i) {
    ring->publish(make_wide(i));
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(torn.load(), 0U);
  EXPECT_EQ(reordered.load(), 0U);
  EXPECT_GT(delivered.load(), 0U);
}

TEST(SpmcRingTest, WorksBetweenProcesses) {
  const std::string name = "/shmring_test_spmc_" + std::to_string(::getpid());
  auto writer = shared_spmc_ring<Wide, 64>::create(name);
  ASSERT_TRUE(writer.has_value());
  auto reader_side = shared_spmc_ring<Wide, 64>::open(name);
  ASSERT_TRUE(reader_side.has_value());
  auto reader = reader_side.value().queue().join_latest();

  const pid_t pid = ::fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    for (std::uint64_t i = 0; i < 40; ++i) {
      writer.value().queue().publish(make_wide(i));
    }
    ::_exit(0);
  }
  int status = 0;
  ::waitpid(pid, &status, 0);
  Wide w{};
  std::uint64_t seen = 0;
  while (reader_side.value().queue().try_read(reader, w) != read_status::empty) {
    EXPECT_TRUE(consistent(w));
    ++seen;
  }
  EXPECT_GT(seen, 0U);
  (void)shared_spmc_ring<Wide, 64>::unlink(name);
}

TEST(SpmcRingTest, ReportsCapacityAndPublishedCount) {
  spmc_ring<int, 8> ring;
  EXPECT_EQ(ring.capacity(), 8U);
  EXPECT_EQ(ring.published(), 0U);
  ring.publish(1);
  ring.publish(2);
  EXPECT_EQ(ring.published(), 2U);
}

TEST(SpmcRingTest, JoiningOldestOnAYoungRingStartsAtTheFirstElement) {
  spmc_ring<int, 8> ring;
  ring.publish(5);
  ring.publish(6);
  auto reader = ring.join_oldest();
  int v = 0;
  ASSERT_EQ(ring.try_read(reader, v), read_status::ok);
  EXPECT_EQ(v, 5);
  ASSERT_EQ(ring.try_read(reader, v), read_status::ok);
  EXPECT_EQ(v, 6);
  EXPECT_EQ(ring.try_read(reader, v), read_status::empty);
}

TEST(SpmcRingTest, AReaderThatTheWriterLappedIsOverrun) {
  spmc_ring<int, 4> ring;
  auto reader = ring.join_latest();
  for (int i = 0; i < 4; ++i) {
    ring.publish(i);
  }
  int v = -1;
  ASSERT_EQ(ring.try_read(reader, v), read_status::ok);  // a full ring is still intact
  EXPECT_EQ(v, 0);
  ring.publish(4);  // reuses the slot of position 0, which the reader has already passed
  ASSERT_EQ(ring.try_read(reader, v), read_status::ok);
  EXPECT_EQ(v, 1);
  ring.publish(5);
  ring.publish(6);  // now the slot of position 2 is gone
  EXPECT_EQ(ring.try_read(reader, v), read_status::overrun);
  EXPECT_EQ(reader.dropped(), 3U);  // positions 2, 3 and 4 were skipped: resumes at 5
  ASSERT_EQ(ring.try_read(reader, v), read_status::ok);
  EXPECT_EQ(v, 5);
}

TEST(SpmcRingTest, StoresElementsOfAnySize) {
  spmc_ring<char, 4> tiny;
  auto r1 = tiny.join_latest();
  tiny.publish('q');
  char c = 0;
  ASSERT_EQ(tiny.try_read(r1, c), read_status::ok);
  EXPECT_EQ(c, 'q');

  spmc_ring<std::array<char, 13>, 4> odd;
  auto r2 = odd.join_latest();
  std::array<char, 13> in{};
  in[12] = 'z';
  odd.publish(in);
  std::array<char, 13> out{};
  ASSERT_EQ(odd.try_read(r2, out), read_status::ok);
  EXPECT_EQ(out[12], 'z');
}

TEST(SpmcRingTest, AWriterThatStoppedMidPublishLeavesTheSlotUnreadableUntilANewWriterContinues) {
  spmc_ring<int, 4> ring;
  auto reader = ring.join_latest();
  ring.publish(1);  // position 0
  int v = 0;
  ASSERT_EQ(ring.try_read(reader, v), read_status::ok);
  // The writer died while writing position 1: it had marked the slot as being rewritten (odd
  // stamp, the first word of the cell, which follows the head cache line) but had not advanced
  // the head. Each cell here is a stamp plus one data word.
  testing_support::poke_u64(ring, cache_line + 1 * (sizeof(std::uint64_t) * 2), 2 * 1 + 1);
  EXPECT_EQ(ring.try_read(reader, v), read_status::empty);
  ring.publish(77);  // a new writer picks up at the head and completes position 1
  ASSERT_EQ(ring.try_read(reader, v), read_status::ok);
  EXPECT_EQ(v, 77);
}

}  // namespace
}  // namespace shmring
