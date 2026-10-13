#include "shmring/mpmc_ring.hpp"
#include "shmring/shared_queue.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <vector>

#include <unistd.h>

#include "support.hpp"

namespace shmring {
namespace {

TEST(MpmcRingTest, FifoBoundariesAndWrapAround) {
  mpmc_ring<int, 4> ring;
  int v = 0;
  EXPECT_FALSE(ring.try_pop(v));
  for (int i = 0; i < 4; ++i) {
    EXPECT_TRUE(ring.try_push(i));
  }
  EXPECT_FALSE(ring.try_push(4));
  EXPECT_EQ(ring.size_approx(), 4U);
  for (int round = 0; round < 50; ++round) {
    ASSERT_TRUE(ring.try_pop(v));
    EXPECT_EQ(v, round);
    ASSERT_TRUE(ring.try_push(round + 4));
  }
}

TEST(MpmcRingTest, WorksAcrossCounterOverflow) {
  mpmc_ring<int, 4> ring(std::numeric_limits<std::uint64_t>::max() - 5);
  int v = 0;
  EXPECT_FALSE(ring.try_pop(v));
  for (int i = 0; i < 40; ++i) {
    ASSERT_TRUE(ring.try_push(i));
    ASSERT_TRUE(ring.try_pop(v));
    EXPECT_EQ(v, i);
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_TRUE(ring.try_push(i));
  }
  EXPECT_FALSE(ring.try_push(4));
}

struct Message {
  std::uint32_t producer;
  std::uint32_t seq;
  std::uint64_t checksum;
};

TEST(MpmcRingTest, ManyProducersAndConsumersLoseAndDuplicateNothing) {
  constexpr std::uint32_t producers = 4;
  constexpr std::uint32_t consumers = 4;
  constexpr std::uint32_t per_producer = 40000;
  auto ring = std::make_unique<mpmc_ring<Message, 64>>();
  std::vector<std::atomic<std::uint8_t>> seen(producers * per_producer);
  std::atomic<std::uint64_t> received{0};
  std::atomic<std::uint64_t> bad{0};

  std::vector<std::thread> threads;
  for (std::uint32_t p = 0; p < producers; ++p) {
    threads.emplace_back([&, p] {
      for (std::uint32_t i = 0; i < per_producer; ++i) {
        const Message m{p, i, (static_cast<std::uint64_t>(p) << 32) ^ i};
        while (!ring->try_push(m)) {
        }
      }
    });
  }
  for (std::uint32_t c = 0; c < consumers; ++c) {
    threads.emplace_back([&] {
      std::vector<std::uint32_t> last(producers, 0);
      std::vector<bool> any(producers, false);
      while (received.load(std::memory_order_relaxed) < producers * per_producer) {
        Message m{};
        if (!ring->try_pop(m)) {
          continue;
        }
        received.fetch_add(1, std::memory_order_relaxed);
        if (m.producer >= producers || m.seq >= per_producer ||
            m.checksum != ((static_cast<std::uint64_t>(m.producer) << 32) ^ m.seq)) {
          bad.fetch_add(1);
          continue;
        }
        if (seen[m.producer * per_producer + m.seq].exchange(1) != 0) {
          bad.fetch_add(1);  // duplicate
        }
        if (any[m.producer] && m.seq <= last[m.producer]) {
          bad.fetch_add(1);  // reordered within one producer
        }
        any[m.producer] = true;
        last[m.producer] = m.seq;
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(bad.load(), 0U);
  EXPECT_EQ(received.load(), producers * per_producer);
  for (const auto& flag : seen) {
    ASSERT_EQ(flag.load(), 1);
  }
}

TEST(MpmcRingTest, ChildProducersFeedTheParentThroughASegment) {
  constexpr std::uint32_t producers = 2;
  constexpr std::uint32_t per_producer = 20000;
  const std::string name = "/shmring_test_mpmc_" + std::to_string(::getpid());
  auto created = shared_mpmc_ring<Message, 64>::create(name);
  ASSERT_TRUE(created.has_value());

  std::vector<pid_t> children;
  for (std::uint32_t p = 0; p < producers; ++p) {
    const pid_t pid = ::fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
      auto ring = shared_mpmc_ring<Message, 64>::open(name);
      if (!ring) {
        ::_exit(2);
      }
      for (std::uint32_t i = 0; i < per_producer; ++i) {
        const Message m{p, i, (static_cast<std::uint64_t>(p) << 32) ^ i};
        while (!ring.value().queue().try_push(m)) {
        }
      }
      ::_exit(0);
    }
    children.push_back(pid);
  }
  std::vector<std::uint32_t> next(producers, 0);
  std::uint64_t bad = 0;
  for (std::uint64_t got = 0; got < producers * per_producer;) {
    Message m{};
    if (!created.value().queue().try_pop(m)) {
      continue;
    }
    ++got;
    if (m.producer >= producers || m.seq != next[m.producer] ||
        m.checksum != ((static_cast<std::uint64_t>(m.producer) << 32) ^ m.seq)) {
      ++bad;
      continue;
    }
    ++next[m.producer];
  }
  for (const pid_t pid : children) {
    int status = 0;
    ::waitpid(pid, &status, 0);
    EXPECT_EQ(WEXITSTATUS(status), 0);
  }
  EXPECT_EQ(bad, 0U);
  (void)shared_mpmc_ring<Message, 64>::unlink(name);
}

TEST(MpmcRingTest, SmallestRingHoldsTwoElements) {
  mpmc_ring<int, 2> ring;
  int v = 0;
  EXPECT_TRUE(ring.try_push(1));
  EXPECT_TRUE(ring.try_push(2));
  EXPECT_FALSE(ring.try_push(3));
  EXPECT_EQ(ring.size_approx(), 2U);
  for (int round = 0; round < 100; ++round) {
    ASSERT_TRUE(ring.try_pop(v));
    ASSERT_TRUE(ring.try_push(round + 3));
  }
  EXPECT_EQ(ring.capacity(), 2U);
}

TEST(MpmcRingTest, HandlesElementsOfAnyAlignmentAndSize) {
  struct alignas(32) Wide {
    char bytes[40];
  };
  mpmc_ring<Wide, 4> wide;
  Wide w{};
  w.bytes[39] = 'z';
  ASSERT_TRUE(wide.try_push(w));
  Wide r{};
  ASSERT_TRUE(wide.try_pop(r));
  EXPECT_EQ(r.bytes[39], 'z');

  mpmc_ring<std::array<char, 3>, 4> odd;
  ASSERT_TRUE(odd.try_push({'a', 'b', 'c'}));
  std::array<char, 3> o{};
  ASSERT_TRUE(odd.try_pop(o));
  EXPECT_EQ(o[0], 'a');
}

TEST(MpmcRingTest, ASlotClaimedByAProducerThatDiedBlocksConsumersAtThatSlot) {
  // Simulates the documented limitation: a producer takes ticket 0 (advancing the enqueue counter,
  // the first word of the ring) and then disappears without publishing.
  mpmc_ring<int, 4> ring;
  testing_support::poke_u64(ring, 0, 1);

  EXPECT_TRUE(ring.try_push(10));  // tickets 1..3 are still usable
  EXPECT_TRUE(ring.try_push(11));
  EXPECT_TRUE(ring.try_push(12));
  EXPECT_FALSE(ring.try_push(13));  // ticket 4 would reuse the unpublished slot: looks full

  int v = 0;
  EXPECT_FALSE(ring.try_pop(v));  // the consumer is stuck at ticket 0 although three elements wait
  EXPECT_EQ(ring.size_approx(), 4U);
}

}  // namespace
}  // namespace shmring
