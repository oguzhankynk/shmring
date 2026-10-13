#include "shmring/spsc_ring.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <thread>
#include <vector>

#include "support.hpp"

namespace shmring {
namespace {

TEST(SpscRingTest, StartsEmptyAndReportsCapacity) {
  spsc_ring<int, 8> ring;
  int v = 0;
  EXPECT_FALSE(ring.try_pop(v));
  EXPECT_EQ(ring.size_approx(), 0U);
  EXPECT_EQ(ring.capacity(), 8U);
}

TEST(SpscRingTest, PreservesFifoOrderAndRejectsWhenFull) {
  spsc_ring<int, 4> ring;
  for (int i = 0; i < 4; ++i) {
    EXPECT_TRUE(ring.try_push(i));
  }
  EXPECT_FALSE(ring.try_push(99));
  EXPECT_EQ(ring.size_approx(), 4U);
  for (int i = 0; i < 4; ++i) {
    int v = -1;
    ASSERT_TRUE(ring.try_pop(v));
    EXPECT_EQ(v, i);
  }
  int v = -1;
  EXPECT_FALSE(ring.try_pop(v));
}

TEST(SpscRingTest, WrapsAroundThePhysicalBuffer) {
  spsc_ring<int, 4> ring;
  int next_in = 0;
  int next_out = 0;
  for (int round = 0; round < 100; ++round) {
    ASSERT_TRUE(ring.try_push(next_in++));
    ASSERT_TRUE(ring.try_push(next_in++));
    int v = 0;
    ASSERT_TRUE(ring.try_pop(v));
    EXPECT_EQ(v, next_out++);
    ASSERT_TRUE(ring.try_pop(v));
    EXPECT_EQ(v, next_out++);
  }
}

TEST(SpscRingTest, WorksAcrossCounterOverflow) {
  constexpr std::uint64_t start = std::numeric_limits<std::uint64_t>::max() - 5;
  spsc_ring<int, 4> ring(start);
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

TEST(SpscRingTest, BatchOperationsHandleWrapAndPartialTransfers) {
  constexpr std::uint64_t start = std::numeric_limits<std::uint64_t>::max() - 2;
  spsc_ring<int, 8> ring(start);
  const std::vector<int> input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  EXPECT_EQ(ring.try_push_n(input.data(), input.size()), 8U);
  EXPECT_EQ(ring.try_push_n(input.data(), 1), 0U);

  std::vector<int> out(5);
  EXPECT_EQ(ring.try_pop_n(out.data(), out.size()), 5U);
  EXPECT_EQ(out, (std::vector<int>{1, 2, 3, 4, 5}));

  EXPECT_EQ(ring.try_push_n(input.data() + 8, 2), 2U);
  std::vector<int> rest(10);
  EXPECT_EQ(ring.try_pop_n(rest.data(), rest.size()), 5U);
  rest.resize(5);
  EXPECT_EQ(rest, (std::vector<int>{6, 7, 8, 9, 10}));
  EXPECT_EQ(ring.try_pop_n(rest.data(), 1), 0U);
}

struct Message {
  std::uint64_t seq;
  std::uint64_t checksum;
};

TEST(SpscRingTest, ThreadsTransferEveryMessageInOrder) {
  constexpr std::uint64_t total = 200000;
  spsc_ring<Message, 1024> ring;

  std::thread producer([&] {
    for (std::uint64_t i = 0; i < total; ++i) {
      const Message m{i, ~i};
      while (!ring.try_push(m)) {
      }
    }
  });

  std::uint64_t bad = 0;
  for (std::uint64_t expected = 0; expected < total;) {
    Message m{};
    if (!ring.try_pop(m)) {
      continue;
    }
    if (m.seq != expected || m.checksum != ~expected) {
      ++bad;
    }
    ++expected;
  }
  producer.join();
  EXPECT_EQ(bad, 0U);
}

TEST(SpscRingTest, EmptyBatchRequestsDoNothing) {
  spsc_ring<int, 4> ring;
  int buffer[4] = {};
  EXPECT_EQ(ring.try_push_n(buffer, 0), 0U);
  EXPECT_EQ(ring.try_pop_n(buffer, 0), 0U);
  EXPECT_EQ(ring.size_approx(), 0U);
}

TEST(SpscRingTest, BatchPushStopsWhenFullAndBatchPopStopsWhenEmpty) {
  spsc_ring<int, 4> ring;
  const int in[6] = {1, 2, 3, 4, 5, 6};
  EXPECT_EQ(ring.try_push_n(in, 3), 3U);
  EXPECT_EQ(ring.try_push_n(in + 3, 3), 1U);  // only one slot was left
  EXPECT_EQ(ring.try_push_n(in + 3, 3), 0U);  // now full
  int out[8] = {};
  EXPECT_EQ(ring.try_pop_n(out, 8), 4U);
  EXPECT_EQ(out[0], 1);
  EXPECT_EQ(out[3], 4);
  EXPECT_EQ(ring.try_pop_n(out, 8), 0U);
}

TEST(SpscRingTest, MixesScalarAndBatchOperationsAcrossManyWraps) {
  spsc_ring<std::uint32_t, 8> ring(std::numeric_limits<std::uint64_t>::max() - 3);
  std::uint32_t next_in = 0;
  std::uint32_t next_out = 0;
  for (int round = 0; round < 500; ++round) {
    std::uint32_t batch[5];
    const std::size_t want = 1 + static_cast<std::size_t>(round % 5);
    for (std::size_t i = 0; i < want; ++i) {
      batch[i] = next_in + static_cast<std::uint32_t>(i);
    }
    const std::size_t pushed = ring.try_push_n(batch, want);
    next_in += static_cast<std::uint32_t>(pushed);
    if (round % 3 == 0) {
      std::uint32_t one = 0;
      if (ring.try_pop(one)) {
        ASSERT_EQ(one, next_out++);
      }
    } else {
      std::uint32_t out[8];
      const std::size_t popped = ring.try_pop_n(out, 1 + static_cast<std::size_t>(round % 7));
      for (std::size_t i = 0; i < popped; ++i) {
        ASSERT_EQ(out[i], next_out++);
      }
    }
  }
}

TEST(SpscRingTest, HandlesElementsOfAnyAlignmentAndSize) {
  struct alignas(32) Wide {
    char bytes[96];
  };
  spsc_ring<Wide, 4> wide;
  Wide w{};
  w.bytes[0] = 'a';
  w.bytes[95] = 'z';
  ASSERT_TRUE(wide.try_push(w));
  Wide r{};
  ASSERT_TRUE(wide.try_pop(r));
  EXPECT_EQ(r.bytes[0], 'a');
  EXPECT_EQ(r.bytes[95], 'z');

  spsc_ring<std::array<char, 3>, 4> odd;
  ASSERT_TRUE(odd.try_push({'x', 'y', 'z'}));
  std::array<char, 3> o{};
  ASSERT_TRUE(odd.try_pop(o));
  EXPECT_EQ(o[2], 'z');
}

TEST(SpscRingTest, SizeApproxNeverExceedsTheCapacity) {
  spsc_ring<int, 8> ring;
  testing_support::poke_u64(ring, cache_line, 1000);  // tail ahead of head: garbage, but clamped
  EXPECT_LE(ring.size_approx(), 8U);
}

TEST(SpscRingTest, CorruptedCountersNeverCauseOutOfBoundsAccess) {
  {
    spsc_ring<std::uint64_t, 8> ring;
    testing_support::poke_u64(ring, 0, 1000);  // head far ahead of tail: more than N "in flight"
    std::uint64_t out[16] = {};
    EXPECT_EQ(ring.try_pop_n(out, 16), 0U);
    std::uint64_t one = 0;
    (void)ring.try_pop(one);  // reads a masked slot; must stay inside the ring (ASan checks)
  }
  {
    spsc_ring<std::uint64_t, 8> ring;
    testing_support::poke_u64(ring, cache_line, 1000);  // tail far ahead of head
    const std::uint64_t in[16] = {};
    EXPECT_EQ(ring.try_push_n(in, 16), 0U);
    EXPECT_FALSE(ring.try_push(7));
  }
}

}  // namespace
}  // namespace shmring
