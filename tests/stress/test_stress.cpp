// Millions of messages between threads and between processes. Every message carries a sequence
// number and redundant fields, so loss, duplication, reordering and torn copies are all detected.
#include "shmring/blocking_spsc_ring.hpp"
#include "shmring/mpmc_ring.hpp"
#include "shmring/shared_queue.hpp"
#include "shmring/shared_spsc_msg_ring.hpp"
#include "shmring/spmc_ring.hpp"
#include "shmring/spsc_msg_ring.hpp"
#include "shmring/spsc_ring.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <sys/mman.h>
#include <sys/wait.h>
#include <thread>
#include <vector>

#include <unistd.h>

#include "messages.hpp"
#include "support.hpp"

namespace shmring {
namespace {

using testing_support::aligned_buffer;
using testing_support::byte_for;
using testing_support::length_for;
using testing_support::make_msg;
using testing_support::msg;
using testing_support::name_cleanup;
using testing_support::unique_name;
using testing_support::valid;

constexpr std::uint64_t kMillions = 1000000;

TEST(StressThreads, SpscRingScalarFiveMillion) {
  constexpr std::uint64_t total = 5 * kMillions;
  auto ring = std::make_unique<spsc_ring<msg, 1024>>();
  std::thread producer([&] {
    for (std::uint64_t i = 0; i < total; ++i) {
      const msg m = make_msg(i);
      while (!ring->try_push(m)) {
      }
    }
  });
  std::uint64_t bad = 0;
  for (std::uint64_t expected = 0; expected < total; ++expected) {
    msg m{};
    while (!ring->try_pop(m)) {
    }
    bad += (m.seq == expected && valid(m)) ? 0U : 1U;
  }
  producer.join();
  EXPECT_EQ(bad, 0U);
}

TEST(StressThreads, SpscRingBatchesOfRandomSizeFiveMillion) {
  constexpr std::uint64_t total = 5 * kMillions;
  auto ring = std::make_unique<spsc_ring<msg, 256>>();
  std::thread producer([&] {
    std::mt19937 rng(1);
    msg batch[37];
    std::uint64_t next = 0;
    while (next < total) {
      const std::size_t want = std::min<std::uint64_t>(1 + rng() % 37, total - next);
      for (std::size_t i = 0; i < want; ++i) {
        batch[i] = make_msg(next + i);
      }
      std::size_t done = 0;
      while (done < want) {
        done += ring->try_push_n(batch + done, want - done);
      }
      next += want;
    }
  });
  std::uint64_t bad = 0;
  std::uint64_t expected = 0;
  std::mt19937 rng(2);
  msg out[41];
  while (expected < total) {
    const std::size_t got = ring->try_pop_n(out, 1 + rng() % 41);
    for (std::size_t i = 0; i < got; ++i) {
      bad += (out[i].seq == expected + i && valid(out[i])) ? 0U : 1U;
    }
    expected += got;
  }
  producer.join();
  EXPECT_EQ(bad, 0U);
}

TEST(StressThreads, MessageRingTwoMillionVariableLengthMessages) {
  constexpr std::uint64_t total = 2 * kMillions;
  constexpr std::size_t capacity = 4096;
  aligned_buffer mem(spsc_msg_ring::required_bytes(capacity));
  spsc_msg_ring producer_side = spsc_msg_ring::init(mem.get(), capacity);
  spsc_msg_ring consumer_side = spsc_msg_ring::attach(mem.get(), capacity);
  const std::size_t max_len = producer_side.max_message_size();

  std::thread producer([&] {
    for (std::uint64_t seq = 0; seq < total; ++seq) {
      const std::size_t len = length_for(seq, max_len);
      span<std::byte> room = producer_side.reserve(len);
      while (!room) {
        room = producer_side.reserve(len);
      }
      for (std::size_t i = 0; i < len; ++i) {
        room.data()[i] = static_cast<std::byte>(byte_for(seq, i));
      }
      producer_side.commit();
    }
  });
  std::uint64_t bad = 0;
  for (std::uint64_t seq = 0; seq < total; ++seq) {
    span<const std::byte> m = consumer_side.peek();
    while (!m) {
      m = consumer_side.peek();
    }
    bool ok =
        m.size() == length_for(seq, max_len) && reinterpret_cast<std::uintptr_t>(m.data()) % 8 == 0;
    for (std::size_t i = 0; ok && i < m.size(); ++i) {
      ok = static_cast<unsigned char>(m.data()[i]) == byte_for(seq, i);
    }
    bad += ok ? 0U : 1U;
    consumer_side.release();
  }
  producer.join();
  EXPECT_EQ(bad, 0U);
}

TEST(StressThreads, MpmcFourProducersFourConsumersTwoMillion) {
  constexpr std::uint32_t producers = 4;
  constexpr std::uint32_t consumers = 4;
  constexpr std::uint64_t per_producer = 500000;
  auto ring = std::make_unique<mpmc_ring<msg, 128>>();
  std::vector<std::atomic<std::uint8_t>> seen(producers * per_producer);
  std::atomic<std::uint64_t> received{0};
  std::atomic<std::uint64_t> bad{0};

  std::vector<std::thread> threads;
  for (std::uint32_t p = 0; p < producers; ++p) {
    threads.emplace_back([&, p] {
      for (std::uint64_t i = 0; i < per_producer; ++i) {
        const msg m = make_msg((static_cast<std::uint64_t>(p) << 32) | i);
        while (!ring->try_push(m)) {
        }
      }
    });
  }
  for (std::uint32_t c = 0; c < consumers; ++c) {
    threads.emplace_back([&] {
      std::vector<std::int64_t> last(producers, -1);
      while (received.load(std::memory_order_relaxed) < producers * per_producer) {
        msg m{};
        if (!ring->try_pop(m)) {
          continue;
        }
        received.fetch_add(1, std::memory_order_relaxed);
        const std::uint64_t p = m.seq >> 32;
        const std::uint64_t i = m.seq & 0xFFFFFFFFU;
        if (!valid(m) || p >= producers || i >= per_producer) {
          bad.fetch_add(1);
          continue;
        }
        if (seen[p * per_producer + i].exchange(1) != 0) {
          bad.fetch_add(1);  // duplicate
        }
        if (static_cast<std::int64_t>(i) <= last[p]) {
          bad.fetch_add(1);  // reordered within one producer
        }
        last[p] = static_cast<std::int64_t>(i);
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

TEST(StressThreads, BroadcastRingFiveMillionWithFourReaders) {
  constexpr std::uint64_t total = 5 * kMillions;
  constexpr int readers = 4;
  auto ring = std::make_unique<spmc_ring<msg, 64>>();
  std::atomic<int> joined{0};
  std::atomic<std::uint64_t> torn{0};
  std::atomic<std::uint64_t> reordered{0};
  std::atomic<std::uint64_t> delivered{0};

  std::vector<std::thread> threads;
  for (int r = 0; r < readers; ++r) {
    threads.emplace_back([&] {
      spmc_reader reader = ring->join_latest();
      joined.fetch_add(1);
      std::uint64_t last = 0;
      bool any = false;
      for (;;) {
        msg m{};
        const read_status st = ring->try_read(reader, m);
        if (st == read_status::ok) {
          if (!valid(m))
            torn.fetch_add(1);
          if (any && m.seq <= last)
            reordered.fetch_add(1);
          any = true;
          last = m.seq;
          delivered.fetch_add(1, std::memory_order_relaxed);
          if (m.seq + 1 == total)
            break;
        }
      }
    });
  }
  while (joined.load() < readers) {
  }
  for (std::uint64_t i = 0; i < total; ++i) {
    ring->publish(make_msg(i));
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(torn.load(), 0U);
  EXPECT_EQ(reordered.load(), 0U);
  EXPECT_GT(delivered.load(), 0U);
}

TEST(StressThreads, BlockingRingOneMillionBlockWakeCyclesLoseNoWakeup) {
  constexpr std::uint64_t total = kMillions;
  // Two slots: the producer and the consumer wait for each other constantly.
  auto ring = std::make_unique<blocking_spsc_ring<std::uint64_t, 2>>();
  std::thread producer([&] {
    for (std::uint64_t i = 0; i < total; ++i) {
      if (!ring->push(i, std::chrono::seconds(20))) {
        ADD_FAILURE() << "producer stalled at " << i;
        return;
      }
    }
  });
  std::uint64_t bad = 0;
  for (std::uint64_t expected = 0; expected < total; ++expected) {
    std::uint64_t v = ~0ULL;
    if (!ring->pop(v, std::chrono::seconds(20))) {
      ADD_FAILURE() << "consumer stalled at " << expected;
      break;
    }
    bad += v == expected ? 0U : 1U;
  }
  producer.join();
  EXPECT_EQ(bad, 0U);
}

// ---------------------------------------------------------------------------------------------
// Between processes

TEST(StressProcesses, SpscRingThreeMillion) {
  constexpr std::uint64_t total = 3 * kMillions;
  const name_cleanup name(unique_name("st_spsc"));
  auto created = shared_spsc_ring<msg, 1024>::create(name.name());
  ASSERT_TRUE(created.has_value());
  const pid_t pid = ::fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    auto side = shared_spsc_ring<msg, 1024>::open(name.name());
    if (!side)
      ::_exit(2);
    for (std::uint64_t i = 0; i < total; ++i) {
      const msg m = make_msg(i);
      while (!side.value().queue().try_push(m)) {
      }
    }
    ::_exit(0);
  }
  std::uint64_t bad = 0;
  for (std::uint64_t expected = 0; expected < total; ++expected) {
    msg m{};
    while (!created.value().queue().try_pop(m)) {
    }
    bad += (m.seq == expected && valid(m)) ? 0U : 1U;
  }
  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  EXPECT_EQ(WEXITSTATUS(status), 0);
  EXPECT_EQ(bad, 0U);
}

TEST(StressProcesses, MessageRingOneMillionVariableLengthMessages) {
  constexpr std::uint64_t total = kMillions;
  const name_cleanup name(unique_name("st_msg"));
  auto created = shared_spsc_msg_ring::create(name.name(), 8192);
  ASSERT_TRUE(created.has_value());
  const pid_t pid = ::fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    auto side = shared_spsc_msg_ring::open(name.name());
    if (!side)
      ::_exit(2);
    spsc_msg_ring& ring = side.value().ring();
    for (std::uint64_t seq = 0; seq < total; ++seq) {
      const std::size_t len = length_for(seq, ring.max_message_size());
      span<std::byte> room = ring.reserve(len);
      while (!room) {
        room = ring.reserve(len);
      }
      for (std::size_t i = 0; i < len; ++i) {
        room.data()[i] = static_cast<std::byte>(byte_for(seq, i));
      }
      ring.commit();
    }
    ::_exit(0);
  }
  spsc_msg_ring& ring = created.value().ring();
  std::uint64_t bad = 0;
  for (std::uint64_t seq = 0; seq < total; ++seq) {
    span<const std::byte> m = ring.peek();
    while (!m) {
      m = ring.peek();
    }
    bool ok = m.size() == length_for(seq, ring.max_message_size());
    for (std::size_t i = 0; ok && i < m.size(); ++i) {
      ok = static_cast<unsigned char>(m.data()[i]) == byte_for(seq, i);
    }
    bad += ok ? 0U : 1U;
    ring.release();
  }
  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  EXPECT_EQ(WEXITSTATUS(status), 0);
  EXPECT_EQ(bad, 0U);
}

TEST(StressProcesses, MpmcThreeProducerProcessesOneConsumerProcess) {
  constexpr std::uint32_t producers = 3;
  constexpr std::uint64_t per_producer = 500000;
  const name_cleanup name(unique_name("st_mpmc"));
  auto created = shared_mpmc_ring<msg, 256>::create(name.name());
  ASSERT_TRUE(created.has_value());
  std::vector<pid_t> children;
  for (std::uint32_t p = 0; p < producers; ++p) {
    const pid_t pid = ::fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
      auto side = shared_mpmc_ring<msg, 256>::open(name.name());
      if (!side)
        ::_exit(2);
      for (std::uint64_t i = 0; i < per_producer; ++i) {
        const msg m = make_msg((static_cast<std::uint64_t>(p) << 32) | i);
        while (!side.value().queue().try_push(m)) {
        }
      }
      ::_exit(0);
    }
    children.push_back(pid);
  }
  std::vector<std::uint64_t> next(producers, 0);
  std::uint64_t bad = 0;
  for (std::uint64_t got = 0; got < producers * per_producer;) {
    msg m{};
    if (!created.value().queue().try_pop(m)) {
      continue;
    }
    ++got;
    const std::uint64_t p = m.seq >> 32;
    const std::uint64_t i = m.seq & 0xFFFFFFFFU;
    if (!valid(m) || p >= producers || i != next[p]) {
      ++bad;
      continue;
    }
    ++next[p];
  }
  for (const pid_t pid : children) {
    int status = 0;
    ASSERT_EQ(::waitpid(pid, &status, 0), pid);
    EXPECT_EQ(WEXITSTATUS(status), 0);
  }
  EXPECT_EQ(bad, 0U);
}

TEST(StressProcesses, BlockingRingThreeHundredThousandBlockWakeCycles) {
  constexpr std::uint64_t total = 300000;
  const name_cleanup name(unique_name("st_block"));
  auto created = shared_blocking_spsc_ring<std::uint64_t, 2>::create(name.name());
  ASSERT_TRUE(created.has_value());
  const pid_t pid = ::fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    auto side = shared_blocking_spsc_ring<std::uint64_t, 2>::open(name.name());
    if (!side)
      ::_exit(2);
    for (std::uint64_t i = 0; i < total; ++i) {
      if (!side.value().queue().push(i, std::chrono::seconds(20)))
        ::_exit(3);
    }
    ::_exit(0);
  }
  std::uint64_t bad = 0;
  for (std::uint64_t expected = 0; expected < total; ++expected) {
    std::uint64_t v = ~0ULL;
    if (!created.value().queue().pop(v, std::chrono::seconds(20))) {
      ADD_FAILURE() << "stalled at " << expected;
      break;
    }
    bad += v == expected ? 0U : 1U;
  }
  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  EXPECT_EQ(WEXITSTATUS(status), 0);
  EXPECT_EQ(bad, 0U);
}

TEST(StressProcesses, BroadcastRingWriterProcessAndReaderProcesses) {
  constexpr std::uint64_t total = 2 * kMillions;
  constexpr int readers = 2;
  const name_cleanup name(unique_name("st_spmc"));
  auto created = shared_spmc_ring<msg, 64>::create(name.name());
  ASSERT_TRUE(created.has_value());
  // A counter in anonymous shared memory tells the writer when the readers have joined.
  auto* joined = static_cast<std::atomic<int>*>(::mmap(nullptr,
                                                       sizeof(std::atomic<int>),
                                                       PROT_READ | PROT_WRITE,
                                                       MAP_SHARED | MAP_ANONYMOUS,
                                                       -1,
                                                       0));
  ASSERT_NE(joined, MAP_FAILED);
  new (joined) std::atomic<int>(0);

  std::vector<pid_t> children;
  for (int r = 0; r < readers; ++r) {
    const pid_t pid = ::fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
      auto side = shared_spmc_ring<msg, 64>::open(name.name());
      if (!side)
        ::_exit(2);
      spmc_reader reader = side.value().queue().join_latest();
      joined->fetch_add(1);
      std::uint64_t last = 0;
      bool any = false;
      for (;;) {
        msg m{};
        if (side.value().queue().try_read(reader, m) != read_status::ok) {
          continue;
        }
        if (!valid(m) || (any && m.seq <= last))
          ::_exit(3);
        any = true;
        last = m.seq;
        if (m.seq + 1 == total)
          ::_exit(0);
      }
    }
    children.push_back(pid);
  }
  while (joined->load() < readers) {
  }
  for (std::uint64_t i = 0; i < total; ++i) {
    created.value().queue().publish(make_msg(i));
  }
  for (const pid_t pid : children) {
    int status = 0;
    ASSERT_EQ(::waitpid(pid, &status, 0), pid);
    EXPECT_EQ(WEXITSTATUS(status), 0);
  }
  ::munmap(joined, sizeof(std::atomic<int>));
}

}  // namespace
}  // namespace shmring
