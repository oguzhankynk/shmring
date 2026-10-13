// A peer is killed with SIGKILL while it works. The surviving side must never see a torn or
// partly written message, and a new peer must be able to attach and continue.
#include "shmring/shared_queue.hpp"
#include "shmring/shared_spsc_msg_ring.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <sys/mman.h>
#include <sys/wait.h>
#include <thread>
#include <vector>

#include <signal.h>
#include <unistd.h>

#include "messages.hpp"
#include "support.hpp"

namespace shmring {
namespace {

using testing_support::byte_for;
using testing_support::length_for;
using testing_support::make_msg;
using testing_support::msg;
using testing_support::name_cleanup;
using testing_support::unique_name;
using testing_support::valid;

// Counters shared between this process and its children.
struct shared_counters {
  std::atomic<std::uint64_t> last_seen;
  std::atomic<std::uint64_t> first_seen;
  std::atomic<int> started;
};

shared_counters* make_shared_counters() {
  void* mem = ::mmap(
      nullptr, sizeof(shared_counters), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (mem == MAP_FAILED) {
    return nullptr;
  }
  return new (mem) shared_counters{};
}

void kill_and_reap(pid_t pid) {
  ::kill(pid, SIGKILL);
  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFSIGNALED(status));
  ASSERT_EQ(WTERMSIG(status), SIGKILL);
}

// Random delay so that the kill lands at different points of the producer's loop.
void random_pause(unsigned seed) {
  std::mt19937 rng(seed);
  std::this_thread::sleep_for(std::chrono::microseconds(rng() % 4000));
}

constexpr int kRepeats = 20;

TEST(CrashSpscRing, KillingTheProducerNeverExposesATornMessageAndANewProducerContinues) {
  for (int round = 0; round < kRepeats; ++round) {
    const name_cleanup name(unique_name("crash_prod"));
    auto created = shared_spsc_ring<msg, 256>::create(name.name());
    ASSERT_TRUE(created.has_value());
    spsc_ring<msg, 256>& ring = created.value().queue();

    const pid_t producer = ::fork();
    ASSERT_GE(producer, 0);
    if (producer == 0) {
      auto side = shared_spsc_ring<msg, 256>::open(name.name());
      if (!side)
        ::_exit(2);
      for (std::uint64_t i = 0;; ++i) {
        const msg m = make_msg(i);
        while (!side.value().queue().try_push(m)) {
        }
      }
    }

    std::uint64_t expected = 0;
    std::uint64_t bad = 0;
    const auto drain_some = [&](std::uint64_t at_least) {
      while (expected < at_least) {
        msg m{};
        if (!ring.try_pop(m))
          continue;
        bad += (m.seq == expected && valid(m)) ? 0U : 1U;
        ++expected;
      }
    };
    drain_some(50000);
    random_pause(static_cast<unsigned>(round));
    kill_and_reap(producer);

    // Everything the dead producer published is intact and in order; the rest is simply absent.
    msg m{};
    while (ring.try_pop(m)) {
      bad += (m.seq == expected && valid(m)) ? 0U : 1U;
      ++expected;
    }
    ASSERT_EQ(bad, 0U) << "round " << round;
    ASSERT_FALSE(ring.try_pop(m));

    // A new producer attaches and carries on from where the stream stopped.
    const std::uint64_t resume = expected;
    const pid_t replacement = ::fork();
    ASSERT_GE(replacement, 0);
    if (replacement == 0) {
      auto side = shared_spsc_ring<msg, 256>::open(name.name());
      if (!side)
        ::_exit(2);
      for (std::uint64_t i = resume; i < resume + 20000; ++i) {
        const msg next = make_msg(i);
        while (!side.value().queue().try_push(next)) {
        }
      }
      ::_exit(0);
    }
    drain_some(resume + 20000);
    int status = 0;
    ASSERT_EQ(::waitpid(replacement, &status, 0), replacement);
    EXPECT_EQ(WEXITSTATUS(status), 0);
    EXPECT_EQ(bad, 0U) << "round " << round;
  }
}

TEST(CrashSpscRing, KillingTheConsumerLosesAtMostTheMessageItWasHandling) {
  for (int round = 0; round < kRepeats; ++round) {
    const name_cleanup name(unique_name("crash_cons"));
    auto created = shared_spsc_ring<msg, 256>::create(name.name());
    ASSERT_TRUE(created.has_value());
    spsc_ring<msg, 256>& ring = created.value().queue();
    shared_counters* counters = make_shared_counters();
    ASSERT_NE(counters, nullptr);

    const pid_t consumer = ::fork();
    ASSERT_GE(consumer, 0);
    if (consumer == 0) {
      auto side = shared_spsc_ring<msg, 256>::open(name.name());
      if (!side)
        ::_exit(2);
      std::uint64_t expected = 0;
      for (;;) {
        msg m{};
        if (!side.value().queue().try_pop(m))
          continue;
        if (m.seq != expected || !valid(m))
          ::_exit(3);
        ++expected;
        counters->last_seen.store(m.seq + 1, std::memory_order_release);  // "handled up to here"
      }
    }

    std::uint64_t next = 0;
    // Pushes until `target` messages were sent. If the ring stays full and `watched` has exited on
    // its own, the test fails instead of waiting forever.
    pid_t watched = consumer;
    int watched_status = 0;
    bool watched_exited = false;
    const auto produce_until = [&](std::uint64_t target) {
      while (next < target && !watched_exited) {
        if (ring.try_push(make_msg(next))) {
          ++next;
        } else if (::waitpid(watched, &watched_status, WNOHANG) == watched) {
          watched_exited = true;
        }
      }
    };
    produce_until(50000);
    ASSERT_FALSE(watched_exited) << "consumer exited by itself, status " << watched_status;
    random_pause(static_cast<unsigned>(round));
    kill_and_reap(consumer);
    const std::uint64_t handled = counters->last_seen.load(std::memory_order_acquire);

    // The new consumer sees an intact, contiguous stream. It may start with the message the dead
    // consumer had taken but not yet acknowledged (delivered twice), or one later (lost).
    const pid_t replacement = ::fork();
    ASSERT_GE(replacement, 0);
    if (replacement == 0) {
      auto side = shared_spsc_ring<msg, 256>::open(name.name());
      if (!side)
        ::_exit(2);
      std::uint64_t expected = ~0ULL;
      for (std::uint64_t got = 0; got < 20000;) {
        msg m{};
        if (!side.value().queue().try_pop(m))
          continue;
        if (!valid(m))
          ::_exit(3);
        if (got == 0) {
          counters->first_seen.store(m.seq, std::memory_order_release);
          expected = m.seq;
        }
        if (m.seq != expected)
          ::_exit(4);
        ++expected;
        ++got;
      }
      ::_exit(0);
    }
    watched = replacement;
    produce_until(next + 20000);  // exactly what the replacement will consume
    ASSERT_FALSE(watched_exited) << "replacement exited early, status " << watched_status;
    int status = 0;
    ASSERT_EQ(::waitpid(replacement, &status, 0), replacement);
    ASSERT_TRUE(WIFEXITED(status)) << "round " << round;
    ASSERT_EQ(WEXITSTATUS(status), 0) << "round " << round << " replacement exit code";
    const std::uint64_t first = counters->first_seen.load(std::memory_order_acquire);
    EXPECT_GE(first + 0, handled > 0 ? handled - 1 : 0) << "round " << round;
    EXPECT_LE(first, handled + 1) << "round " << round;
    ::munmap(counters, sizeof(shared_counters));
  }
}

TEST(CrashMessageRing, AReservedButUncommittedMessageIsNeverSeen) {
  const name_cleanup name(unique_name("crash_resv"));
  auto created = shared_spsc_msg_ring::create(name.name(), 1024);
  ASSERT_TRUE(created.has_value());
  spsc_msg_ring& ring = created.value().ring();

  int pipe_fds[2];
  ASSERT_EQ(::pipe(pipe_fds), 0);
  const pid_t producer = ::fork();
  ASSERT_GE(producer, 0);
  if (producer == 0) {
    auto side = shared_spsc_msg_ring::open(name.name());
    if (!side)
      ::_exit(2);
    span<std::byte> room = side.value().ring().reserve(100);
    if (!room)
      ::_exit(3);
    std::memset(room.data(), 0xEE, 100);  // written, but never committed
    const char go = 1;
    if (::write(pipe_fds[1], &go, 1) != 1)
      ::_exit(4);
    for (;;) {
      ::pause();
    }
  }
  char go = 0;
  ASSERT_EQ(::read(pipe_fds[0], &go, 1), 1);
  kill_and_reap(producer);
  EXPECT_FALSE(ring.peek());
  EXPECT_EQ(ring.size_approx(), 0U);

  // A replacement producer starts clean and its messages arrive without a trace of the old one.
  const pid_t replacement = ::fork();
  ASSERT_GE(replacement, 0);
  if (replacement == 0) {
    auto side = shared_spsc_msg_ring::open(name.name());
    if (!side)
      ::_exit(2);
    for (int i = 0; i < 10; ++i) {
      const std::string text = "message " + std::to_string(i);
      while (!side.value().ring().try_push(text.data(), text.size())) {
      }
    }
    ::_exit(0);
  }
  for (int i = 0; i < 10;) {
    span<const std::byte> m = ring.peek();
    if (!m)
      continue;
    const std::string got(reinterpret_cast<const char*>(m.data()), m.size());
    EXPECT_EQ(got, "message " + std::to_string(i));
    ring.release();
    ++i;
  }
  int status = 0;
  ASSERT_EQ(::waitpid(replacement, &status, 0), replacement);
  EXPECT_EQ(WEXITSTATUS(status), 0);
  ::close(pipe_fds[0]);
  ::close(pipe_fds[1]);
}

TEST(CrashMessageRing, KillingTheProducerNeverExposesATornRecord) {
  for (int round = 0; round < kRepeats; ++round) {
    const name_cleanup name(unique_name("crash_msg"));
    auto created = shared_spsc_msg_ring::create(name.name(), 2048);
    ASSERT_TRUE(created.has_value());
    spsc_msg_ring& ring = created.value().ring();
    const std::size_t max_len = ring.max_message_size();

    const pid_t producer = ::fork();
    ASSERT_GE(producer, 0);
    if (producer == 0) {
      auto side = shared_spsc_msg_ring::open(name.name());
      if (!side)
        ::_exit(2);
      for (std::uint64_t seq = 0;; ++seq) {
        const std::size_t len = length_for(seq, max_len);
        span<std::byte> room = side.value().ring().reserve(len);
        while (!room) {
          room = side.value().ring().reserve(len);
        }
        for (std::size_t i = 0; i < len; ++i) {
          room.data()[i] = static_cast<std::byte>(byte_for(seq, i));
        }
        side.value().ring().commit();
      }
    }

    std::uint64_t seq = 0;
    std::uint64_t bad = 0;
    const auto consume = [&](bool until_empty, std::uint64_t at_least) {
      for (;;) {
        span<const std::byte> m = ring.peek();
        if (!m) {
          if (until_empty && seq >= at_least)
            return;
          continue;
        }
        bool ok = m.size() == length_for(seq, max_len);
        for (std::size_t i = 0; ok && i < m.size(); ++i) {
          ok = static_cast<unsigned char>(m.data()[i]) == byte_for(seq, i);
        }
        bad += ok ? 0U : 1U;
        ring.release();
        ++seq;
        if (!until_empty && seq >= at_least)
          return;
      }
    };
    consume(false, 20000);
    random_pause(static_cast<unsigned>(round) + 100);
    kill_and_reap(producer);
    // Drain what was published; then the ring must look empty, not half-full.
    for (;;) {
      span<const std::byte> m = ring.peek();
      if (!m)
        break;
      bool ok = m.size() == length_for(seq, max_len);
      for (std::size_t i = 0; ok && i < m.size(); ++i) {
        ok = static_cast<unsigned char>(m.data()[i]) == byte_for(seq, i);
      }
      bad += ok ? 0U : 1U;
      ring.release();
      ++seq;
    }
    ASSERT_EQ(bad, 0U) << "round " << round;
    EXPECT_EQ(ring.size_approx(), 0U);

    const std::uint64_t resume = seq;
    const pid_t replacement = ::fork();
    ASSERT_GE(replacement, 0);
    if (replacement == 0) {
      auto side = shared_spsc_msg_ring::open(name.name());
      if (!side)
        ::_exit(2);
      for (std::uint64_t s = resume; s < resume + 5000; ++s) {
        const std::size_t len = length_for(s, max_len);
        span<std::byte> room = side.value().ring().reserve(len);
        while (!room) {
          room = side.value().ring().reserve(len);
        }
        for (std::size_t i = 0; i < len; ++i) {
          room.data()[i] = static_cast<std::byte>(byte_for(s, i));
        }
        side.value().ring().commit();
      }
      ::_exit(0);
    }
    consume(false, resume + 5000);
    int status = 0;
    ASSERT_EQ(::waitpid(replacement, &status, 0), replacement);
    EXPECT_EQ(WEXITSTATUS(status), 0);
    EXPECT_EQ(bad, 0U) << "round " << round;
  }
}

TEST(CrashBroadcastRing, KillingTheWriterNeverExposesATornElementAndANewWriterContinues) {
  for (int round = 0; round < kRepeats; ++round) {
    const name_cleanup name(unique_name("crash_spmc"));
    auto created = shared_spmc_ring<msg, 64>::create(name.name());
    ASSERT_TRUE(created.has_value());
    spmc_ring<msg, 64>& ring = created.value().queue();
    spmc_reader reader = ring.join_latest();

    const pid_t writer = ::fork();
    ASSERT_GE(writer, 0);
    if (writer == 0) {
      auto side = shared_spmc_ring<msg, 64>::open(name.name());
      if (!side)
        ::_exit(2);
      for (std::uint64_t i = 0;; ++i) {
        side.value().queue().publish(make_msg(i));
      }
    }
    std::uint64_t last = 0;
    bool any = false;
    std::uint64_t bad = 0;
    const auto read_for = [&](std::chrono::milliseconds duration) {
      const auto end = std::chrono::steady_clock::now() + duration;
      while (std::chrono::steady_clock::now() < end) {
        msg m{};
        if (ring.try_read(reader, m) != read_status::ok)
          continue;
        bad += valid(m) ? 0U : 1U;
        bad += (any && m.seq <= last) ? 1 : 0;
        any = true;
        last = m.seq;
      }
    };
    read_for(std::chrono::milliseconds(20));
    random_pause(static_cast<unsigned>(round) + 200);
    kill_and_reap(writer);
    read_for(std::chrono::milliseconds(5));  // whatever the dead writer left behind

    // A new writer takes over at the head and the reader keeps receiving consistent elements.
    const std::uint64_t base = ring.published() + 1000000;
    const pid_t replacement = ::fork();
    ASSERT_GE(replacement, 0);
    if (replacement == 0) {
      auto side = shared_spmc_ring<msg, 64>::open(name.name());
      if (!side)
        ::_exit(2);
      for (std::uint64_t i = 0; i < 5000; ++i) {
        side.value().queue().publish(make_msg(base + i));
      }
      ::_exit(0);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(replacement, &status, 0), replacement);
    EXPECT_EQ(WEXITSTATUS(status), 0);
    bool saw_new = false;
    for (;;) {
      msg m{};
      const read_status st = ring.try_read(reader, m);
      if (st == read_status::empty)
        break;
      if (st != read_status::ok)
        continue;
      bad += valid(m) ? 0U : 1U;
      saw_new = saw_new || m.seq >= base;
    }
    EXPECT_EQ(bad, 0U) << "round " << round;
    EXPECT_TRUE(saw_new) << "round " << round;
  }
}

TEST(CrashMpmcRing, KillingAProducerNeverCorruptsWhatOtherParticipantsSee) {
  // A producer that dies between claiming a slot and publishing it can stall consumers; that is a
  // documented limitation. What must hold regardless: no torn message and no duplicate ever shows
  // up, and the elements that do arrive from each producer are in order.
  for (int round = 0; round < kRepeats; ++round) {
    const name_cleanup name(unique_name("crash_mpmc"));
    auto created = shared_mpmc_ring<msg, 64>::create(name.name());
    ASSERT_TRUE(created.has_value());
    mpmc_ring<msg, 64>& ring = created.value().queue();

    std::vector<pid_t> producers;
    for (std::uint32_t p = 0; p < 2; ++p) {
      const pid_t pid = ::fork();
      ASSERT_GE(pid, 0);
      if (pid == 0) {
        auto side = shared_mpmc_ring<msg, 64>::open(name.name());
        if (!side)
          ::_exit(2);
        for (std::uint64_t i = 0;; ++i) {
          const msg m = make_msg((static_cast<std::uint64_t>(p) << 32) | i);
          while (!side.value().queue().try_push(m)) {
          }
        }
      }
      producers.push_back(pid);
    }
    std::vector<std::int64_t> last(2, -1);
    std::uint64_t bad = 0;
    const auto drain_for = [&](std::chrono::milliseconds duration) {
      const auto end = std::chrono::steady_clock::now() + duration;
      while (std::chrono::steady_clock::now() < end) {
        msg m{};
        if (!ring.try_pop(m))
          continue;
        const std::uint64_t p = m.seq >> 32;
        const auto i = static_cast<std::int64_t>(m.seq & 0xFFFFFFFFU);
        if (!valid(m) || p >= 2 || i <= last[p]) {
          ++bad;
          continue;
        }
        last[p] = i;
      }
    };
    drain_for(std::chrono::milliseconds(15));
    random_pause(static_cast<unsigned>(round) + 300);
    kill_and_reap(producers[0]);
    drain_for(std::chrono::milliseconds(15));  // may stall at the dead producer's slot
    kill_and_reap(producers[1]);
    drain_for(std::chrono::milliseconds(5));
    EXPECT_EQ(bad, 0U) << "round " << round;
  }
}

}  // namespace
}  // namespace shmring
