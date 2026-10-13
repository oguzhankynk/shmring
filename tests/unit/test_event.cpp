#include "shmring/blocking_spsc_ring.hpp"
#include "shmring/event.hpp"
#include "shmring/shared_queue.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <vector>

#include <unistd.h>

#include "support.hpp"

namespace shmring {
namespace {

using std::chrono::milliseconds;

TEST(EventTest, WaitReturnsImmediatelyWhenAlreadyTrue) {
  event ev;
  EXPECT_TRUE(ev.wait([] { return true; }, milliseconds(0)));
}

TEST(EventTest, WaitTimesOutWhenNothingHappens) {
  event ev;
  const auto start = std::chrono::steady_clock::now();
  EXPECT_FALSE(ev.wait([] { return false; }, milliseconds(40)));
  EXPECT_GE(std::chrono::steady_clock::now() - start, milliseconds(40));
  EXPECT_FALSE(ev.wait([] { return false; }, milliseconds(0)));
}

TEST(EventTest, NotifyWakesASleepingWaiter) {
  event ev;
  std::atomic<bool> flag{false};
  std::atomic<bool> woke{false};
  std::thread waiter([&] { woke = ev.wait([&] { return flag.load(std::memory_order_acquire); }); });
  std::this_thread::sleep_for(milliseconds(30));
  flag.store(true, std::memory_order_release);
  ev.notify();
  waiter.join();
  EXPECT_TRUE(woke);
}

TEST(EventTest, ToleratesNotificationsThatDoNotMakeTheConditionTrue) {
  event ev;
  std::atomic<int> count{0};
  std::thread waiter([&] { ev.wait([&] { return count.load(std::memory_order_acquire) >= 3; }); });
  for (int i = 0; i < 3; ++i) {
    std::this_thread::sleep_for(milliseconds(10));
    count.fetch_add(1, std::memory_order_release);
    ev.notify();
  }
  waiter.join();
  EXPECT_EQ(count.load(), 3);
}

TEST(BlockingRingTest, TimeoutsOnEmptyAndFull) {
  blocking_spsc_ring<int, 2> ring;
  int v = 0;
  EXPECT_FALSE(ring.pop(v, milliseconds(20)));
  EXPECT_TRUE(ring.push(1));
  EXPECT_TRUE(ring.push(2));
  EXPECT_FALSE(ring.push(3, milliseconds(20)));
  EXPECT_TRUE(ring.pop(v, milliseconds(20)));
  EXPECT_EQ(v, 1);
}

TEST(BlockingRingTest, ManyBlockWakeCyclesLoseNoWakeup) {
  constexpr std::uint64_t total = 200000;
  blocking_spsc_ring<std::uint64_t, 2> ring;  // tiny ring: both sides block constantly
  std::thread producer([&] {
    for (std::uint64_t i = 0; i < total; ++i) {
      ring.push(i);
    }
  });
  std::uint64_t bad = 0;
  for (std::uint64_t expected = 0; expected < total; ++expected) {
    std::uint64_t v = ~0ULL;
    if (!ring.pop(v, std::chrono::seconds(10))) {
      ADD_FAILURE() << "stalled waiting for message " << expected;
      break;
    }
    bad += v == expected ? 0U : 1U;
  }
  producer.join();
  EXPECT_EQ(bad, 0U);
}

TEST(BlockingRingTest, WorksBetweenProcesses) {
  constexpr std::uint64_t total = 50000;
  const std::string name = "/shmring_test_block_" + std::to_string(::getpid());
  auto created = shared_blocking_spsc_ring<std::uint64_t, 4>::create(name);
  ASSERT_TRUE(created.has_value());
  const pid_t pid = ::fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    auto producer = shared_blocking_spsc_ring<std::uint64_t, 4>::open(name);
    if (!producer) {
      ::_exit(2);
    }
    for (std::uint64_t i = 0; i < total; ++i) {
      producer.value().queue().push(i);
    }
    ::_exit(0);
  }
  std::uint64_t bad = 0;
  for (std::uint64_t expected = 0; expected < total; ++expected) {
    std::uint64_t v = 0;
    if (!created.value().queue().pop(v, std::chrono::seconds(10))) {
      ADD_FAILURE() << "stalled waiting for message " << expected;
      break;
    }
    bad += v == expected ? 0U : 1U;
  }
  int status = 0;
  ::waitpid(pid, &status, 0);
  EXPECT_EQ(bad, 0U);
  (void)shared_blocking_spsc_ring<std::uint64_t, 4>::unlink(name);
}

TEST(EventTest, NotifyWakesEveryWaiter) {
  event ev;
  std::atomic<bool> flag{false};
  std::atomic<int> woke{0};
  std::vector<std::thread> waiters;
  for (int i = 0; i < 4; ++i) {
    waiters.emplace_back([&] {
      if (ev.wait([&] { return flag.load(std::memory_order_acquire); })) {
        woke.fetch_add(1);
      }
    });
  }
  std::this_thread::sleep_for(milliseconds(30));
  flag.store(true, std::memory_order_release);
  ev.notify();
  for (auto& t : waiters) {
    t.join();
  }
  EXPECT_EQ(woke.load(), 4);
}

TEST(EventTest, NotifyWithoutWaitersIsHarmless) {
  event ev;
  ev.notify();
  ev.notify(1);
  EXPECT_TRUE(ev.wait([] { return true; }));
}

TEST(EventTest, ADeadlineThatPassesWhileTheConditionBecameTrueStillReportsSuccess) {
  event ev;
  std::atomic<bool> flag{false};
  std::thread setter([&] {
    std::this_thread::sleep_for(milliseconds(10));
    flag.store(true, std::memory_order_release);  // no notify: only the timeout can end the sleep
  });
  EXPECT_TRUE(ev.wait([&] { return flag.load(std::memory_order_acquire); }, milliseconds(60)));
  setter.join();
}

TEST(BlockingRingTest, NonBlockingOperationsAndAccessors) {
  blocking_spsc_ring<int, 4> ring;
  EXPECT_EQ(ring.capacity(), 4U);
  int v = 0;
  EXPECT_FALSE(ring.try_pop(v));
  for (int i = 0; i < 4; ++i) {
    EXPECT_TRUE(ring.try_push(i));
  }
  EXPECT_FALSE(ring.try_push(4));
  EXPECT_EQ(ring.ring().size_approx(), 4U);
  EXPECT_TRUE(ring.try_pop(v));
  EXPECT_EQ(v, 0);
  EXPECT_FALSE(ring.push(9, std::chrono::nanoseconds::zero()) &&
               ring.push(10, std::chrono::nanoseconds::zero()));
}

}  // namespace
}  // namespace shmring
