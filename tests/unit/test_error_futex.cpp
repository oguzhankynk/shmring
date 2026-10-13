#include "shmring/error.hpp"
#include "shmring/futex.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include <pthread.h>
#include <signal.h>

namespace shmring {
namespace {

using std::chrono::milliseconds;

TEST(ErrorTest, EveryCodeHasAName) {
  for (int code = static_cast<int>(errc::invalid_argument);
       code <= static_cast<int>(errc::bad_cache_line);
       ++code) {
    const char* name = to_string(static_cast<errc>(code));
    ASSERT_NE(name, nullptr);
    EXPECT_STRNE(name, "unknown") << code;
  }
  EXPECT_STREQ(to_string(static_cast<errc>(9999)), "unknown");
}

TEST(ResultTest, HoldsEitherAValueOrAnError) {
  result<int> ok = 42;
  EXPECT_TRUE(ok.has_value());
  EXPECT_TRUE(static_cast<bool>(ok));
  EXPECT_EQ(ok.value(), 42);
  ok.value() = 43;
  const result<int>& view = ok;
  EXPECT_EQ(view.value(), 43);

  result<int> failed = error{errc::not_found, 2};
  EXPECT_FALSE(failed.has_value());
  EXPECT_FALSE(static_cast<bool>(failed));
  EXPECT_EQ(failed.error().code, errc::not_found);
  EXPECT_EQ(failed.error().sys_errno, 2);

  result<std::string> text = std::string("moved");
  const std::string taken = std::move(text).value();
  EXPECT_EQ(taken, "moved");
}

TEST(ResultDeathTest, AccessingTheWrongAlternativeTerminates) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  const result<int> failed = error{errc::timed_out};
  const result<int> ok = 1;
  EXPECT_DEATH((void)failed.value(), "");
  EXPECT_DEATH((void)ok.error(), "");
}

TEST(FutexTest, DoesNotSleepWhenTheValueDiffers) {
  std::atomic<std::uint32_t> word{5};
  EXPECT_EQ(futex_wait(&word, 4), futex_result::value_mismatch);
}

TEST(FutexTest, TimesOutWhenNobodyWakes) {
  std::atomic<std::uint32_t> word{0};
  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(futex_wait(&word, 0, milliseconds(20)), futex_result::timed_out);
  EXPECT_GE(std::chrono::steady_clock::now() - start, milliseconds(20));
  EXPECT_EQ(futex_wait(&word, 0, milliseconds(0)), futex_result::timed_out);
  EXPECT_EQ(futex_wait(&word, 0, std::chrono::nanoseconds(-5)), futex_result::timed_out);
}

TEST(FutexTest, WakeEndsTheWaitOfASleepingThread) {
  std::atomic<std::uint32_t> word{0};
  EXPECT_EQ(futex_wake(&word, 10), 0);

  std::atomic<bool> started{false};
  std::atomic<bool> finished{false};
  std::atomic<futex_result> outcome{futex_result::failed};
  std::thread sleeper([&] {
    started.store(true);
    outcome.store(futex_wait(&word, 0, std::chrono::seconds(30)));
    finished.store(true);
  });
  while (!started.load()) {
  }
  // The thread may not have reached futex_wait yet. Changing the word before it does makes the
  // wait return value_mismatch; changing it after makes futex_wake return the sleeper.
  word.store(1);
  int woken_total = 0;
  while (!finished.load()) {
    const int woken = futex_wake(&word, 1);
    EXPECT_GE(woken, 0);
    woken_total += woken;
    std::this_thread::sleep_for(milliseconds(1));
  }
  sleeper.join();
  const futex_result result = outcome.load();
  EXPECT_TRUE(result == futex_result::woken || result == futex_result::value_mismatch);
  if (result == futex_result::woken) {
    EXPECT_EQ(woken_total, 1);
  }
}

void ignore_signal(int) {}

TEST(FutexTest, ReportsAnInterruptingSignal) {
  struct sigaction action {};
  action.sa_handler = ignore_signal;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;  // no SA_RESTART: the wait must return
  struct sigaction previous {};
  ASSERT_EQ(sigaction(SIGUSR1, &action, &previous), 0);

  std::atomic<std::uint32_t> word{0};
  std::atomic<bool> interrupted{false};
  std::thread waiter([&] {
    while (!interrupted.load()) {
      if (futex_wait(&word, 0, milliseconds(2000)) == futex_result::interrupted) {
        interrupted.store(true);
      }
    }
  });
  while (!interrupted.load()) {
    pthread_kill(waiter.native_handle(), SIGUSR1);
    std::this_thread::sleep_for(milliseconds(1));
  }
  waiter.join();
  sigaction(SIGUSR1, &previous, nullptr);
  EXPECT_TRUE(interrupted.load());
}

TEST(FutexTest, ReportsAFailedSystemCall) {
  const std::atomic<std::uint32_t>* invalid = nullptr;
  EXPECT_EQ(futex_wait(invalid, 0, milliseconds(1)), futex_result::failed);
  EXPECT_EQ(futex_wake(invalid, 1), -1);
}

}  // namespace
}  // namespace shmring
