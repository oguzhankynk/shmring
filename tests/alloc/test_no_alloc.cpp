// Checks that the fast path of every queue makes no heap allocation. The global allocation
// functions are replaced by counting versions, and each test measures the count around a loop of
// operations after all setup is done.
#include "shmring/blocking_spsc_ring.hpp"
#include "shmring/event.hpp"
#include "shmring/futex.hpp"
#include "shmring/mpmc_ring.hpp"
#include "shmring/shared_queue.hpp"
#include "shmring/shared_spsc_msg_ring.hpp"
#include "shmring/spmc_ring.hpp"
#include "shmring/spsc_msg_ring.hpp"
#include "shmring/spsc_ring.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <new>
#include <string>

#include "support.hpp"

namespace {

std::atomic<std::uint64_t> g_allocations{0};

void* counted_alloc(std::size_t size) {
  g_allocations.fetch_add(1, std::memory_order_relaxed);
  void* p = std::malloc(size == 0 ? 1 : size);
  if (p == nullptr) {
    throw std::bad_alloc();
  }
  return p;
}

void* counted_aligned_alloc(std::size_t size, std::size_t alignment) {
  g_allocations.fetch_add(1, std::memory_order_relaxed);
  void* p = std::aligned_alloc(alignment, (size + alignment - 1) / alignment * alignment);
  if (p == nullptr) {
    throw std::bad_alloc();
  }
  return p;
}

// Runs `body` and returns how many allocations it made.
template <typename Body>
std::uint64_t allocations_during(Body body) {
  const std::uint64_t before = g_allocations.load();
  body();
  return g_allocations.load() - before;
}

}  // namespace

void* operator new(std::size_t size) {
  return counted_alloc(size);
}
void* operator new[](std::size_t size) {
  return counted_alloc(size);
}
void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
  try {
    return counted_alloc(size);
  } catch (...) {
    return nullptr;
  }
}
void* operator new(std::size_t size, std::align_val_t alignment) {
  return counted_aligned_alloc(size, static_cast<std::size_t>(alignment));
}
void* operator new[](std::size_t size, std::align_val_t alignment) {
  return counted_aligned_alloc(size, static_cast<std::size_t>(alignment));
}
void operator delete(void* p) noexcept {
  std::free(p);
}
void operator delete[](void* p) noexcept {
  std::free(p);
}
void operator delete(void* p, std::size_t) noexcept {
  std::free(p);
}
void operator delete[](void* p, std::size_t) noexcept {
  std::free(p);
}
void operator delete(void* p, std::align_val_t) noexcept {
  std::free(p);
}
void operator delete[](void* p, std::align_val_t) noexcept {
  std::free(p);
}
void operator delete(void* p, std::size_t, std::align_val_t) noexcept {
  std::free(p);
}
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept {
  std::free(p);
}

namespace shmring {
namespace {

using testing_support::aligned_buffer;
using testing_support::name_cleanup;
using testing_support::unique_name;

constexpr int kIterations = 100000;

TEST(NoAllocation, TheCountingHookSeesRealAllocations) {
  EXPECT_GE(allocations_during([] {
              auto p = std::make_unique<int>(1);
              (void)p;
            }),
            1U);
  EXPECT_GE(allocations_during([] {
              std::string s(200, 'x');
              (void)s;
            }),
            1U);
}

TEST(NoAllocation, SpscRing) {
  auto ring = std::make_unique<spsc_ring<std::uint64_t, 64>>();
  std::array<std::uint64_t, 8> batch{};
  const std::uint64_t made = allocations_during([&] {
    std::uint64_t v = 0;
    for (int i = 0; i < kIterations; ++i) {
      ring->try_push(static_cast<std::uint64_t>(i));
      ring->try_pop(v);
      ring->try_push_n(batch.data(), batch.size());
      ring->try_pop_n(batch.data(), batch.size());
      (void)ring->size_approx();
    }
  });
  EXPECT_EQ(made, 0U);
}

TEST(NoAllocation, MessageRing) {
  aligned_buffer mem(spsc_msg_ring::required_bytes(1024));
  spsc_msg_ring ring = spsc_msg_ring::init(mem.get(), 1024);
  const std::array<char, 40> payload{};
  const std::uint64_t made = allocations_during([&] {
    for (int i = 0; i < kIterations; ++i) {
      auto room = ring.reserve(static_cast<std::size_t>(1 + i % 40));
      if (room) {
        ring.commit();
      }
      ring.try_push(payload.data(), payload.size());
      while (ring.peek()) {
        ring.release();
      }
    }
  });
  EXPECT_EQ(made, 0U);
}

TEST(NoAllocation, MpmcRing) {
  auto ring = std::make_unique<mpmc_ring<std::uint64_t, 64>>();
  const std::uint64_t made = allocations_during([&] {
    std::uint64_t v = 0;
    for (int i = 0; i < kIterations; ++i) {
      ring->try_push(static_cast<std::uint64_t>(i));
      ring->try_pop(v);
    }
  });
  EXPECT_EQ(made, 0U);
}

TEST(NoAllocation, BroadcastRing) {
  auto ring = std::make_unique<spmc_ring<std::array<std::uint64_t, 4>, 16>>();
  spmc_reader reader = ring->join_latest();
  const std::uint64_t made = allocations_during([&] {
    std::array<std::uint64_t, 4> v{};
    for (int i = 0; i < kIterations; ++i) {
      ring->publish(v);
      ring->try_read(reader, v);
    }
  });
  EXPECT_EQ(made, 0U);
}

TEST(NoAllocation, BlockingLayerWhenNobodyHasToSleep) {
  auto ring = std::make_unique<blocking_spsc_ring<std::uint64_t, 64>>();
  event ev;
  std::atomic<std::uint32_t> word{0};
  const std::uint64_t made = allocations_during([&] {
    std::uint64_t v = 0;
    for (int i = 0; i < kIterations; ++i) {
      ring->try_push(static_cast<std::uint64_t>(i));
      ring->push(static_cast<std::uint64_t>(i), std::chrono::seconds(1));
      ring->pop(v, std::chrono::seconds(1));
      ring->try_pop(v);
      ev.notify();
      ev.wait([] { return true; });
      futex_wait(&word, 1);  // value differs: returns at once
      futex_wake(&word, 1);
    }
  });
  EXPECT_EQ(made, 0U);
}

TEST(NoAllocation, QueuesInsideASharedMemorySegment) {
  const name_cleanup spsc_name(unique_name("na_spsc"));
  const name_cleanup msg_name(unique_name("na_msg"));
  auto spsc = shared_spsc_ring<std::uint64_t, 64>::create(spsc_name.name());
  auto msg = shared_spsc_msg_ring::create(msg_name.name(), 1024);
  ASSERT_TRUE(spsc.has_value());
  ASSERT_TRUE(msg.has_value());
  const std::uint64_t made = allocations_during([&] {
    std::uint64_t v = 0;
    for (int i = 0; i < kIterations; ++i) {
      spsc.value().queue().try_push(static_cast<std::uint64_t>(i));
      spsc.value().queue().try_pop(v);
      msg.value().ring().try_push(&v, sizeof(v));
      if (msg.value().ring().peek()) {
        msg.value().ring().release();
      }
    }
  });
  EXPECT_EQ(made, 0U);
}

TEST(NoAllocation, HeaderValidation) {
  alignas(cache_line) std::byte bytes[4096]{};
  const segment_expectation want{queue_kind::spsc_ring, 8, 8, 16};
  auto* h = new (bytes) segment_header(want, 1);
  h->state.store(static_cast<std::uint32_t>(init_state::ready));
  const std::uint64_t made = allocations_during([&] {
    for (int i = 0; i < kIterations; ++i) {
      (void)validate_header(bytes, sizeof(bytes), want);
    }
  });
  EXPECT_EQ(made, 0U);
}

}  // namespace
}  // namespace shmring
