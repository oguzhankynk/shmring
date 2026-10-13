// Single-operation costs measured with Google Benchmark: one push followed by one pop on a single
// thread, and two-thread streaming of 8-byte elements.
#include "shmring/mpmc_ring.hpp"
#include "shmring/spsc_msg_ring.hpp"
#include "shmring/spsc_ring.hpp"

#include <atomic>
#include <benchmark/benchmark.h>
#include <cstdint>
#include <memory>

#ifdef SHMRING_BENCH_HAVE_BOOST
#include <boost/lockfree/spsc_queue.hpp>
#endif

namespace {

using namespace shmring;

void SpscPushPop(benchmark::State& state) {
  auto ring = std::make_unique<spsc_ring<std::uint64_t, 1024>>();
  std::uint64_t v = 0;
  for (auto _ : state) {
    ring->try_push(v);
    ring->try_pop(v);
    benchmark::DoNotOptimize(v);
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(SpscPushPop);

void MpmcPushPop(benchmark::State& state) {
  auto ring = std::make_unique<mpmc_ring<std::uint64_t, 1024>>();
  std::uint64_t v = 0;
  for (auto _ : state) {
    ring->try_push(v);
    ring->try_pop(v);
    benchmark::DoNotOptimize(v);
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(MpmcPushPop);

void MsgRingPushPop(benchmark::State& state) {
  alignas(cache_line) static unsigned char memory[2 * cache_line + 4096];
  spsc_msg_ring ring = spsc_msg_ring::init(memory, 4096);
  std::uint64_t v = 0;
  for (auto _ : state) {
    ring.try_push(&v, sizeof(v));
    auto m = ring.peek();
    benchmark::DoNotOptimize(m.data());
    ring.release();
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(MsgRingPushPop);

#ifdef SHMRING_BENCH_HAVE_BOOST
void BoostPushPop(benchmark::State& state) {
  auto q = std::make_unique<
      boost::lockfree::spsc_queue<std::uint64_t, boost::lockfree::capacity<1024>>>();
  std::uint64_t v = 0;
  for (auto _ : state) {
    q->push(v);
    q->pop(v);
    benchmark::DoNotOptimize(v);
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BoostPushPop);
#endif

// Two threads: thread 0 produces, thread 1 consumes. Items are counted on the consumer.
template <typename Ring>
struct Shared {
  Ring ring;
  std::atomic<bool> stop{false};
};

void SpscStream(benchmark::State& state) {
  static std::unique_ptr<Shared<spsc_ring<std::uint64_t, 1024>>> shared;
  if (state.thread_index() == 0) {
    shared = std::make_unique<Shared<spsc_ring<std::uint64_t, 1024>>>();
  }
  std::int64_t items = 0;
  if (state.thread_index() == 0) {
    std::uint64_t v = 0;
    for (auto _ : state) {
      while (!shared->ring.try_push(v)) {
      }
      ++v;
    }
    shared->stop.store(true, std::memory_order_release);
  } else {
    std::uint64_t v = 0;
    for (auto _ : state) {
      // Only successful pops count. If the producer has finished, stop once the ring is drained.
      bool got = false;
      while (!(got = shared->ring.try_pop(v))) {
        if (shared->stop.load(std::memory_order_acquire) && shared->ring.size_approx() == 0) {
          break;
        }
      }
      if (got) {
        ++items;
      }
    }
  }
  if (state.thread_index() == 1) {
    state.SetItemsProcessed(items);
  }
}
BENCHMARK(SpscStream)->Threads(2)->UseRealTime();

}  // namespace

BENCHMARK_MAIN();
