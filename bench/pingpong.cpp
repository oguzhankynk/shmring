// Round-trip latency of an 8-byte message between two pinned processes (or threads, for the
// in-process baseline). The first side sends, the second echoes; every round trip is timed.
#include "shmring/shared_queue.hpp"
#include "shmring/shared_spsc_msg_ring.hpp"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <mqueue.h>
#include <unistd.h>

#include "common.hpp"

#ifdef SHMRING_BENCH_HAVE_BOOST
#include <boost/lockfree/spsc_queue.hpp>
#endif

namespace {

using namespace shmring;
using std::uint64_t;

constexpr int kQueueSlots = 1024;

// ---- shmring transports -------------------------------------------------------------------

struct SpscTransport {
  static constexpr bool threads = false;
  using ring = shared_spsc_ring<uint64_t, kQueueSlots>;
  explicit SpscTransport(const char*) {
    name_ab = bench::unique_name("pp_ab");
    name_ba = bench::unique_name("pp_ba");
    ab = std::make_unique<ring>(std::move(ring::create(name_ab).value()));
    ba = std::make_unique<ring>(std::move(ring::create(name_ba).value()));
  }
  ~SpscTransport() {
    (void)ring::unlink(name_ab);
    (void)ring::unlink(name_ba);
  }
  void a_send(uint64_t v) {
    while (!ab->queue().try_push(v)) bench::cpu_relax();
  }
  uint64_t a_recv() {
    uint64_t v;
    while (!ba->queue().try_pop(v)) bench::cpu_relax();
    return v;
  }
  void b_send(uint64_t v) {
    while (!ba->queue().try_push(v)) bench::cpu_relax();
  }
  uint64_t b_recv() {
    uint64_t v;
    while (!ab->queue().try_pop(v)) bench::cpu_relax();
    return v;
  }
  std::string name_ab, name_ba;
  std::unique_ptr<ring> ab, ba;
};

struct SpscThreadsTransport {
  static constexpr bool threads = true;
  explicit SpscThreadsTransport(const char*) {}
  using ring = spsc_ring<uint64_t, kQueueSlots>;
  void a_send(uint64_t v) {
    while (!ab.try_push(v)) bench::cpu_relax();
  }
  uint64_t a_recv() {
    uint64_t v;
    while (!ba.try_pop(v)) bench::cpu_relax();
    return v;
  }
  void b_send(uint64_t v) {
    while (!ba.try_push(v)) bench::cpu_relax();
  }
  uint64_t b_recv() {
    uint64_t v;
    while (!ab.try_pop(v)) bench::cpu_relax();
    return v;
  }
  ring ab, ba;
};

struct FutexTransport {
  static constexpr bool threads = false;
  using ring = shared_blocking_spsc_ring<uint64_t, kQueueSlots>;
  explicit FutexTransport(const char*) {
    name_ab = bench::unique_name("pf_ab");
    name_ba = bench::unique_name("pf_ba");
    ab = std::make_unique<ring>(std::move(ring::create(name_ab).value()));
    ba = std::make_unique<ring>(std::move(ring::create(name_ba).value()));
  }
  ~FutexTransport() {
    (void)ring::unlink(name_ab);
    (void)ring::unlink(name_ba);
  }
  void a_send(uint64_t v) {
    ab->queue().push(v);
  }
  uint64_t a_recv() {
    uint64_t v;
    ba->queue().pop(v);
    return v;
  }
  void b_send(uint64_t v) {
    ba->queue().push(v);
  }
  uint64_t b_recv() {
    uint64_t v;
    ab->queue().pop(v);
    return v;
  }
  std::string name_ab, name_ba;
  std::unique_ptr<ring> ab, ba;
};

struct MsgTransport {
  static constexpr bool threads = false;
  explicit MsgTransport(const char*) {
    name_ab = bench::unique_name("pm_ab");
    name_ba = bench::unique_name("pm_ba");
    ab = std::make_unique<shared_spsc_msg_ring>(
        std::move(shared_spsc_msg_ring::create(name_ab, 4096).value()));
    ba = std::make_unique<shared_spsc_msg_ring>(
        std::move(shared_spsc_msg_ring::create(name_ba, 4096).value()));
  }
  ~MsgTransport() {
    (void)shared_spsc_msg_ring::unlink(name_ab);
    (void)shared_spsc_msg_ring::unlink(name_ba);
  }
  static void send(spsc_msg_ring& r, uint64_t v) {
    while (!r.try_push(&v, sizeof(v))) bench::cpu_relax();
  }
  static uint64_t recv(spsc_msg_ring& r) {
    for (;;) {
      auto m = r.peek();
      if (m) {
        uint64_t v;
        std::memcpy(&v, m.data(), sizeof(v));
        r.release();
        return v;
      }
      bench::cpu_relax();
    }
  }
  void a_send(uint64_t v) {
    send(ab->ring(), v);
  }
  uint64_t a_recv() {
    return recv(ba->ring());
  }
  void b_send(uint64_t v) {
    send(ba->ring(), v);
  }
  uint64_t b_recv() {
    return recv(ab->ring());
  }
  std::string name_ab, name_ba;
  std::unique_ptr<shared_spsc_msg_ring> ab, ba;
};

struct MpmcTransport {
  static constexpr bool threads = false;
  using ring = shared_mpmc_ring<uint64_t, kQueueSlots>;
  explicit MpmcTransport(const char*) {
    name_ab = bench::unique_name("px_ab");
    name_ba = bench::unique_name("px_ba");
    ab = std::make_unique<ring>(std::move(ring::create(name_ab).value()));
    ba = std::make_unique<ring>(std::move(ring::create(name_ba).value()));
  }
  ~MpmcTransport() {
    (void)ring::unlink(name_ab);
    (void)ring::unlink(name_ba);
  }
  void a_send(uint64_t v) {
    while (!ab->queue().try_push(v)) bench::cpu_relax();
  }
  uint64_t a_recv() {
    uint64_t v;
    while (!ba->queue().try_pop(v)) bench::cpu_relax();
    return v;
  }
  void b_send(uint64_t v) {
    while (!ba->queue().try_push(v)) bench::cpu_relax();
  }
  uint64_t b_recv() {
    uint64_t v;
    while (!ab->queue().try_pop(v)) bench::cpu_relax();
    return v;
  }
  std::string name_ab, name_ba;
  std::unique_ptr<ring> ab, ba;
};

// ---- kernel transports --------------------------------------------------------------------

struct PipeTransport {
  static constexpr bool threads = false;
  explicit PipeTransport(const char*) {
    if (::pipe(ab) != 0 || ::pipe(ba) != 0)
      std::abort();
  }
  void a_send(uint64_t v) {
    if (::write(ab[1], &v, sizeof(v)) != sizeof(v))
      std::abort();
  }
  uint64_t a_recv() {
    uint64_t v;
    if (::read(ba[0], &v, sizeof(v)) != sizeof(v))
      std::abort();
    return v;
  }
  void b_send(uint64_t v) {
    if (::write(ba[1], &v, sizeof(v)) != sizeof(v))
      std::abort();
  }
  uint64_t b_recv() {
    uint64_t v;
    if (::read(ab[0], &v, sizeof(v)) != sizeof(v))
      std::abort();
    return v;
  }
  int ab[2], ba[2];
};

template <int Type>
struct SocketTransport {
  static constexpr bool threads = false;
  explicit SocketTransport(const char*) {
    if (::socketpair(AF_UNIX, Type, 0, fds) != 0)
      std::abort();
  }
  void a_send(uint64_t v) {
    if (::send(fds[0], &v, sizeof(v), 0) != sizeof(v))
      std::abort();
  }
  uint64_t a_recv() {
    uint64_t v;
    if (::recv(fds[0], &v, sizeof(v), 0) != sizeof(v))
      std::abort();
    return v;
  }
  void b_send(uint64_t v) {
    if (::send(fds[1], &v, sizeof(v), 0) != sizeof(v))
      std::abort();
  }
  uint64_t b_recv() {
    uint64_t v;
    if (::recv(fds[1], &v, sizeof(v), 0) != sizeof(v))
      std::abort();
    return v;
  }
  int fds[2];
};

struct MqTransport {
  static constexpr bool threads = false;
  explicit MqTransport(const char*) {
    mq_attr attr{};
    attr.mq_maxmsg = 10;
    attr.mq_msgsize = sizeof(uint64_t);
    name_ab = bench::unique_name("mq_ab");
    name_ba = bench::unique_name("mq_ba");
    ab = ::mq_open(name_ab.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600, &attr);
    ba = ::mq_open(name_ba.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600, &attr);
    if (ab == -1 || ba == -1) {
      std::perror("mq_open");
      std::abort();
    }
  }
  ~MqTransport() {
    ::mq_unlink(name_ab.c_str());
    ::mq_unlink(name_ba.c_str());
  }
  static void send(mqd_t q, uint64_t v) {
    if (::mq_send(q, reinterpret_cast<const char*>(&v), sizeof(v), 0) != 0)
      std::abort();
  }
  static uint64_t recv(mqd_t q) {
    uint64_t v;
    if (::mq_receive(q, reinterpret_cast<char*>(&v), sizeof(v), nullptr) !=
        static_cast<ssize_t>(sizeof(v)))
      std::abort();
    return v;
  }
  void a_send(uint64_t v) {
    send(ab, v);
  }
  uint64_t a_recv() {
    return recv(ba);
  }
  void b_send(uint64_t v) {
    send(ba, v);
  }
  uint64_t b_recv() {
    return recv(ab);
  }
  std::string name_ab, name_ba;
  mqd_t ab, ba;
};

#ifdef SHMRING_BENCH_HAVE_BOOST
struct BoostTransport {
  static constexpr bool threads = true;
  explicit BoostTransport(const char*) {}
  using queue = boost::lockfree::spsc_queue<uint64_t, boost::lockfree::capacity<kQueueSlots>>;
  void a_send(uint64_t v) {
    while (!ab.push(v)) bench::cpu_relax();
  }
  uint64_t a_recv() {
    uint64_t v;
    while (!ba.pop(v)) bench::cpu_relax();
    return v;
  }
  void b_send(uint64_t v) {
    while (!ba.push(v)) bench::cpu_relax();
  }
  uint64_t b_recv() {
    uint64_t v;
    while (!ab.pop(v)) bench::cpu_relax();
    return v;
  }
  queue ab, ba;
};
#endif

// ---- driver -------------------------------------------------------------------------------

struct Options {
  int cpu_a;
  int cpu_b;
  uint64_t warmup;
  uint64_t iterations;
  int repetitions;
  int fast_repetitions;
};

struct Sample {
  uint64_t p50, p99, p999, max, count;
  bool ok;
};

template <typename Transport>
Sample measure_once(const char* label, const Options& opt) {
  Transport t(label);
  const uint64_t total = opt.warmup + opt.iterations;
  auto echo = [&] {
    bench::pin_to_cpu(opt.cpu_b);
    for (uint64_t i = 0; i < total; ++i) {
      const uint64_t v = t.b_recv();
      t.b_send(v);
    }
  };

  pid_t child = 0;
  std::thread echo_thread;
  if constexpr (Transport::threads) {
    echo_thread = std::thread(echo);
  } else {
    child = bench::spawn(echo);
  }

  bench::pin_to_cpu(opt.cpu_a);
  static bench::histogram hist;  // static: allocated before measuring starts
  hist = bench::histogram();
  bool ok = true;
  for (uint64_t i = 0; i < total; ++i) {
    const uint64_t t0 = bench::now_ns();
    t.a_send(i);
    const uint64_t v = t.a_recv();
    const uint64_t t1 = bench::now_ns();
    if (v != i)
      ok = false;
    if (i >= opt.warmup)
      hist.record(t1 - t0);
  }
  if constexpr (Transport::threads) {
    echo_thread.join();
  } else if (bench::wait_for(child) != 0) {
    ok = false;
  }
  return {hist.percentile(0.50),
          hist.percentile(0.99),
          hist.percentile(0.999),
          hist.max(),
          hist.count(),
          ok};
}

uint64_t median_of(std::vector<uint64_t> v) {
  std::sort(v.begin(), v.end());
  return v[v.size() / 2];
}

struct Entry {
  const char* label;
  const char* note;
  Sample (*measure)(const char*, const Options&);
  int repetitions;
  std::vector<uint64_t> p50, p99, p999, max;
  bool ok = true;
};

template <typename Transport>
Entry entry(const char* label, const char* note, int repetitions) {
  return Entry{label, note, &measure_once<Transport>, repetitions, {}, {}, {}, {}, true};
}

void print_entry(const Entry& e) {
  const auto [lo, hi] = std::minmax_element(e.p50.begin(), e.p50.end());
  std::printf("| %-34s | %7llu | %7llu | %7llu | %7llu | %7llu | %9llu | %4d | %s%s |\n",
              e.label,
              static_cast<unsigned long long>(*lo),
              static_cast<unsigned long long>(median_of(e.p50)),
              static_cast<unsigned long long>(*hi),
              static_cast<unsigned long long>(median_of(e.p99)),
              static_cast<unsigned long long>(median_of(e.p999)),
              static_cast<unsigned long long>(median_of(e.max)),
              e.repetitions,
              e.note,
              e.ok ? "" : " (ERRORS)");
}

}  // namespace

int main(int argc, char** argv) {
  ::alarm(1800);
  Options opt{bench::env_int("BENCH_CPU_A", 0),
              bench::env_int("BENCH_CPU_B", 2),
              static_cast<uint64_t>(bench::env_int("BENCH_WARMUP", 20000)),
              static_cast<uint64_t>(bench::env_int("BENCH_ITERATIONS", 200000)),
              bench::env_int("BENCH_REPETITIONS", 5),
              bench::env_int("BENCH_FAST_REPETITIONS", 21)};
  if (argc > 1)
    opt.iterations = static_cast<uint64_t>(std::atoll(argv[1]));

  std::printf(
      "Round-trip latency of an 8-byte message, in nanoseconds (cpu %d <-> cpu %d, %llu samples "
      "per "
      "repetition).\n"
      "Repetitions are taken round-robin across the rows. 'p50 min', 'p50 med' and 'p50 max' are "
      "the\n"
      "lowest, median and highest p50 seen in any repetition; p99, p99.9 and max are medians over "
      "the\n"
      "repetitions. Transports that spin get more repetitions than transports that sleep.\n\n",
      opt.cpu_a,
      opt.cpu_b,
      static_cast<unsigned long long>(opt.iterations));
  std::printf("| %-34s | %7s | %7s | %7s | %7s | %7s | %9s | %4s | %s |\n",
              "transport",
              "p50 min",
              "p50 med",
              "p50 max",
              "p99",
              "p99.9",
              "max",
              "reps",
              "note");
  std::printf("|%s|%s|%s|%s|%s|%s|%s|%s|%s|\n",
              std::string(36, '-').c_str(),
              std::string(9, '-').c_str(),
              std::string(9, '-').c_str(),
              std::string(9, '-').c_str(),
              std::string(9, '-').c_str(),
              std::string(9, '-').c_str(),
              std::string(11, '-').c_str(),
              std::string(6, '-').c_str(),
              std::string(6, '-').c_str());

  const int slow = opt.repetitions;       // transports that sleep: seconds per repetition
  const int fast = opt.fast_repetitions;  // transports that spin: fractions of a second
  std::vector<Entry> entries;
  entries.push_back(entry<SpscTransport>("shmring spsc_ring (busy-poll)", "processes", fast));
  entries.push_back(
      entry<SpscThreadsTransport>("shmring spsc_ring (busy-poll)", "threads, in-process", fast));
  entries.push_back(entry<MsgTransport>("shmring spsc_msg_ring (busy-poll)", "processes", fast));
  entries.push_back(entry<MpmcTransport>("shmring mpmc_ring (busy-poll)", "processes", fast));
  entries.push_back(entry<FutexTransport>("shmring blocking ring (futex)", "processes", slow));
  entries.push_back(entry<PipeTransport>("pipe", "processes", slow));
  entries.push_back(
      entry<SocketTransport<SOCK_SEQPACKET>>("unix socket (SOCK_SEQPACKET)", "processes", slow));
  entries.push_back(
      entry<SocketTransport<SOCK_STREAM>>("unix socket (SOCK_STREAM)", "processes", slow));
  entries.push_back(entry<MqTransport>("POSIX message queue", "processes", slow));
#ifdef SHMRING_BENCH_HAVE_BOOST
  entries.push_back(
      entry<BoostTransport>("boost::lockfree::spsc_queue", "threads, in-process only", fast));
#endif
  const char* only = std::getenv("BENCH_FILTER");
  // Repetition-major order: every round measures all transports back to back, so each transport
  // sees the same mix of fast and slow periods of the virtual CPUs.
  const int rounds = std::max(slow, fast);
  for (int rep = 0; rep < rounds; ++rep) {
    for (Entry& e : entries) {
      // Entries with fewer repetitions are spread evenly over the rounds, so that every entry is
      // measured across the whole run and not only in one stretch of it.
      const int stride = rounds / e.repetitions;
      if (rep % stride != 0 || rep / stride >= e.repetitions)
        continue;
      if (only != nullptr && std::strstr(e.label, only) == nullptr)
        continue;
      const Sample sample = e.measure(e.label, opt);
      e.p50.push_back(sample.p50);
      e.p99.push_back(sample.p99);
      e.p999.push_back(sample.p999);
      e.max.push_back(sample.max);
      e.ok = e.ok && sample.ok;
    }
  }
  for (const Entry& e : entries) {
    if (!e.p50.empty())
      print_entry(e);
  }
  return 0;
}
