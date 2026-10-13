// Throughput of one-way message streams between two pinned processes (or threads, for the
// in-process baseline). Every transport delivers each message into a buffer owned by the
// receiver, so the copy cost is included for all of them.
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
using std::size_t;
using std::uint64_t;

template <size_t S>
struct Payload {
  unsigned char bytes[S];
};

template <size_t S>
constexpr size_t slots() {
  return (size_t{1} << 20) / S;  // about 1 MiB of ring for every payload size
}

template <size_t S>
uint64_t sequence_of(const Payload<S>& p) {
  uint64_t v;
  std::memcpy(&v, p.bytes, sizeof(v));
  return v;
}

template <size_t S>
void set_sequence(Payload<S>& p, uint64_t v) {
  std::memcpy(p.bytes, &v, sizeof(v));
}

// ---- shmring transports -------------------------------------------------------------------

template <size_t S>
struct SpscT {
  static constexpr bool threads = false;
  using ring = shared_spsc_ring<Payload<S>, slots<S>()>;
  SpscT() : name(bench::unique_name("tp_spsc")), q(std::move(ring::create(name).value())) {}
  ~SpscT() {
    (void)ring::unlink(name);
  }
  void send(const Payload<S>& p) {
    while (!q.queue().try_push(p)) bench::cpu_relax();
  }
  void flush() {}
  void recv(Payload<S>& p) {
    while (!q.queue().try_pop(p)) bench::cpu_relax();
  }
  std::string name;
  ring q;
};

template <size_t S>
struct SpscBatchT {
  static constexpr bool threads = false;
  static constexpr size_t batch = 32;
  using ring = shared_spsc_ring<Payload<S>, slots<S>()>;
  SpscBatchT() : name(bench::unique_name("tp_spscb")), q(std::move(ring::create(name).value())) {}
  ~SpscBatchT() {
    (void)ring::unlink(name);
  }
  void send(const Payload<S>& p) {
    out[out_n++] = p;
    if (out_n == batch)
      flush();
  }
  void flush() {
    size_t done = 0;
    while (done < out_n) {
      const size_t n = q.queue().try_push_n(out + done, out_n - done);
      done += n;
      if (n == 0)
        bench::cpu_relax();
    }
    out_n = 0;
  }
  void recv(Payload<S>& p) {
    while (in_pos == in_n) {
      in_n = q.queue().try_pop_n(in, batch);
      in_pos = 0;
      if (in_n == 0)
        bench::cpu_relax();
    }
    p = in[in_pos++];
  }
  std::string name;
  ring q;
  Payload<S> out[batch];
  Payload<S> in[batch];
  size_t out_n = 0, in_n = 0, in_pos = 0;
};

template <size_t S>
struct MsgT {
  static constexpr bool threads = false;
  MsgT()
      : name(bench::unique_name("tp_msg")),
        q(std::move(shared_spsc_msg_ring::create(name, size_t{1} << 20).value())) {}
  ~MsgT() {
    (void)shared_spsc_msg_ring::unlink(name);
  }
  void send(const Payload<S>& p) {
    for (;;) {
      auto w = q.ring().reserve(S);
      if (w) {
        std::memcpy(w.data(), p.bytes, S);
        q.ring().commit();
        return;
      }
      bench::cpu_relax();
    }
  }
  void flush() {}
  void recv(Payload<S>& p) {
    for (;;) {
      auto m = q.ring().peek();
      if (m) {
        std::memcpy(p.bytes, m.data(), S);
        q.ring().release();
        return;
      }
      bench::cpu_relax();
    }
  }
  std::string name;
  shared_spsc_msg_ring q;
};

template <size_t S>
struct MpmcT {
  static constexpr bool threads = false;
  using ring = shared_mpmc_ring<Payload<S>, slots<S>()>;
  MpmcT() : name(bench::unique_name("tp_mpmc")), q(std::move(ring::create(name).value())) {}
  ~MpmcT() {
    (void)ring::unlink(name);
  }
  void send(const Payload<S>& p) {
    while (!q.queue().try_push(p)) bench::cpu_relax();
  }
  void flush() {}
  void recv(Payload<S>& p) {
    while (!q.queue().try_pop(p)) bench::cpu_relax();
  }
  std::string name;
  ring q;
};

template <size_t S>
struct FutexT {
  static constexpr bool threads = false;
  using ring = shared_blocking_spsc_ring<Payload<S>, slots<S>()>;
  FutexT() : name(bench::unique_name("tp_futex")), q(std::move(ring::create(name).value())) {}
  ~FutexT() {
    (void)ring::unlink(name);
  }
  void send(const Payload<S>& p) {
    q.queue().push(p);
  }
  void flush() {}
  void recv(Payload<S>& p) {
    q.queue().pop(p);
  }
  std::string name;
  ring q;
};

// ---- kernel transports --------------------------------------------------------------------

void write_full(int fd, const void* buf, size_t n) {
  const auto* p = static_cast<const unsigned char*>(buf);
  while (n > 0) {
    const ssize_t w = ::write(fd, p, n);
    if (w <= 0)
      std::abort();
    p += w;
    n -= static_cast<size_t>(w);
  }
}

void read_full(int fd, void* buf, size_t n) {
  auto* p = static_cast<unsigned char*>(buf);
  while (n > 0) {
    const ssize_t r = ::read(fd, p, n);
    if (r <= 0)
      std::abort();
    p += r;
    n -= static_cast<size_t>(r);
  }
}

template <size_t S>
struct PipeT {
  static constexpr bool threads = false;
  PipeT() {
    if (::pipe(fds) != 0)
      std::abort();
  }
  void send(const Payload<S>& p) {
    write_full(fds[1], p.bytes, S);
  }
  void flush() {}
  void recv(Payload<S>& p) {
    read_full(fds[0], p.bytes, S);
  }
  int fds[2];
};

template <size_t S, int Type>
struct SocketT {
  static constexpr bool threads = false;
  SocketT() {
    if (::socketpair(AF_UNIX, Type, 0, fds) != 0)
      std::abort();
  }
  void send(const Payload<S>& p) {
    write_full(fds[0], p.bytes, S);
  }
  void flush() {}
  void recv(Payload<S>& p) {
    if constexpr (Type == SOCK_SEQPACKET) {
      if (::recv(fds[1], p.bytes, S, 0) != static_cast<ssize_t>(S))
        std::abort();
    } else {
      read_full(fds[1], p.bytes, S);
    }
  }
  int fds[2];
};

template <size_t S>
struct MqT {
  static constexpr bool threads = false;
  MqT() {
    mq_attr attr{};
    attr.mq_maxmsg = 10;
    attr.mq_msgsize = static_cast<long>(S);
    name = bench::unique_name("tp_mq");
    q = ::mq_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600, &attr);
    if (q == -1) {
      std::perror("mq_open");
      std::abort();
    }
  }
  ~MqT() {
    ::mq_unlink(name.c_str());
  }
  void send(const Payload<S>& p) {
    if (::mq_send(q, reinterpret_cast<const char*>(p.bytes), S, 0) != 0)
      std::abort();
  }
  void flush() {}
  void recv(Payload<S>& p) {
    if (::mq_receive(q, reinterpret_cast<char*>(p.bytes), S, nullptr) != static_cast<ssize_t>(S))
      std::abort();
  }
  std::string name;
  mqd_t q;
};

#ifdef SHMRING_BENCH_HAVE_BOOST
template <size_t S>
struct BoostT {
  static constexpr bool threads = true;
  using queue = boost::lockfree::spsc_queue<Payload<S>, boost::lockfree::capacity<slots<S>()>>;
  void send(const Payload<S>& p) {
    while (!q.push(p)) bench::cpu_relax();
  }
  void flush() {}
  void recv(Payload<S>& p) {
    while (!q.pop(p)) bench::cpu_relax();
  }
  queue q;
};
#endif

// ---- driver -------------------------------------------------------------------------------

struct Options {
  int cpu_consumer;
  int cpu_producer;
  int repetitions;
  int fast_repetitions;
};

struct Result {
  double msgs_per_s;
  bool ok;
};

template <size_t S, template <size_t> class Transport>
Result measure_once(const Options& opt) {
  uint64_t messages = std::min<uint64_t>(4000000, (uint64_t{1} << 30) / S);
  if (const int forced = bench::env_int("BENCH_MESSAGES", 0); forced > 0)
    messages = static_cast<uint64_t>(forced);
  auto t = std::make_unique<Transport<S>>();

  auto produce = [&] {
    bench::pin_to_cpu(opt.cpu_producer);
    auto p = std::make_unique<Payload<S>>();
    std::memset(p->bytes, 0xA5, S);
    for (uint64_t i = 0; i < messages; ++i) {
      set_sequence(*p, i);
      t->send(*p);
    }
    t->flush();
  };

  pid_t child = 0;
  std::thread producer;
  if constexpr (Transport<S>::threads) {
    producer = std::thread(produce);
  } else {
    child = bench::spawn(produce);
  }

  bench::pin_to_cpu(opt.cpu_consumer);
  auto p = std::make_unique<Payload<S>>();
  bool ok = true;
  uint64_t start = 0;
  for (uint64_t i = 0; i < messages; ++i) {
    t->recv(*p);
    if (i == 0)
      start = bench::now_ns();
    if (sequence_of(*p) != i)
      ok = false;
  }
  const uint64_t end = bench::now_ns();
  if constexpr (Transport<S>::threads) {
    producer.join();
  } else if (bench::wait_for(child) != 0) {
    ok = false;
  }
  const double seconds = static_cast<double>(end - start) / 1e9;
  return {static_cast<double>(messages - 1) / seconds, ok};
}

struct Entry {
  const char* label;
  const char* note;
  size_t payload;
  Result (*measure)(const Options&);
  int repetitions;
  std::vector<double> rates;
  bool ok = true;
};

template <size_t S, template <size_t> class Transport>
Entry entry(const char* label, const char* note, int repetitions) {
  return Entry{label, note, S, &measure_once<S, Transport>, repetitions, {}, true};
}

template <template <size_t> class Transport>
void add_sizes(
    std::vector<Entry>& out, const char* label, const char* note, bool extended, int repetitions) {
  out.push_back(entry<8, Transport>(label, note, repetitions));
  if (extended) {
    out.push_back(entry<16, Transport>(label, note, repetitions));
    out.push_back(entry<32, Transport>(label, note, repetitions));
  }
  out.push_back(entry<64, Transport>(label, note, repetitions));
  if (extended) {
    out.push_back(entry<128, Transport>(label, note, repetitions));
  }
  out.push_back(entry<256, Transport>(label, note, repetitions));
  if (extended) {
    out.push_back(entry<512, Transport>(label, note, repetitions));
  }
  out.push_back(entry<1024, Transport>(label, note, repetitions));
  out.push_back(entry<4096, Transport>(label, note, repetitions));
}

void print_entry(const Entry& e) {
  std::vector<double> rates = e.rates;
  std::sort(rates.begin(), rates.end());
  const double median = rates[rates.size() / 2];
  std::printf("| %-34s | %7zu | %10.3f | %8.3f | %7.3f | %7.3f | %4d | %s%s |\n",
              e.label,
              e.payload,
              median / 1e6,
              median * static_cast<double>(e.payload) / 1e9,
              rates.front() / 1e6,
              rates.back() / 1e6,
              e.repetitions,
              e.note,
              e.ok ? "" : " (ERRORS)");
}

template <size_t S>
using SeqPacketT = SocketT<S, SOCK_SEQPACKET>;
template <size_t S>
using StreamT = SocketT<S, SOCK_STREAM>;

}  // namespace

int main() {
  ::alarm(3600);
  const Options opt{bench::env_int("BENCH_CPU_A", 0),
                    bench::env_int("BENCH_CPU_B", 2),
                    bench::env_int("BENCH_REPETITIONS", 3),
                    bench::env_int("BENCH_FAST_REPETITIONS", 9)};
  std::printf(
      "Throughput of a one-way stream (producer cpu %d -> consumer cpu %d), payload bytes only.\n"
      "Repetitions are taken round-robin across the rows. 'Mmsg/s' and 'GB/s' are the median over "
      "the\n"
      "repetitions; 'min' and 'max' are the slowest and fastest repetition in Mmsg/s. "
      "Shared-memory\n"
      "queues get more repetitions than kernel transports, which take seconds per repetition.\n\n",
      opt.cpu_producer,
      opt.cpu_consumer);
  std::printf("| %-34s | %7s | %10s | %8s | %7s | %7s | %4s | %s |\n",
              "transport",
              "payload",
              "Mmsg/s",
              "GB/s",
              "min",
              "max",
              "reps",
              "note");
  std::printf("|%s|%s|%s|%s|%s|%s|%s|%s|\n",
              std::string(36, '-').c_str(),
              std::string(9, '-').c_str(),
              std::string(12, '-').c_str(),
              std::string(10, '-').c_str(),
              std::string(9, '-').c_str(),
              std::string(9, '-').c_str(),
              std::string(6, '-').c_str(),
              std::string(6, '-').c_str());
  const bool extended = bench::env_int("BENCH_EXTENDED", 0) != 0;  // adds 16, 32, 128 and 512 bytes
  const int slow = opt.repetitions;       // kernel transports: seconds per repetition
  const int fast = opt.fast_repetitions;  // shared-memory queues
  std::vector<Entry> entries;
  add_sizes<SpscT>(entries, "shmring spsc_ring", "processes", extended, fast);
  add_sizes<SpscBatchT>(entries, "shmring spsc_ring (batch of 32)", "processes", extended, fast);
  if (!extended) {
    add_sizes<MsgT>(entries, "shmring spsc_msg_ring", "processes", false, fast);
    add_sizes<MpmcT>(entries, "shmring mpmc_ring (1 producer)", "processes", false, fast);
    add_sizes<FutexT>(entries, "shmring blocking ring (futex)", "processes", false, fast);
    add_sizes<PipeT>(entries, "pipe", "processes", false, slow);
    add_sizes<SeqPacketT>(entries, "unix socket (SOCK_SEQPACKET)", "processes", false, slow);
    add_sizes<StreamT>(entries, "unix socket (SOCK_STREAM)", "processes", false, slow);
    add_sizes<MqT>(entries, "POSIX message queue", "processes", false, slow);
  }
#ifdef SHMRING_BENCH_HAVE_BOOST
  add_sizes<BoostT>(
      entries, "boost::lockfree::spsc_queue", "threads, in-process only", extended, fast);
#endif
  const char* only = std::getenv("BENCH_FILTER");
  // Repetition-major order: every round measures all rows back to back, so each row sees the same
  // mix of fast and slow periods of the virtual CPUs.
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
      const Result r = e.measure(opt);
      e.rates.push_back(r.msgs_per_s);
      e.ok = e.ok && r.ok;
    }
  }
  for (const Entry& e : entries) {
    if (!e.rates.empty())
      print_entry(e);
  }
  return 0;
}
