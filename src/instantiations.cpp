// Instantiates every public template once so that the static analyzers (clang-tidy, cppcheck) see
// the code of the header-only queues, and so that each header is checked to compile on its own.
#include "shmring/blocking_spsc_ring.hpp"
#include "shmring/error.hpp"
#include "shmring/event.hpp"
#include "shmring/futex.hpp"
#include "shmring/mpmc_ring.hpp"
#include "shmring/segment.hpp"
#include "shmring/shared_queue.hpp"
#include "shmring/shared_spsc_msg_ring.hpp"
#include "shmring/span.hpp"
#include "shmring/spmc_ring.hpp"
#include "shmring/spsc_msg_ring.hpp"
#include "shmring/spsc_ring.hpp"

#include <cstdint>

namespace shmring {

constexpr std::size_t slots = 16;

template class spsc_ring<std::uint64_t, slots>;
template class mpmc_ring<std::uint64_t, slots>;
template class spmc_ring<std::uint64_t, slots>;
template class blocking_spsc_ring<std::uint64_t, slots>;
template class span<const std::byte>;
template class result<segment>;
template class shared_queue<spsc_ring<std::uint64_t, slots>>;
template class shared_queue<blocking_spsc_ring<std::uint64_t, slots>>;
template class shared_queue<mpmc_ring<std::uint64_t, slots>>;
template class shared_queue<spmc_ring<std::uint64_t, slots>>;

}  // namespace shmring
