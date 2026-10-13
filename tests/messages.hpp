// A test message whose fields depend on each other, so that a torn or mixed-up copy is detected.
#ifndef SHMRING_TESTS_MESSAGES_HPP
#define SHMRING_TESTS_MESSAGES_HPP

#include <cstdint>

#include "support.hpp"

namespace shmring::testing_support {

struct msg {
  std::uint64_t seq;
  std::uint64_t checksum;
  std::uint64_t inverse;
  std::uint64_t triple;
};

inline msg make_msg(std::uint64_t seq) {
  return {seq, checksum_of(seq), ~seq, seq * 3};
}

inline bool valid(const msg& m) {
  return m.checksum == checksum_of(m.seq) && m.inverse == ~m.seq && m.triple == m.seq * 3;
}

// Variable-length payload for the message ring: the length and every byte follow from `seq`.
inline std::size_t length_for(std::uint64_t seq, std::size_t max_length) {
  return 8 + static_cast<std::size_t>((seq * 2654435761ULL) % (max_length - 7));
}

inline unsigned char byte_for(std::uint64_t seq, std::size_t index) {
  return static_cast<unsigned char>((seq * 31 + index * 7) & 0xFF);
}

}  // namespace shmring::testing_support

#endif  // SHMRING_TESTS_MESSAGES_HPP
