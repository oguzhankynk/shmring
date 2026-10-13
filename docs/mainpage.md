shmring {#mainpage}
=======

Lock-free queues for passing messages between processes and threads on one
Linux host through POSIX shared memory. See the top-level `README.md` for how
to build the library and `docs/DESIGN.md` for the memory-ordering design.

@code
#include <shmring/version.hpp>
#include <iostream>

int main() {
  std::cout << "shmring " << shmring::version() << "\n";
}
@endcode
