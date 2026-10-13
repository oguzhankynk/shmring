include(FetchContent)

find_path(SHMRING_BOOST_INCLUDE_DIR boost/lockfree/spsc_queue.hpp)
set(SHMRING_BENCH_DEFS "")
if(SHMRING_BOOST_INCLUDE_DIR)
  list(APPEND SHMRING_BENCH_DEFS SHMRING_BENCH_HAVE_BOOST)
  message(STATUS "Benchmarks: boost::lockfree baseline enabled (${SHMRING_BOOST_INCLUDE_DIR})")
else()
  message(STATUS "Benchmarks: Boost.Lockfree not found, skipping that baseline")
endif()

foreach(tool pingpong throughput)
  add_executable(shmring_${tool} ${tool}.cpp)
  target_link_libraries(shmring_${tool} PRIVATE shmring::shmring)
  target_compile_definitions(shmring_${tool} PRIVATE ${SHMRING_BENCH_DEFS})
  if(SHMRING_BOOST_INCLUDE_DIR)
    target_include_directories(shmring_${tool} SYSTEM PRIVATE ${SHMRING_BOOST_INCLUDE_DIR})
  endif()
  shmring_set_warnings(shmring_${tool})
endforeach()

FetchContent_Declare(
  googlebenchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG v1.8.3
  GIT_SHALLOW TRUE
  SYSTEM)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(BENCHMARK_INSTALL_DOCS OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googlebenchmark)

add_executable(shmring_micro micro.cpp)
target_link_libraries(shmring_micro PRIVATE shmring::shmring benchmark::benchmark)
target_compile_definitions(shmring_micro PRIVATE ${SHMRING_BENCH_DEFS})
if(SHMRING_BOOST_INCLUDE_DIR)
  target_include_directories(shmring_micro SYSTEM PRIVATE ${SHMRING_BOOST_INCLUDE_DIR})
endif()
