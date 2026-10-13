include(GoogleTest)
find_package(Threads REQUIRED)

add_executable(shmring_unit_tests
  unit/test_spsc_ring.cpp
  unit/test_error_futex.cpp
  unit/test_segment.cpp
  unit/test_spmc_ring.cpp
  unit/test_mpmc_ring.cpp
  unit/test_event.cpp
  unit/test_spsc_msg_ring.cpp
  unit/test_version.cpp
)

target_include_directories(shmring_unit_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(shmring_unit_tests PRIVATE shmring::shmring GTest::gtest GTest::gtest_main Threads::Threads)

shmring_set_warnings(shmring_unit_tests)
shmring_enable_sanitizers(shmring_unit_tests)
shmring_enable_coverage(shmring_unit_tests)
shmring_apply_sanitizer_test_wrapper(shmring_unit_tests)

gtest_discover_tests(shmring_unit_tests)

shmring_add_coverage_target()
if(TARGET coverage)
  # The 'coverage' custom target has no implicit dependency on the test
  # binaries; without this, a clean build runs ctest before
  # shmring_unit_tests exists and every test reports NOT_BUILT.
  add_dependencies(coverage shmring_unit_tests)
endif()

# Long-running stress and crash tests. They run with the regular test presets (and therefore
# under the sanitizers) but carry their own label, so `ctest -L stress` selects only them.
add_executable(shmring_stress_tests
  stress/test_stress.cpp
  stress/test_crash.cpp
)
target_include_directories(shmring_stress_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(shmring_stress_tests PRIVATE shmring::shmring GTest::gtest GTest::gtest_main Threads::Threads)
shmring_set_warnings(shmring_stress_tests)
shmring_enable_sanitizers(shmring_stress_tests)
shmring_enable_coverage(shmring_stress_tests)
shmring_apply_sanitizer_test_wrapper(shmring_stress_tests)
gtest_discover_tests(shmring_stress_tests PROPERTIES LABELS stress TIMEOUT 900)
if(TARGET coverage)
  add_dependencies(coverage shmring_stress_tests)
endif()

# Replaces the global allocation functions with counting versions, so it is a separate executable.
add_executable(shmring_alloc_tests alloc/test_no_alloc.cpp)
target_include_directories(shmring_alloc_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(shmring_alloc_tests PRIVATE shmring::shmring GTest::gtest GTest::gtest_main Threads::Threads)
shmring_set_warnings(shmring_alloc_tests)
shmring_enable_sanitizers(shmring_alloc_tests)
shmring_enable_coverage(shmring_alloc_tests)
shmring_apply_sanitizer_test_wrapper(shmring_alloc_tests)
gtest_discover_tests(shmring_alloc_tests PROPERTIES LABELS alloc)
if(TARGET coverage)
  add_dependencies(coverage shmring_alloc_tests)
endif()
