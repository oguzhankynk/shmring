cmake_minimum_required(VERSION 3.25)

project(shmring
  VERSION 0.1.0
  DESCRIPTION "Lock-free inter-process queues over POSIX shared memory"
  LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
find_package(Doxygen QUIET)

option(BUILD_SHARED_LIBS "Build shmring as a shared library" OFF)
option(SHMRING_BUILD_TESTS "Build unit tests" ${PROJECT_IS_TOP_LEVEL})
option(SHMRING_BUILD_EXAMPLES "Build example programs" ${PROJECT_IS_TOP_LEVEL})
option(SHMRING_BUILD_DOCS "Build the Doxygen API reference ('docs' / 'docs-check' targets)" ${DOXYGEN_FOUND})
option(SHMRING_ENABLE_CLANG_TIDY "Run clang-tidy on the shmring target while building" OFF)
option(SHMRING_ENABLE_CPPCHECK "Run cppcheck on the shmring target while building" OFF)
option(SHMRING_ENABLE_COVERAGE "Build shmring and its tests with --coverage instrumentation" OFF)
option(SHMRING_BUILD_BENCHMARKS "Build the benchmark tools (downloads Google Benchmark)" OFF)
option(SHMRING_BUILD_WIREFORGE_EXAMPLE "Build the example that sends WireForge frames (needs WireForge_DIR)" OFF)
option(SHMRING_BUILD_FUZZERS "Build libFuzzer targets (requires Clang)" OFF)
option(SHMRING_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)
set(SHMRING_CACHE_LINE_SIZE 64 CACHE STRING
  "Cache line size in bytes used for queue layout (64 for x86-64, 128 for some AArch64 cores)")
set(SHMRING_SANITIZER "" CACHE STRING
  "Semicolon-separated sanitizers to enable, e.g. 'address;undefined' or 'thread'")

include(CompilerWarnings)
include(Sanitizers)
include(Coverage)
include(ClangTidy)
include(Cppcheck)
include(Doxygen)
include(Dependencies)

add_subdirectory(src)

if(SHMRING_BUILD_TESTS)
  enable_testing()
  shmring_fetch_test_dependencies()
  add_subdirectory(tests)
endif()

if(SHMRING_BUILD_FUZZERS)
  add_subdirectory(fuzz)
endif()

if(SHMRING_BUILD_BENCHMARKS)
  add_subdirectory(bench)
endif()

if(SHMRING_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(SHMRING_BUILD_DOCS)
  shmring_add_doxygen_targets()
endif()

# ---------------------------------------------------------------------------
# Install / export
# ---------------------------------------------------------------------------
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS shmring
  EXPORT shmringTargets
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(DIRECTORY include/shmring DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT shmringTargets
  FILE shmringTargets.cmake
  NAMESPACE shmring::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/shmring)

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/shmringConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/shmringConfig.cmake"
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/shmring)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/shmringConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/shmringConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/shmringConfigVersion.cmake"
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/shmring)
