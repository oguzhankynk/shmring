configure_file(version.cpp.in "${CMAKE_CURRENT_BINARY_DIR}/version.cpp" @ONLY)

add_library(shmring
  "${CMAKE_CURRENT_BINARY_DIR}/version.cpp"
  segment.cpp
  futex.cpp
)
add_library(shmring::shmring ALIAS shmring)

target_compile_features(shmring PUBLIC cxx_std_17)

target_include_directories(shmring
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

find_library(SHMRING_RT_LIBRARY rt)
find_package(Threads REQUIRED)
target_link_libraries(shmring PUBLIC Threads::Threads $<$<BOOL:${SHMRING_RT_LIBRARY}>:rt>)

target_compile_definitions(shmring PUBLIC SHMRING_CACHE_LINE=${SHMRING_CACHE_LINE_SIZE})

shmring_set_warnings(shmring)
shmring_enable_sanitizers(shmring)
shmring_enable_coverage(shmring)
shmring_enable_clang_tidy(shmring)
shmring_enable_cppcheck(shmring)

# Instantiates every public template once so that the static analyzers see the header-only
# queues. The object code is never linked or run, so this target gets no coverage
# instrumentation; its unused copies would otherwise count as untested code.
add_library(shmring_analysis OBJECT instantiations.cpp)
target_link_libraries(shmring_analysis PRIVATE shmring::shmring)
shmring_set_warnings(shmring_analysis)
shmring_enable_clang_tidy(shmring_analysis)
shmring_enable_cppcheck(shmring_analysis)
