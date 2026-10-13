include_guard(GLOBAL)
include(FetchContent)

## shmring_fetch_test_dependencies()
#
# Fetches GoogleTest/GoogleMock, used by tests/ only.
# Pinned to an exact tag for reproducible builds.
function(shmring_fetch_test_dependencies)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
    GIT_SHALLOW TRUE
    SYSTEM)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  set(BUILD_GMOCK ON CACHE BOOL "" FORCE)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endfunction()
