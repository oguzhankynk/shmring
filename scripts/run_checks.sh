#!/usr/bin/env bash
# Runs every quality gate this template defines, in the same order CI does,
# stopping at the first failure. Run from anywhere; paths are resolved
# relative to this script's location.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

step() {
  echo
  echo "==> $*"
}

step "clang-format --dry-run"
mapfile -t sources < <(git ls-files '*.hpp' '*.cpp')
clang-format --dry-run --Werror "${sources[@]}"

step "clang-tidy (preset: tidy)"
cmake --preset tidy
cmake --build --preset tidy

step "cppcheck (preset: cppcheck)"
cmake --preset cppcheck
cmake --build --preset cppcheck

step "Doxygen documentation coverage (preset: debug)"
cmake --preset debug
cmake --build --preset debug --target docs-check

step "Unit tests (preset: debug)"
cmake --build --preset debug
ctest --preset debug

step "AddressSanitizer + UndefinedBehaviorSanitizer (preset: asan)"
cmake --preset asan
cmake --build --preset asan
ctest --preset asan

step "ThreadSanitizer (preset: tsan)"
cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan

step "Coverage (preset: coverage)"
cmake --preset coverage
cmake --build --preset coverage --target coverage

step "Install + find_package(shmring)"
cmake --preset release
cmake --build --preset release

INSTALL_PREFIX="$(mktemp -d)"
CONSUMER_DIR="$(mktemp -d)"
trap 'rm -rf "${INSTALL_PREFIX}" "${CONSUMER_DIR}"' EXIT

cmake --install build/release --prefix "${INSTALL_PREFIX}"

cat > "${CONSUMER_DIR}/CMakeLists.txt" <<'EOF'
cmake_minimum_required(VERSION 3.25)
project(shmring_install_check LANGUAGES CXX)
find_package(shmring CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE shmring::shmring)
EOF
cat > "${CONSUMER_DIR}/main.cpp" <<'EOF'
#include <shmring/version.hpp>
int main() { return shmring::version() != nullptr ? 0 : 1; }
EOF

cmake -S "${CONSUMER_DIR}" -B "${CONSUMER_DIR}/build" -G Ninja \
  -DCMAKE_PREFIX_PATH="${INSTALL_PREFIX}"
cmake --build "${CONSUMER_DIR}/build"
"${CONSUMER_DIR}/build/consumer"

echo
echo "All checks passed."
