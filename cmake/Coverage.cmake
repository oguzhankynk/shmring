include_guard(GLOBAL)

## shmring_enable_coverage(<target>)
#
# Instruments <target> with --coverage when SHMRING_ENABLE_COVERAGE is ON.
# Call shmring_add_coverage_target() once, after tests exist, to register the
# aggregate 'coverage' target (gcovr HTML + XML + threshold).
function(shmring_enable_coverage target)
  if(NOT SHMRING_ENABLE_COVERAGE)
    return()
  endif()

  if(NOT (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
    message(WARNING "SHMRING_ENABLE_COVERAGE is set but ${CMAKE_CXX_COMPILER_ID} is not GCC/Clang; ignoring.")
    return()
  endif()

  target_compile_options(${target} PUBLIC --coverage -O0 -g)
  target_link_options(${target} PUBLIC --coverage)
endfunction()

## shmring_add_coverage_target()
#
# Registers a 'coverage' target that runs ctest and then gcovr, failing the
# build if the line/branch thresholds below are not met.
# No-op if SHMRING_ENABLE_COVERAGE is OFF or gcovr is not found.
function(shmring_add_coverage_target)
  if(NOT SHMRING_ENABLE_COVERAGE)
    return()
  endif()

  find_program(GCOVR_EXECUTABLE gcovr)
  if(NOT GCOVR_EXECUTABLE)
    message(WARNING "SHMRING_ENABLE_COVERAGE is ON but gcovr was not found; 'coverage' target skipped.")
    return()
  endif()

  set(coverage_html_dir "${CMAKE_BINARY_DIR}/coverage")
  file(MAKE_DIRECTORY "${coverage_html_dir}")

  add_custom_target(coverage
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure --test-dir "${CMAKE_BINARY_DIR}"
    # gcovr re-resolves each .gcno's recorded source path relative to its own
    # current working directory (one of several fallback heuristics it
    # tries); run it from CMAKE_SOURCE_DIR, matching --root, so resolution
    # lands on the real src/ and include/shmring/ files instead of silently
    # falling back to whatever it can find via another heuristic.
    COMMAND ${GCOVR_EXECUTABLE}
            --root "${CMAKE_SOURCE_DIR}"
            --object-directory "${CMAKE_BINARY_DIR}"
            # Works around a long-standing gcov bug (GCC PR68080) where
            # branch counters on some code paths come out negative; gcovr's
            # own suggested workaround.
            --gcov-ignore-parse-errors negative_hits.warn
            # The stress tests spin on try_push/try_pop, so small functions are executed billions of
            # times. Those are real counts; gcovr's default threshold for "suspicious" hit counts
            # would otherwise reject the report.
            --gcov-suspicious-hits-threshold 1000000000000
            # The queues are templates. Count a source line as covered when any instantiation ran
            # it, instead of counting every instantiation's copy of the line separately; the latter
            # would demand that every error path run for every element type.
            --merge-lines
            # Substrings, not anchored paths: gcovr matches these relative to
            # --root, and generated sources (e.g. version.cpp, written by
            # configure_file into the build tree) must still match.
            --filter "src/"
            --filter "include/shmring/"
            --exclude "_deps/"
            --exclude "tests/"
            --html --html-details -o "${coverage_html_dir}/index.html"
            --xml "${CMAKE_BINARY_DIR}/coverage.xml"
            --print-summary
            # Defaults for this template's own trivial example code; lower
            # these to match what your own project's test suite actually
            # achieves (implicit branches from things like std::string's
            # allocation paths are often impractical to hit deliberately).
            --fail-under-line 90
            --fail-under-branch 60
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    COMMENT "Running tests and generating coverage report in ${coverage_html_dir}"
    VERBATIM)
endfunction()
