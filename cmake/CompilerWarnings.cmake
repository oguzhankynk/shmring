include_guard(GLOBAL)

## shmring_set_warnings(<target>)
#
# Applies the project's warning policy to <target>. SHMRING_WARNINGS_AS_ERRORS
# turns every warning into a build failure.
function(shmring_set_warnings target)
  set(gcc_clang_warnings
    -Wall
    -Wextra
    -Wpedantic
    -Wshadow
    -Wconversion
    -Wsign-conversion
    -Wold-style-cast
    -Wnon-virtual-dtor
    -Wnull-dereference
    -Wdouble-promotion
    -Wformat=2
    -Wcast-align
    -Woverloaded-virtual
    -Wunused
  )

  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    list(APPEND gcc_clang_warnings
      -Wdocumentation
      -Wdocumentation-pedantic
    )
  endif()

  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    list(APPEND gcc_clang_warnings
      -Wduplicated-cond
      -Wduplicated-branches
      -Wlogical-op
      -Wuseless-cast
    )
  endif()

  target_compile_options(${target} PRIVATE ${gcc_clang_warnings})

  if(SHMRING_WARNINGS_AS_ERRORS)
    target_compile_options(${target} PRIVATE -Werror)
  endif()
endfunction()
