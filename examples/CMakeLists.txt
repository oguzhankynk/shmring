foreach(example quickstart_producer quickstart_consumer message_ring_demo)
  add_executable(${example} ${example}.cpp)
  target_link_libraries(${example} PRIVATE shmring::shmring)
  shmring_set_warnings(${example})
endforeach()

if(SHMRING_BUILD_WIREFORGE_EXAMPLE)
  add_subdirectory(wireforge)
endif()
