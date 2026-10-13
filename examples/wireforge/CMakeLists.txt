# Needs the WireForge package: -DWireForge_DIR=$(wireforge cmake-dir)
find_package(WireForge 0.1 REQUIRED)
wireforge_generate(ring_telemetry_model PROJECT "${CMAKE_CURRENT_SOURCE_DIR}/telemetry")

add_executable(telemetry_over_ring telemetry_over_ring.cpp)
target_link_libraries(telemetry_over_ring PRIVATE shmring::shmring ring_telemetry_model)
