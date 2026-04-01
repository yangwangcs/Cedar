# Simple example
add_executable(simple_example simple_example.cc)
target_link_libraries(simple_example cedar cedar_graph cedar_compression pthread lz4)
