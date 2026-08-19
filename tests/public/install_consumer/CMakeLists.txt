cmake_minimum_required(VERSION 3.14)
project(cedar_install_consumer LANGUAGES CXX)

find_package(Cedar CONFIG REQUIRED)

add_executable(cedar_install_consumer main.cc)
target_link_libraries(cedar_install_consumer PRIVATE Cedar::cedar)
