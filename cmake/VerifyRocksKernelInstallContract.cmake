if(NOT DEFINED CEDAR_SOURCE_DIR)
  message(FATAL_ERROR "CEDAR_SOURCE_DIR is required")
endif()

file(READ "${CEDAR_SOURCE_DIR}/CMakeLists.txt" cmake_lists)

if(NOT cmake_lists MATCHES "install\\(TARGETS cedar_core")
  message(FATAL_ERROR "cedar_core is not installed")
endif()

if(cmake_lists MATCHES "install\\(TARGETS cedar_projection")
  message(FATAL_ERROR "derived projection target must not be installed")
endif()

if(NOT cmake_lists MATCHES "install\\(DIRECTORY include/cedar")
  message(FATAL_ERROR "public Cedar headers are not installed")
endif()
