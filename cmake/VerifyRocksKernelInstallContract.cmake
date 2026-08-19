if(NOT DEFINED CEDAR_SOURCE_DIR)
  message(FATAL_ERROR "CEDAR_SOURCE_DIR is required")
endif()

file(READ "${CEDAR_SOURCE_DIR}/CMakeLists.txt" cmake_lists)

if(NOT cmake_lists MATCHES "install\\(TARGETS cedar_core EXPORT CedarTargets")
  message(FATAL_ERROR "Cedar product target is not exported")
endif()

if(cmake_lists MATCHES "install\\(TARGETS cedar_projection")
  message(FATAL_ERROR "derived projection target must not be installed")
endif()

if(NOT cmake_lists MATCHES "install\\(DIRECTORY include/cedar")
  message(FATAL_ERROR "public Cedar headers are not installed")
endif()

if(NOT cmake_lists MATCHES "NAMESPACE Cedar::")
  message(FATAL_ERROR "Cedar::cedar package namespace is not exported")
endif()

if(cmake_lists MATCHES "install\\(DIRECTORY[^
]*rocksdb")
  message(FATAL_ERROR "RocksDB headers must not be installed by Cedar")
endif()
