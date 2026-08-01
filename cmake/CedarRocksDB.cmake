include_guard(GLOBAL)

set(CEDAR_ROCKSDB_SOURCE_DIR "${CMAKE_SOURCE_DIR}/third_party/rocksdb")
if(NOT EXISTS "${CEDAR_ROCKSDB_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "Cedar requires the pinned third_party/rocksdb submodule")
endif()

# Keep every RocksDB build choice local to the repository-owned source tree.
set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_CORE_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_EXAMPLES OFF CACHE BOOL "" FORCE)
set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)
set(WITH_JEMALLOC OFF CACHE BOOL "" FORCE)
set(WITH_JNI OFF CACHE BOOL "" FORCE)
set(WITH_LIBURING OFF CACHE BOOL "" FORCE)
set(WITH_LZ4 OFF CACHE BOOL "" FORCE)
set(WITH_SNAPPY OFF CACHE BOOL "" FORCE)
set(WITH_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_TRACE_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_ZLIB OFF CACHE BOOL "" FORCE)
set(WITH_ZSTD OFF CACHE BOOL "" FORCE)
set(WITH_BZ2 OFF CACHE BOOL "" FORCE)

add_subdirectory("${CEDAR_ROCKSDB_SOURCE_DIR}"
                 "${CMAKE_BINARY_DIR}/third_party/rocksdb"
                 EXCLUDE_FROM_ALL)

add_library(cedar_rocksdb INTERFACE)
target_link_libraries(cedar_rocksdb INTERFACE rocksdb)
target_compile_definitions(cedar_rocksdb INTERFACE
    CEDAR_ROCKSDB_VERSION="11.1.2")
