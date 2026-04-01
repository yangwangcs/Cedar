cmake_minimum_required(VERSION 3.14)
project(cedar VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# macOS: Fix C++ standard library headers path when CommandLineTools usr/include is incomplete
if(APPLE)
    execute_process(COMMAND xcrun --sdk macosx --show-sdk-path
                    OUTPUT_VARIABLE OSX_SDK_PATH
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(OSX_CXX_V1 "${OSX_SDK_PATH}/usr/include/c++/v1")
    if(EXISTS "${OSX_CXX_V1}/cstdint" AND NOT EXISTS "/usr/include/c++/v1/cstdint")
        add_compile_options(-isystem "${OSX_CXX_V1}")
    endif()
endif()

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/src)

# Find required packages
find_package(PkgConfig REQUIRED)
pkg_check_modules(LZ4 REQUIRED liblz4)
find_package(gRPC CONFIG QUIET)
if(NOT gRPC_FOUND)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GRPC REQUIRED grpc++ grpc protobuf)
    find_package(Protobuf REQUIRED)
endif()

# Core library sources
set(CEDAR_CORE_SOURCES
    src/core/status.cc
    src/core/env_posix.cc
    src/core/threading.cc
    src/core/crc32c.cc
    src/types/descriptor.cc
)

# Storage sources
set(CEDAR_STORAGE_SOURCES
    src/storage/lsm_engine.cc
    src/storage/sst_reader_cache.cc
    src/storage/block_cache.cc
    src/storage/compaction_merger.cc
    src/storage/vsl_memtable.cc
    src/storage/versioned_skiplist_lockfree.cc
    src/storage/cedar_memtable.cc
    src/storage/query_cache.cc
    src/storage/async_index_builder.cc
    src/storage/cedar_graph_storage.cc
    src/storage/entity_lifecycle.cc
    src/storage/blob_gc_manager.cc
    src/storage/size_tiered_compaction.cc
    src/storage/parallel_compaction_engine.cc
    src/storage/block_level_compaction.cc
    src/storage/parallel_query_engine.cc
    src/storage/adaptive_thread_pool.cc
    src/storage/auto_blob_storage.cc
    src/storage/temporal_storage_layer.cc
)

# Transaction sources
set(CEDAR_TRANSACTION_SOURCES
    src/transaction/occ_transaction.cc
    src/transaction/wal.cc
    src/transaction/batch_api.cc
    src/transaction/transaction_pool.cc
    src/transaction/batch_transaction.cc
    src/transaction/sharded_timestamp_allocator.cc
    src/transaction/wal_batch_writer.cc
    src/transaction/parallel_batch_processor.cc
)

# DB sources
set(CEDAR_DB_SOURCES
    src/db/graph_db.cc
    src/db/graph_db_impl.cc
    src/db/manifest.cc
    src/db/database.cc
)

# Frond format sources
set(CEDAR_FROND_SOURCES
    src/frond/bloom_filter.cc
    src/frond/compression.cc
    src/frond/column_coders.cc
    src/frond/blob_file_manager.cc
    src/frond/zone_encoder.cc
    src/frond/zone_columnar_format.cc
    src/frond/blob_file.cc
)

# IO sources
set(CEDAR_IO_SOURCES
    src/io/async_io.cc
)

# Create cedar library
add_library(cedar STATIC 
    ${CEDAR_CORE_SOURCES}
    ${CEDAR_STORAGE_SOURCES}
    ${CEDAR_TRANSACTION_SOURCES}
    ${CEDAR_FROND_SOURCES}
    ${CEDAR_IO_SOURCES}
    ${CEDAR_DB_SOURCES}
)

target_link_libraries(cedar ${LZ4_LIBRARIES})
target_include_directories(cedar PUBLIC ${LZ4_INCLUDE_DIRS})
target_link_directories(cedar PUBLIC ${LZ4_LIBRARY_DIRS})

# Graph sources
set(CEDAR_GRAPH_SOURCES
    src/graph/cedar_graph.cc
    src/graph/cedar_graph_temporal.cc
    src/graph/cedar_graph_db.cc
    src/graph/graph_semantic_layer.cc
    src/graph/pushdown_predicate.cc
)

add_library(cedar_graph STATIC ${CEDAR_GRAPH_SOURCES})
target_link_libraries(cedar_graph cedar)

# Cypher engine sources
set(CEDAR_CYPHER_SOURCES
    src/cypher/cypher_engine.cc
    src/cypher/parser.cc
    src/cypher/parser_temporal.cc
    src/cypher/execution_plan.cc
    src/cypher/temporal_dialect.cc
    src/cypher/operators/temporal_operators.cc
)

add_library(cedar_cypher STATIC ${CEDAR_CYPHER_SOURCES})
target_link_libraries(cedar_cypher cedar cedar_graph)

# Compression sources
set(CEDAR_COMPRESSION_SOURCES
    src/compression/delta_encoder.cc
)

add_library(cedar_compression STATIC ${CEDAR_COMPRESSION_SOURCES})

# Generate protobuf files
set(PROTO_FILES ${CMAKE_SOURCE_DIR}/proto/cedar_graph.proto)
set(PROTO_OUT_DIR ${CMAKE_SOURCE_DIR})

add_custom_command(
    OUTPUT ${PROTO_OUT_DIR}/cedar_graph.pb.cc ${PROTO_OUT_DIR}/cedar_graph.pb.h
           ${PROTO_OUT_DIR}/cedar_graph.grpc.pb.cc ${PROTO_OUT_DIR}/cedar_graph.grpc.pb.h
    COMMAND protobuf::protoc
    ARGS --grpc_out=${PROTO_OUT_DIR}
         --cpp_out=${PROTO_OUT_DIR}
         --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
         -I ${CMAKE_SOURCE_DIR}/proto
         ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Generating protobuf and gRPC files"
)

add_custom_target(generate_proto ALL
    DEPENDS ${PROTO_OUT_DIR}/cedar_graph.pb.cc ${PROTO_OUT_DIR}/cedar_graph.pb.h
            ${PROTO_OUT_DIR}/cedar_graph.grpc.pb.cc ${PROTO_OUT_DIR}/cedar_graph.grpc.pb.h
)

# Optional: Build examples if enabled
option(BUILD_EXAMPLES "Build example programs" OFF)
if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# Optional: Build tests if enabled
option(BUILD_TESTS "Build test programs" OFF)
if(BUILD_TESTS)
    enable_testing()
    find_package(GTest)
    if(NOT GTest_FOUND)
        include(FetchContent)
        FetchContent_Declare(
            googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG        v1.15.2
        )
        FetchContent_MakeAvailable(googletest)
    endif()
    add_subdirectory(tests)
endif()
