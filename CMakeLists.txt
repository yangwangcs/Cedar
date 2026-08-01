cmake_minimum_required(VERSION 3.14)
project(cedar VERSION 2.0.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(CEDAR_ENABLE_ASAN "Build Cedar with AddressSanitizer" OFF)
option(CEDAR_ENABLE_UBSAN "Build Cedar with UndefinedBehaviorSanitizer" OFF)
option(CEDAR_ENABLE_TSAN "Build Cedar with ThreadSanitizer" OFF)
option(CEDAR_MINIMAL_INSTRUMENTATION
       "Build the test-only Tier-0 instrumentation comparison profile" OFF)

set(CEDAR_SANITIZER_COUNT 0)
foreach(CEDAR_SANITIZER_OPTION
        CEDAR_ENABLE_ASAN CEDAR_ENABLE_UBSAN CEDAR_ENABLE_TSAN)
    if(${CEDAR_SANITIZER_OPTION})
        math(EXPR CEDAR_SANITIZER_COUNT "${CEDAR_SANITIZER_COUNT} + 1")
    endif()
endforeach()
if(CEDAR_SANITIZER_COUNT GREATER 1)
    message(FATAL_ERROR "Enable at most one of CEDAR_ENABLE_ASAN, CEDAR_ENABLE_UBSAN, or CEDAR_ENABLE_TSAN")
endif()

if(CEDAR_SANITIZER_COUNT GREATER 0)
    if(MSVC)
        message(FATAL_ERROR "Cedar sanitizer profiles currently require a Clang or GCC toolchain")
    endif()
    if(CEDAR_ENABLE_ASAN)
        set(CEDAR_SANITIZER_FLAG "-fsanitize=address")
    elseif(CEDAR_ENABLE_UBSAN)
        set(CEDAR_SANITIZER_FLAG "-fsanitize=undefined")
    else()
        set(CEDAR_SANITIZER_FLAG "-fsanitize=thread")
    endif()
    add_compile_options(${CEDAR_SANITIZER_FLAG} -fno-omit-frame-pointer)
    add_link_options(${CEDAR_SANITIZER_FLAG})
endif()

if(APPLE)
    execute_process(COMMAND xcrun --sdk macosx --show-sdk-path
                    OUTPUT_VARIABLE OSX_SDK_PATH
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(OSX_CXX_V1 "${OSX_SDK_PATH}/usr/include/c++/v1")
    if(EXISTS "${OSX_CXX_V1}/cstdint" AND NOT EXISTS "/usr/include/c++/v1/cstdint")
        add_compile_options(-isystem "${OSX_CXX_V1}")
    endif()
endif()

include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/src)

include(cmake/CedarRocksDB.cmake)

set(CEDAR_SOURCES
    src/core/status.cc
    src/core/crc32c.cc
    src/types/value.cc
    src/fact/fact.cc
    src/fact/fact_codec.cc
    src/fact/meta_codec.cc
    src/index/canonical_value.cc
    src/index/index_sidecar.cc
    src/index/hybrid_scan.cc
    src/index/index_catalog.cc
    src/optimizer/cost_model.cc
    src/optimizer/runtime_feedback.cc
    src/statistics/stats_fragment.cc
    src/statistics/stats_snapshot.cc
    src/schema/schema_registry.cc
    src/columnar/page_format.cc
    src/columnar/granule_block.cc
    src/columnar/sst.cc
    src/columnar/temporal_read_merger.cc
    src/storage/temporal_event.cc
    src/storage/temporal_memtable.cc
    src/storage/storage_shard.cc
    src/storage/version_set.cc
    src/storage/sst_flush.cc
    src/storage/sst_compaction.cc
    src/transaction/logical_key.cc
    src/transaction/database_format.cc
    src/transaction/logical_id_allocator.cc
    src/transaction/shard_directory.cc
    src/transaction/decision_log.cc
    src/transaction/commit_timeline.cc
    src/transaction/visible_prefix.cc
    src/transaction/transaction_measurements.cc
    src/transaction/transaction_coordinator.cc
    src/db/database_lifecycle.cc
    src/db/cedar_database.cc
    src/tcypher/runtime/column_batch.cc
    src/tcypher/runtime/query_result.cc
    src/tcypher/runtime/query_runtime.cc
    src/tcypher/runtime/query_spill.cc
    src/tcypher/runtime/vector_filter.cc
    src/tcypher/runtime/vector_project.cc
    src/tcypher/runtime/vector_pipeline.cc
    src/tcypher/runtime/transaction_sink.cc
    src/tcypher/runtime/vector_expand.cc
    src/tcypher/runtime/interval_derive.cc
    src/tcypher/runtime/interval_align.cc
    src/tcypher/runtime/temporal_coalesce.cc
    src/tcypher/storage/temporal_scan.cc
    src/tcypher/storage/property_gather.cc
    src/tcypher/syntax/tokenizer.cc
    src/tcypher/syntax/parser.cc
    src/tcypher/query_snapshot.cc
    src/tcypher/physical_plan.cc
    src/tcypher/executor.cc
    src/tcypher/binder.cc
    src/tcypher/logical_plan.cc
    src/blob/blob_store.cc
    src/blob/blob_gc.cc
    src/blob/blob_reference_catalog.cc
    src/benchmark/cedar_tg.cc
    src/benchmark/fault_campaign.cc
    src/benchmark/ldbc_adapter.cc
    src/benchmark/run_manifest.cc
    src/benchmark/environment_probe.cc
    src/benchmark/phase_runner.cc
    src/benchmark/profile.cc
    src/benchmark/production_campaign.cc
    src/benchmark/artifact_writer.cc
    src/benchmark/artifact_reader.cc
    src/benchmark/arrival_scheduler.cc
    src/benchmark/workload_driver.cc
    src/benchmark/report_builder.cc
    src/benchmark/regression_compare.cc
    src/benchmark/regression_gate.cc
    src/benchmark/regression_orchestrator.cc
    src/cache/cache_manager.cc
    src/observability/event_ring.cc
    src/observability/explain_analyze_profile.cc
    src/observability/metric_exporter.cc
    src/observability/operator_runtime_stats.cc
    src/observability/production_metric_schema.cc
    src/observability/telemetry_aggregator.cc
    src/runtime/io_governor.cc
    src/runtime/work_execution_service.cc
    src/runtime/maintenance_executor.cc
)

add_library(cedar_blake3 STATIC
    third_party/blake3/blake3.c
    third_party/blake3/blake3_dispatch.c
    third_party/blake3/blake3_portable.c
)
target_include_directories(cedar_blake3 PUBLIC ${CMAKE_SOURCE_DIR}/third_party/blake3)
target_compile_definitions(cedar_blake3 PRIVATE
    BLAKE3_USE_NEON=0 BLAKE3_NO_SSE2 BLAKE3_NO_SSE41 BLAKE3_NO_AVX2 BLAKE3_NO_AVX512)

# Page compression is part of the current on-disk format. Build the pinned
# source snapshots shipped with Cedar so release binaries are independent of
# host packages and can be reproduced offline.
add_library(cedar_lz4 STATIC third_party/lz4/lib/lz4.c)
target_include_directories(cedar_lz4 PUBLIC
    ${CMAKE_SOURCE_DIR}/third_party/lz4/lib)
set_target_properties(cedar_lz4 PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(cedar STATIC ${CEDAR_SOURCES})
target_include_directories(cedar PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(cedar PRIVATE cedar_blake3 cedar_lz4)
if(CEDAR_MINIMAL_INSTRUMENTATION)
    target_compile_definitions(cedar PUBLIC CEDAR_MINIMAL_INSTRUMENTATION=1)
else()
    target_compile_definitions(cedar PUBLIC CEDAR_MINIMAL_INSTRUMENTATION=0)
endif()
option(CEDAR_ENABLE_ZSTD "Compile Cedar's bundled Zstd page codec" ON)
if(CEDAR_ENABLE_ZSTD)
    file(GLOB CEDAR_ZSTD_COMMON_SOURCES CONFIGURE_DEPENDS
        third_party/zstd/lib/common/*.c)
    file(GLOB CEDAR_ZSTD_COMPRESS_SOURCES CONFIGURE_DEPENDS
        third_party/zstd/lib/compress/*.c)
    file(GLOB CEDAR_ZSTD_DECOMPRESS_SOURCES CONFIGURE_DEPENDS
        third_party/zstd/lib/decompress/*.c)
    add_library(cedar_zstd STATIC
        ${CEDAR_ZSTD_COMMON_SOURCES}
        ${CEDAR_ZSTD_COMPRESS_SOURCES}
        ${CEDAR_ZSTD_DECOMPRESS_SOURCES})
    target_include_directories(cedar_zstd PUBLIC
        ${CMAKE_SOURCE_DIR}/third_party/zstd/lib)
    target_include_directories(cedar_zstd PRIVATE
        ${CMAKE_SOURCE_DIR}/third_party/zstd/lib/common
        ${CMAKE_SOURCE_DIR}/third_party/zstd/lib/compress
        ${CMAKE_SOURCE_DIR}/third_party/zstd/lib/decompress)
    target_compile_definitions(cedar_zstd PRIVATE
        ZSTD_DISABLE_ASM=1 ZSTD_MULTITHREAD=0)
    set_target_properties(cedar_zstd PROPERTIES POSITION_INDEPENDENT_CODE ON)
    target_link_libraries(cedar PRIVATE cedar_zstd)
    target_compile_definitions(cedar PUBLIC CEDAR_HAS_ZSTD=1)
else()
    target_compile_definitions(cedar PUBLIC CEDAR_HAS_ZSTD=0)
endif()

add_executable(cedar_bench benchmarks/cedar_bench.cc)
target_link_libraries(cedar_bench PRIVATE cedar)
add_executable(cedar_bench_report benchmarks/cedar_bench_report.cc)
target_link_libraries(cedar_bench_report PRIVATE cedar)
add_executable(cedar_evidence_verify benchmarks/cedar_evidence_verify.cc)
target_link_libraries(cedar_evidence_verify PRIVATE cedar)
add_executable(cedar_bench_pair benchmarks/cedar_bench_pair.cc)
target_link_libraries(cedar_bench_pair PRIVATE cedar)
add_executable(cedar_production_campaign
    benchmarks/cedar_production_campaign.cc)
target_link_libraries(cedar_production_campaign PRIVATE cedar)
add_executable(cedar_bench_alt benchmarks/cedar_bench.cc)
target_link_libraries(cedar_bench_alt PRIVATE cedar)
target_compile_definitions(cedar_bench_alt PRIVATE CEDAR_BENCH_VARIANT="alt")

find_program(CEDAR_GIT_EXECUTABLE git)
if(NOT CEDAR_GIT_EXECUTABLE)
    set(CEDAR_GIT_EXECUTABLE git)
endif()
set(CEDAR_BENCHMARK_PROVENANCE_HEADER
    "${CMAKE_BINARY_DIR}/generated/cedar/benchmark/build_provenance.h")
add_custom_target(cedar_benchmark_provenance
    COMMAND ${CMAKE_COMMAND}
        -DSOURCE_DIR=${CMAKE_SOURCE_DIR}
        -DOUTPUT_FILE=${CEDAR_BENCHMARK_PROVENANCE_HEADER}
        -DGIT_EXECUTABLE=${CEDAR_GIT_EXECUTABLE}
        -P ${CMAKE_SOURCE_DIR}/cmake/GenerateBenchmarkProvenance.cmake
    BYPRODUCTS ${CEDAR_BENCHMARK_PROVENANCE_HEADER}
    VERBATIM)
foreach(CEDAR_BENCHMARK_TARGET cedar_bench cedar_bench_alt)
    add_dependencies(${CEDAR_BENCHMARK_TARGET} cedar_benchmark_provenance)
    target_include_directories(${CEDAR_BENCHMARK_TARGET} PRIVATE
        "${CMAKE_BINARY_DIR}/generated")
endforeach()

option(BUILD_TESTS "Build Cedar test programs" OFF)
if(BUILD_TESTS)
    enable_testing()
    add_test(NAME release_source_contract
        COMMAND ${CMAKE_COMMAND}
            -DSOURCE_ROOT=${CMAKE_SOURCE_DIR}
            -DOUTPUT_ROOT=${CMAKE_BINARY_DIR}/release-source-contract
            -P ${CMAKE_SOURCE_DIR}/cmake/VerifyReleaseSourceContract.cmake)
    add_test(NAME release_source_contract_negative
        COMMAND ${CMAKE_COMMAND}
            -DSCANNER=${CMAKE_SOURCE_DIR}/cmake/VerifyReleaseSourceContract.cmake
            -DTEST_ROOT=${CMAKE_BINARY_DIR}/release-source-contract-negative
            -P ${CMAKE_SOURCE_DIR}/tests/test_release_source_contract.cmake)
    add_test(NAME benchmark_provenance_generation
        COMMAND ${CMAKE_COMMAND}
            -DGENERATOR=${CMAKE_SOURCE_DIR}/cmake/GenerateBenchmarkProvenance.cmake
            -DTEST_ROOT=${CMAKE_BINARY_DIR}/benchmark-provenance-test-repo
            -P ${CMAKE_SOURCE_DIR}/tests/test_benchmark_provenance.cmake)
    find_package(GTest CONFIG QUIET)
    if(NOT GTest_FOUND)
        include(FetchContent)
        FetchContent_Declare(
            googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG v1.15.2
        )
        FetchContent_MakeAvailable(googletest)
    endif()
    add_subdirectory(tests)
endif()
