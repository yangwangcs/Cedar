cmake_minimum_required(VERSION 3.14)
project(cedar VERSION 2.0.0 LANGUAGES C CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(CEDAR_ENABLE_ASAN "Build Cedar with AddressSanitizer" OFF)
option(CEDAR_ENABLE_UBSAN "Build Cedar with UndefinedBehaviorSanitizer" OFF)
option(CEDAR_ENABLE_TSAN "Build Cedar with ThreadSanitizer" OFF)

set(CEDAR_SANITIZER_COUNT 0)
foreach(CEDAR_SANITIZER_OPTION
        CEDAR_ENABLE_ASAN CEDAR_ENABLE_UBSAN CEDAR_ENABLE_TSAN)
    if(${CEDAR_SANITIZER_OPTION})
        math(EXPR CEDAR_SANITIZER_COUNT "${CEDAR_SANITIZER_COUNT} + 1")
    endif()
endforeach()
if(CEDAR_SANITIZER_COUNT GREATER 1)
    message(FATAL_ERROR "Enable at most one sanitizer")
endif()

if(CEDAR_SANITIZER_COUNT GREATER 0)
    if(MSVC)
        message(FATAL_ERROR "Cedar sanitizer profiles require Clang or GCC")
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

include(cmake/CedarRocksDB.cmake)

add_library(cedar_core STATIC
    src/core/crc32c.cc
    src/core/status.cc
    src/query/types.cc
    src/query/temporal/interval.cc
    src/query/temporal/corrected_chain.cc
    src/query/logical/expression.cc
    src/query/logical/logical_plan.cc
    src/query/query_api.cc
    src/query/runtime/canonical_source.cc
    src/query/runtime/query_runtime.cc
    src/types/value.cc
    src/storage/rocks/commit_publisher.cc
    src/storage/rocks/decided_epoch.cc
    src/storage/facts/fact.cc
    src/storage/facts/fact_codec.cc
    src/storage/facts/group_commit_planner.cc
    src/storage/facts/pending_version_overlay.cc
    src/storage/facts/recent_fact_write_index.cc
    src/storage/facts/version_validation_cache.cc
    src/storage/facts/meta_codec.cc
    src/storage/facts/prepared_commit_codec.cc
    src/storage/rocks/rocksdb_config.cc
    src/storage/rocks/storage_file_inspection.cc
    src/storage/facts/vacuum.cc
    src/storage/rocks/rocks_adapter.cc
    src/runtime/pressure_controller.cc
    src/kernel/adaptive_epoch_controller.cc
    src/kernel/epoch_completion.cc
    src/kernel/database.cc
    src/kernel/async_submission_executor.cc
    src/kernel/maintenance_controller.cc
    src/kernel/maintenance_policy.cc
    src/kernel/snapshot.cc
    src/kernel/temporal_validation.cc
    src/kernel/transaction.cc)
target_include_directories(cedar_core
    PUBLIC $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE ${CMAKE_SOURCE_DIR}/src
            ${CEDAR_ENGINE_SOURCE_DIR})
target_link_libraries(cedar_core PRIVATE cedar_rocksdb)
set_property(TARGET cedar_core PROPERTY INTERFACE_LINK_LIBRARIES
    "$<BUILD_INTERFACE:cedar_rocksdb>")
target_compile_features(cedar_core PUBLIC cxx_std_20)
set_target_properties(cedar_core PROPERTIES EXPORT_NAME cedar)
add_library(Cedar::cedar ALIAS cedar_core)

add_executable(cedar tools/cedar_main.cc)
target_link_libraries(cedar PRIVATE cedar_core)

add_library(cedar_kernel_bench_options STATIC
    benchmarks/cedar_kernel_bench_options.cc)
target_include_directories(cedar_kernel_bench_options PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(cedar_kernel_bench_options PUBLIC cedar_core)

add_library(cedar_kernel_bench_workload STATIC
    benchmarks/cedar_kernel_bench_workload.cc)
target_include_directories(cedar_kernel_bench_workload PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(cedar_kernel_bench_workload PUBLIC cedar_core)

option(BUILD_BENCHMARKS "Build Cedar kernel benchmarks" OFF)
if(BUILD_BENCHMARKS)
    add_executable(cedar_kernel_bench benchmarks/cedar_kernel_bench.cc)
    target_link_libraries(cedar_kernel_bench PRIVATE cedar_kernel_bench_options
        cedar_kernel_bench_workload)
    add_executable(cedar_kernel_columnar_bench
        benchmarks/cedar_kernel_columnar_bench.cc)
    target_include_directories(cedar_kernel_columnar_bench PRIVATE
        ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/src/engine/rocksdb)
    target_link_libraries(cedar_kernel_columnar_bench PRIVATE cedar_core)
endif()

install(TARGETS cedar_core EXPORT CedarTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(TARGETS cedar RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES
    "${CEDAR_ROCKSDB_LIBRARY}"
    "${CEDAR_ROCKSDB_LZ4_LIBRARY}"
    "${CEDAR_ROCKSDB_ZSTD_LIBRARY}"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cedar/internal")
install(DIRECTORY include/cedar DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")
configure_package_config_file(
    cmake/CedarConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/CedarConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Cedar")
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/CedarConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(EXPORT CedarTargets
    FILE CedarTargets.cmake
    NAMESPACE Cedar::
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Cedar")
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/CedarConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/CedarConfigVersion.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Cedar")

option(BUILD_TESTS "Build Cedar test programs" OFF)
if(BUILD_TESTS)
    enable_testing()
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
