cmake_minimum_required(VERSION 3.14)
project(cedar VERSION 2.0.0 LANGUAGES C CXX)

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
    src/types/value.cc
    src/fact/commit_publisher.cc
    src/fact/decided_epoch.cc
    src/fact/fact.cc
    src/fact/fact_codec.cc
    src/fact/group_commit_planner.cc
    src/fact/pending_version_overlay.cc
    src/fact/recent_fact_write_index.cc
    src/fact/version_validation_cache.cc
    src/fact/meta_codec.cc
    src/fact/prepared_commit_codec.cc
    src/fact/rocksdb_config.cc
    src/fact/vacuum.cc
    src/fact/fact_store.cc
    src/fact/pressure_controller.cc
    src/kernel/database.cc
    src/kernel/async_submission_executor.cc
    src/kernel/maintenance_controller.cc
    src/kernel/maintenance_policy.cc
    src/kernel/snapshot.cc
    src/kernel/temporal_validation.cc
    src/kernel/transaction.cc)
target_include_directories(cedar_core
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_SOURCE_DIR}/src
            ${CEDAR_ROCKSDB_SOURCE_DIR})
target_link_libraries(cedar_core PUBLIC cedar_rocksdb)

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
endif()

install(TARGETS cedar_core
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin)
install(DIRECTORY include/cedar DESTINATION include
    FILES_MATCHING PATTERN "*.h")

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
