# Cedar 存储层基准 (P0 baseline)
# 使用: ./build/benchmarks/storage_bench --json > benchmarks/baseline.json

add_executable(storage_bench storage_bench.cc)
target_link_libraries(storage_bench cedar pthread)

# 把构建环境写进基准元数据（机器/编译器/commit/构建类型）
execute_process(
    COMMAND git rev-parse --short=12 HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE CEDAR_GIT_COMMIT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)
if(NOT CEDAR_GIT_COMMIT)
    set(CEDAR_GIT_COMMIT "unknown")
endif()

target_compile_definitions(storage_bench PRIVATE
    CEDAR_BENCH_GIT_COMMIT="${CEDAR_GIT_COMMIT}"
    CEDAR_BENCH_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
    CEDAR_BENCH_COMPILER="${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
