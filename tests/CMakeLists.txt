include(GoogleTest)

if(GTest_FOUND)
    set(GTEST_TARGET GTest::gtest)
    set(GTEST_MAIN_TARGET GTest::gtest_main)
else()
    set(GTEST_TARGET gtest)
    set(GTEST_MAIN_TARGET gtest_main)
endif()

set(CEDAR_TEST_LIBS
    cedar
    cedar_graph
    cedar_compression
    ${GTEST_MAIN_TARGET}
    pthread
)

add_executable(test_cedar_key test_cedar_key.cc)
target_link_libraries(test_cedar_key ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cedar_key)

add_executable(test_descriptor test_descriptor.cc)
target_link_libraries(test_descriptor ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_descriptor)

add_executable(test_cedar_graph_storage test_cedar_graph_storage.cc)
target_link_libraries(test_cedar_graph_storage ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cedar_graph_storage)

# ============================================================================
# Phase 0 / Task 0.2：存储层 P0 红测试（tests/storage/）
# 每个文件对应审计 P0 条目，修复后转绿；超时保护防止并发插入损坏链表后挂起。
# ============================================================================
set(CEDAR_STORAGE_TESTS
    test_zone_columnar_roundtrip   # 10.1 单行 SST / 2.1 多 block target
    test_flush_recovery            # 3.2 墓碑 flush 后复活
    test_edge_target               # 2.4/2.5/8.1/8.2 边 target 贯穿
    test_compaction                # 3.1/9.1/9.2/9.3 压缩列隔离与下层保留
    test_vsl_concurrency           # 1.1/1.9 8 线程 80k 行
)

foreach(storage_test ${CEDAR_STORAGE_TESTS})
    add_executable(${storage_test} storage/${storage_test}.cc)
    target_link_libraries(${storage_test} ${CEDAR_TEST_LIBS})
    gtest_discover_tests(${storage_test} PROPERTIES TIMEOUT 180)
endforeach()
