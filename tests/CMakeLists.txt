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
    ${GTEST_MAIN_TARGET}
)

add_executable(test_correctness_kernel test_correctness_kernel.cc)
target_link_libraries(test_correctness_kernel ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_correctness_kernel)

add_executable(test_rocksdb_dependency test_rocksdb_dependency.cc)
target_link_libraries(test_rocksdb_dependency
    PRIVATE cedar_rocksdb ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_rocksdb_dependency)

add_executable(test_fact_model test_fact_model.cc)
target_link_libraries(test_fact_model ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_fact_model)

add_executable(test_fact_codec test_fact_codec.cc)
target_link_libraries(test_fact_codec ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_fact_codec)

add_executable(test_fact_store test_fact_store.cc)
target_link_libraries(test_fact_store
    PRIVATE cedar_fact_store cedar_rocksdb ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_fact_store)

add_executable(test_fact_store_commit test_fact_store_commit.cc)
target_link_libraries(test_fact_store_commit
    PRIVATE cedar_fact_store cedar_rocksdb ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_fact_store_commit)

add_executable(test_fact_store_metadata test_fact_store_metadata.cc)
target_link_libraries(test_fact_store_metadata
    PRIVATE cedar_fact_store cedar_rocksdb ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_fact_store_metadata)

add_executable(test_kernel_interface test_kernel_interface.cc)
target_link_libraries(test_kernel_interface
    PRIVATE cedar_kernel ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_kernel_interface)
