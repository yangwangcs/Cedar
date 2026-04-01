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
