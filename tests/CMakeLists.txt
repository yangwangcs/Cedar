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
