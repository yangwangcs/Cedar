if(NOT DEFINED CEDAR_SOURCE_DIR)
  message(FATAL_ERROR "CEDAR_SOURCE_DIR is required")
endif()

set(CEDAR_INSTALL_PREFIX "${CEDAR_SOURCE_DIR}/build-install-consumer-prefix")
set(CEDAR_CONSUMER_BUILD "${CEDAR_SOURCE_DIR}/build-install-consumer")

execute_process(
  COMMAND "${CMAKE_COMMAND}" -S "${CEDAR_SOURCE_DIR}" -B
          "${CEDAR_SOURCE_DIR}/build-install-producer"
          -DBUILD_TESTS=OFF -DBUILD_BENCHMARKS=OFF
          -DCMAKE_BUILD_TYPE=Debug
  RESULT_VARIABLE configure_result)
if(NOT configure_result EQUAL 0)
  message(FATAL_ERROR "failed to configure Cedar install producer")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build
          "${CEDAR_SOURCE_DIR}/build-install-producer" --parallel 2
  RESULT_VARIABLE build_result)
if(NOT build_result EQUAL 0)
  message(FATAL_ERROR "failed to build Cedar install producer")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" --install
          "${CEDAR_SOURCE_DIR}/build-install-producer" --prefix
          "${CEDAR_INSTALL_PREFIX}"
  RESULT_VARIABLE install_result)
if(NOT install_result EQUAL 0)
  message(FATAL_ERROR "failed to install Cedar")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" -S
          "${CEDAR_SOURCE_DIR}/tests/public/install_consumer" -B
          "${CEDAR_CONSUMER_BUILD}"
          -DCedar_DIR=${CEDAR_INSTALL_PREFIX}/lib/cmake/Cedar
  RESULT_VARIABLE consumer_configure_result)
if(NOT consumer_configure_result EQUAL 0)
  message(FATAL_ERROR "failed to configure Cedar install consumer")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build "${CEDAR_CONSUMER_BUILD}" --parallel 2
  RESULT_VARIABLE consumer_build_result)
if(NOT consumer_build_result EQUAL 0)
  message(FATAL_ERROR "failed to build Cedar install consumer")
endif()
execute_process(
  COMMAND "${CEDAR_CONSUMER_BUILD}/cedar_install_consumer"
  RESULT_VARIABLE consumer_run_result)
if(NOT consumer_run_result EQUAL 0)
  message(FATAL_ERROR "Cedar install consumer failed: ${consumer_run_result}")
endif()
