if(NOT DEFINED CEDAR_RADIX_BENCHMARK)
  message(FATAL_ERROR "CEDAR_RADIX_BENCHMARK is required")
endif()
set(script "${CMAKE_CURRENT_LIST_DIR}/../../benchmarks/run_cedar_patricia_retention_matrix.sh")
set(summarizer "${CMAKE_CURRENT_LIST_DIR}/../../benchmarks/summarize_cedar_patricia_retention.py")
if(NOT EXISTS "${script}" OR NOT EXISTS "${summarizer}")
  message(FATAL_ERROR "retention runner and summarizer must exist")
endif()
set(output "${CMAKE_CURRENT_BINARY_DIR}/patricia-retention-contract")
file(REMOVE_RECURSE "${output}")
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env CEDAR_RETENTION_TEST_MODE=1 bash "${script}"
          --nibble "${CEDAR_RADIX_BENCHMARK}" --byte "${CEDAR_RADIX_BENCHMARK}"
          --output "${output}"
  RESULT_VARIABLE runner_status OUTPUT_VARIABLE runner_out ERROR_VARIABLE runner_err)
if(NOT runner_status EQUAL 0)
  message(FATAL_ERROR "retention runner failed: ${runner_err}\n${runner_out}")
endif()
execute_process(
  COMMAND python3 "${summarizer}" --self-test
  RESULT_VARIABLE summary_status OUTPUT_VARIABLE summary_out ERROR_VARIABLE summary_err)
if(NOT summary_status EQUAL 0)
  message(FATAL_ERROR "retention summarizer self-test failed: ${summary_err}\n${summary_out}")
endif()
file(READ "${output}/matrix-raw/matrix.csv" matrix)
execute_process(
  COMMAND awk -F, [=[NR == 1 { if (NF != 29) exit 1 } NR > 1 { if (NF != 29 || $10 != 64 || $18 != 0) exit 1 }]=]
          "${output}/matrix-raw/matrix.csv"
  RESULT_VARIABLE csv_status)
if(NOT csv_status EQUAL 0)
  message(FATAL_ERROR "retention matrix violates its 29-column/count/error contract")
endif()
string(FIND "${matrix}" "radix_stats" stats_pos)
if(NOT stats_pos EQUAL -1)
  message(FATAL_ERROR "timed retention matrix must not contain stats output")
endif()
