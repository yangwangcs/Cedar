if(NOT DEFINED CEDAR_BENCHMARK OR NOT EXISTS "${CEDAR_BENCHMARK}")
  message(FATAL_ERROR "cedar columnar benchmark executable is missing")
endif()

string(RANDOM LENGTH 12 ALPHABET 0123456789 COLUMNAR_RANDOM)
set(database_path "/tmp/cedar-kernel-columnar-smoke-${COLUMNAR_RANDOM}")
execute_process(
  COMMAND "${CEDAR_BENCHMARK}"
          --path "${database_path}"
          --rows 512
          --iterations 2
  RESULT_VARIABLE benchmark_result
  OUTPUT_VARIABLE benchmark_output
  ERROR_VARIABLE benchmark_error)
file(REMOVE_RECURSE "${database_path}")
if(NOT benchmark_result EQUAL 0)
  message(FATAL_ERROR
    "columnar benchmark failed with ${benchmark_result}: ${benchmark_error}\n${benchmark_output}")
endif()
string(REPLACE "\n" ";" benchmark_lines "${benchmark_output}")
list(GET benchmark_lines 1 row)
string(REPLACE "," ";" row_fields "${row}")
list(GET row_fields 4 persisted_sst_bytes)
list(GET row_fields 5 scanned_rows)
if(persisted_sst_bytes LESS 1 OR scanned_rows LESS 1)
  message(FATAL_ERROR
    "columnar benchmark did not produce a persisted scan: ${benchmark_output}")
endif()
list(GET row_fields 9 qualification)
if(NOT qualification STREQUAL "persisted_sst_and_rows")
  message(FATAL_ERROR "invalid columnar benchmark qualification: ${benchmark_output}")
endif()
