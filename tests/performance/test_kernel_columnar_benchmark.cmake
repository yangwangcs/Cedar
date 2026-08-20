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
list(GET benchmark_lines 0 header)
string(REPLACE "," ";" header_fields "${header}")
string(REPLACE "," ";" row_fields "${row}")
list(GET row_fields 4 persisted_sst_bytes)
list(GET row_fields 5 scanned_rows)
if(persisted_sst_bytes LESS 1 OR scanned_rows LESS 1)
  message(FATAL_ERROR
    "columnar benchmark did not produce a persisted scan: ${benchmark_output}")
endif()
list(GET row_fields 12 qualification)
if(NOT qualification STREQUAL "persisted_sst_and_rows")
  message(FATAL_ERROR "invalid columnar benchmark qualification: ${benchmark_output}")
endif()
list(LENGTH header_fields header_count)
list(LENGTH row_fields row_count)
if(header_count LESS 13 OR row_count LESS 13)
  message(FATAL_ERROR
    "columnar benchmark is missing physical scan metrics: ${benchmark_output}")
endif()
foreach(required_field IN ITEMS
    projected_scan_pages_skipped projected_scan_pages_read
    projected_scan_physical_bytes_read)
  list(FIND header_fields "${required_field}" field_index)
  if(field_index LESS 0)
    message(FATAL_ERROR
      "columnar benchmark is missing ${required_field}: ${benchmark_output}")
  endif()
endforeach()
