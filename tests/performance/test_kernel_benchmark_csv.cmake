if(NOT DEFINED CEDAR_BENCHMARK OR NOT EXISTS "${CEDAR_BENCHMARK}")
  message(FATAL_ERROR "cedar kernel benchmark executable is missing")
endif()

string(RANDOM LENGTH 12 ALPHABET 0123456789 CSV_RANDOM)
set(database_path "/tmp/cedar-kernel-csv-contract-${CSV_RANDOM}")
execute_process(
  COMMAND "${CEDAR_BENCHMARK}"
          --path "${database_path}"
          --duration-seconds 1
          --writer-clients 2
          --workload property-put
          --verify-reopen false
          --campaign none
  RESULT_VARIABLE benchmark_result
  OUTPUT_VARIABLE benchmark_output
  ERROR_VARIABLE benchmark_error)
file(REMOVE_RECURSE "${database_path}")
if(NOT benchmark_result EQUAL 0)
  message(FATAL_ERROR
    "benchmark failed with ${benchmark_result}: ${benchmark_error}\n${benchmark_output}")
endif()

string(REPLACE "\n" ";" benchmark_lines "${benchmark_output}")
list(GET benchmark_lines 0 header)
list(GET benchmark_lines 1 row)
string(REPLACE "," ";" header_fields "${header}")
string(REPLACE "," ";" row_fields "${row}")
list(LENGTH header_fields header_count)
list(LENGTH row_fields row_count)
if(NOT header_count EQUAL 65 OR NOT row_count EQUAL 65)
  message(FATAL_ERROR
    "unexpected Cedar benchmark CSV width: header=${header_count}, row=${row_count}\n${benchmark_output}")
endif()
foreach(required_field IN ITEMS
    schema_version commit_epochs epoch_transactions epoch_bytes wal_sync_count
    wal_rotations group_fill_p50 group_fill_p95 group_fill_max queue_p50_us
    wal_sync_p95_us publication_p99_us qualification)
  list(FIND header_fields "${required_field}" field_index)
  if(field_index LESS 0)
    message(FATAL_ERROR "benchmark CSV is missing ${required_field}: ${header}")
  endif()
endforeach()
