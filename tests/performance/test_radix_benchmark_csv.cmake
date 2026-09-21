if(NOT DEFINED CEDAR_RADIX_BENCHMARK OR NOT EXISTS "${CEDAR_RADIX_BENCHMARK}")
  message(FATAL_ERROR "cedar radix benchmark executable is missing")
endif()

execute_process(
  COMMAND "${CEDAR_RADIX_BENCHMARK}" --entries 64 --writers 1 --phase all
  RESULT_VARIABLE result
  OUTPUT_VARIABLE output
  ERROR_VARIABLE error)
if(NOT result EQUAL 0)
  message(FATAL_ERROR "radix benchmark failed: ${error}\n${output}")
endif()
string(REPLACE "\n" ";" lines "${output}")
list(GET lines 0 header)
list(GET lines 1 row)
string(REPLACE "," ";" header_fields "${header}")
string(REPLACE "," ";" row_fields "${row}")
list(LENGTH header_fields header_count)
list(LENGTH row_fields row_count)
if(NOT header_count EQUAL 28 OR NOT row_count EQUAL 28)
  message(FATAL_ERROR "unexpected radix benchmark CSV width: header=${header_count}, row=${row_count}\n${output}")
endif()
foreach(required IN ITEMS phase elapsed_ns index_insert_ns allocate_and_insert_ns
    active_scan_ns_phase freeze_prepare_ns first_frozen_scan_ns ready_frozen_scan_ns)
  list(FIND header_fields "${required}" index)
  if(index LESS 0)
    message(FATAL_ERROR "missing ${required} in ${header}")
  endif()
endforeach()
list(FIND header_fields p50_ns p50_index)
list(GET row_fields ${p50_index} p50)
if(NOT p50 STREQUAL "NA")
  message(FATAL_ERROR "disabled latency sampling must emit NA, got ${p50}")
endif()
list(FIND header_fields errors errors_index)
list(GET row_fields ${errors_index} errors)
if(NOT errors STREQUAL "0")
  message(FATAL_ERROR "benchmark reported errors=${errors}")
endif()

# Diagnostics are deliberately emitted on stderr so consumers of the stable
# benchmark CSV schema do not need to special-case instrumented runs.
execute_process(
  COMMAND "${CEDAR_RADIX_BENCHMARK}" --entries 64 --writers 1 --phase index
          --implementation radix --stats
  RESULT_VARIABLE stats_result
  OUTPUT_VARIABLE stats_output
  ERROR_VARIABLE stats_error)
if(NOT stats_result EQUAL 0)
  message(FATAL_ERROR "radix benchmark stats run failed: ${stats_error}\n${stats_output}")
endif()
string(REGEX MATCH
  "radix_stats,branch_loads,table_snapshot_cas,boundary_candidates[\r\n]+radix_stats,[1-9][0-9]*,[1-9][0-9]*,[1-9][0-9]*"
  stats_match "${stats_error}")
if(stats_match STREQUAL "")
  message(FATAL_ERROR "missing nonzero radix diagnostics: ${stats_error}")
endif()

# Two inserts create the first root branch but never descend through an
# existing branch. The diagnostic must snapshot at writer completion, before
# the benchmark's validation scans exercise that root branch.
execute_process(
  COMMAND "${CEDAR_RADIX_BENCHMARK}" --entries 2 --writers 1 --phase index
          --implementation radix --stats
  RESULT_VARIABLE insertion_only_result
  OUTPUT_VARIABLE insertion_only_output
  ERROR_VARIABLE insertion_only_error)
if(NOT insertion_only_result EQUAL 0)
  message(FATAL_ERROR "two-entry radix stats run failed: ${insertion_only_error}\n${insertion_only_output}")
endif()
string(REGEX MATCH
  "radix_stats,branch_loads,table_snapshot_cas,boundary_candidates[\r\n]+radix_stats,0,0,0"
  insertion_only_match "${insertion_only_error}")
if(insertion_only_match STREQUAL "")
  message(FATAL_ERROR "diagnostics include post-insert traversal: ${insertion_only_error}")
endif()

foreach(read_phase IN ITEMS point miss seek-prev range1 range16 range256)
  execute_process(
    COMMAND "${CEDAR_RADIX_BENCHMARK}" --entries 64 --writers 1
            --workload ascending --phase "${read_phase}"
    RESULT_VARIABLE read_result OUTPUT_VARIABLE read_output
    ERROR_VARIABLE read_error)
  if(NOT read_result EQUAL 0)
    message(FATAL_ERROR "read benchmark phase ${read_phase} failed: ${read_error}\n${read_output}")
  endif()
  string(REPLACE "\n" ";" read_lines "${read_output}")
  list(GET read_lines 1 read_row)
  string(REPLACE "," ";" read_fields "${read_row}")
  list(FIND header_fields phase phase_index)
  list(GET read_fields ${phase_index} observed_phase)
  if(NOT observed_phase STREQUAL "${read_phase}")
    message(FATAL_ERROR "phase mismatch: expected ${read_phase}, got ${observed_phase}")
  endif()
  list(FIND header_fields ops read_ops_index)
  list(GET read_fields ${read_ops_index} read_ops)
  if(NOT read_ops STREQUAL "64")
    message(FATAL_ERROR "phase ${read_phase} reported ops=${read_ops}")
  endif()
  list(FIND header_fields errors read_errors_index)
  list(GET read_fields ${read_errors_index} read_errors)
  if(NOT read_errors STREQUAL "0")
    message(FATAL_ERROR "phase ${read_phase} reported errors=${read_errors}")
  endif()
endforeach()
