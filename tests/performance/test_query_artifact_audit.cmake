if(NOT DEFINED CEDAR_CAMPAIGN OR NOT DEFINED CEDAR_BENCHMARK)
  message(FATAL_ERROR "CEDAR_CAMPAIGN and CEDAR_BENCHMARK are required")
endif()

string(RANDOM LENGTH 8 ALPHABET 0123456789 TOKEN)
set(OUTPUT "/tmp/cedar-query-artifact-audit-${TOKEN}")
message(STATUS "artifact audit contract output: ${OUTPUT}")
get_filename_component(BUILD_DIR "${CEDAR_BENCHMARK}" DIRECTORY)
string(ASCII 10 LF)
set(HEADER "exit_code,dataset_checksum,authoritative_bytes,derived_bytes,statistics_bytes,scratch_bytes,space_amplification,reopen_verified,hard_gate_pass,terminal_status${LF}")
set(PASS_ROW "0,123,100,100,1,0,1.0,true,true,OK${LF}")
set(REOPEN_ROW "0,123,100,100,1,0,1.0,false,true,OK${LF}")
set(SPACE_ROW "0,123,100,160,3,1,1.6,true,true,OK${LF}")

file(MAKE_DIRECTORY "${OUTPUT}/pass")
file(WRITE "${OUTPUT}/pass/run.csv" "${HEADER}${PASS_ROW}")
execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}"
    --phase reopen-verification --input "${OUTPUT}/pass" --output "${OUTPUT}/pass-out"
  RESULT_VARIABLE PASS_REOPEN_RC OUTPUT_VARIABLE PASS_REOPEN_OUT ERROR_VARIABLE PASS_REOPEN_ERR)
if(NOT PASS_REOPEN_RC EQUAL 0)
  message(FATAL_ERROR "synthetic passing reopen audit failed: ${PASS_REOPEN_RC}\n${PASS_REOPEN_ERR}\n${PASS_REOPEN_OUT}")
endif()
execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}"
    --phase space-audit --input "${OUTPUT}/pass" --output "${OUTPUT}/pass-space-out"
  RESULT_VARIABLE PASS_SPACE_RC OUTPUT_VARIABLE PASS_SPACE_OUT ERROR_VARIABLE PASS_SPACE_ERR)
if(NOT PASS_SPACE_RC EQUAL 0)
  message(FATAL_ERROR "synthetic passing space audit failed: ${PASS_SPACE_RC}\n${PASS_SPACE_ERR}\n${PASS_SPACE_OUT}")
endif()

file(MAKE_DIRECTORY "${OUTPUT}/reopen-mismatch")
file(WRITE "${OUTPUT}/reopen-mismatch/run.csv" "${HEADER}${REOPEN_ROW}")
execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}"
    --phase reopen-verification --input "${OUTPUT}/reopen-mismatch" --output "${OUTPUT}/reopen-mismatch-out"
  RESULT_VARIABLE REOPEN_RC)
if(REOPEN_RC EQUAL 0)
  message(FATAL_ERROR "reopen mismatch was accepted")
endif()

file(MAKE_DIRECTORY "${OUTPUT}/space-violation")
file(WRITE "${OUTPUT}/space-violation/run.csv" "${HEADER}${SPACE_ROW}")
execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}"
    --phase space-audit --input "${OUTPUT}/space-violation" --output "${OUTPUT}/space-violation-out"
  RESULT_VARIABLE SPACE_RC)
if(SPACE_RC EQUAL 0)
  message(FATAL_ERROR "space violation was accepted")
endif()

file(MAKE_DIRECTORY "${OUTPUT}/duplicate-header")
set(DUPLICATE_HEADER "exit_code,dataset_checksum,dataset_checksum,authoritative_bytes,derived_bytes,statistics_bytes,scratch_bytes,space_amplification,reopen_verified,hard_gate_pass,terminal_status${LF}")
set(DUPLICATE_ROW "0,123,123,100,100,1,0,1.0,true,true,OK${LF}")
file(WRITE "${OUTPUT}/duplicate-header/run.csv" "${DUPLICATE_HEADER}${DUPLICATE_ROW}")
execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}"
    --phase reopen-verification --input "${OUTPUT}/duplicate-header" --output "${OUTPUT}/duplicate-header-out"
  RESULT_VARIABLE DUPLICATE_HEADER_RC OUTPUT_VARIABLE DUPLICATE_HEADER_OUT ERROR_VARIABLE DUPLICATE_HEADER_ERR)
if(DUPLICATE_HEADER_RC EQUAL 0)
  message(FATAL_ERROR "duplicate header was accepted\n${DUPLICATE_HEADER_ERR}\n${DUPLICATE_HEADER_OUT}")
endif()
if(NOT EXISTS "${OUTPUT}/duplicate-header-out/audit-summary.csv" OR
   NOT EXISTS "${OUTPUT}/duplicate-header-out/audit-summary.json")
  message(FATAL_ERROR "duplicate-header audit output missing")
endif()
file(READ "${OUTPUT}/duplicate-header-out/audit-summary.csv" DUPLICATE_HEADER_CSV)
string(FIND "${DUPLICATE_HEADER_CSV}" "FAIL,duplicate header field: dataset_checksum" DUPLICATE_HEADER_REASON_POS)
if(DUPLICATE_HEADER_REASON_POS LESS 0)
  message(FATAL_ERROR "duplicate-header CSV missing FAIL reason: ${DUPLICATE_HEADER_CSV}")
endif()
file(READ "${OUTPUT}/duplicate-header-out/audit-summary.json" DUPLICATE_HEADER_JSON)
string(FIND "${DUPLICATE_HEADER_JSON}" "\"pass\":false" DUPLICATE_HEADER_PASS_POS)
string(FIND "${DUPLICATE_HEADER_JSON}" "\"failed_rows\":1" DUPLICATE_HEADER_FAILED_ROWS_POS)
if(DUPLICATE_HEADER_PASS_POS LESS 0 OR DUPLICATE_HEADER_FAILED_ROWS_POS LESS 0)
  message(FATAL_ERROR "duplicate-header JSON missing failure status: ${DUPLICATE_HEADER_JSON}")
endif()

foreach(OUTPUT_ROOT IN ITEMS "${OUTPUT}/pass-out" "${OUTPUT}/pass-space-out")
  if(NOT EXISTS "${OUTPUT_ROOT}/audit-summary.csv" OR NOT EXISTS "${OUTPUT_ROOT}/audit-summary.json")
    message(FATAL_ERROR "audit output missing under ${OUTPUT_ROOT}")
  endif()
endforeach()
