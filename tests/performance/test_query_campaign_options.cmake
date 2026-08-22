if(NOT DEFINED CEDAR_CAMPAIGN OR NOT DEFINED CEDAR_BENCHMARK)
  message(FATAL_ERROR "CEDAR_CAMPAIGN and CEDAR_BENCHMARK are required")
endif()

string(RANDOM LENGTH 8 ALPHABET 0123456789 TOKEN)
set(OUTPUT "/tmp/cedar-query-campaign-${TOKEN}")
get_filename_component(BUILD_DIR "${CEDAR_BENCHMARK}" DIRECTORY)
execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}"
    --build-dir "${BUILD_DIR}"
    --phase write-idle-five-repeats
    --duration-seconds 1
    --facts-per-txn 4,16
    --writers 1,2
    --output "${OUTPUT}/write"
  RESULT_VARIABLE RC OUTPUT_VARIABLE OUT ERROR_VARIABLE ERR)
if(NOT RC EQUAL 0)
  message(FATAL_ERROR "campaign write option contract failed: ${ERR}\n${OUT}")
endif()
file(READ "${OUTPUT}/write/commands.manifest" MANIFEST)
if(NOT MANIFEST MATCHES "--facts-per-txn=4" OR NOT MANIFEST MATCHES "--facts-per-txn=16")
  message(FATAL_ERROR "facts-per-txn was not applied to campaign commands: ${MANIFEST}")
endif()
if(NOT MANIFEST MATCHES "--writers=1" OR NOT MANIFEST MATCHES "--writers=2")
  message(FATAL_ERROR "writers was not applied to campaign commands: ${MANIFEST}")
endif()
if(NOT MANIFEST MATCHES "--path=/.*")
  message(FATAL_ERROR "campaign did not normalize database paths: ${MANIFEST}")
endif()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}"
    --build-dir "${BUILD_DIR}"
    --phase read-cold
    --duration-seconds 1
    --readers 1
    --degrees 1
    --selectivities 0.1
    --projection-states canonical
    --output "${OUTPUT}/read"
  RESULT_VARIABLE READ_RC OUTPUT_VARIABLE READ_OUT ERROR_VARIABLE READ_ERR)
if(NOT READ_RC EQUAL 0)
  message(FATAL_ERROR "campaign read option contract failed: ${READ_ERR}\n${READ_OUT}")
endif()
file(READ "${OUTPUT}/read/commands.manifest" READ_MANIFEST)
if(NOT READ_MANIFEST MATCHES "--readers=1" OR NOT READ_MANIFEST MATCHES "--degree=1" OR
   NOT READ_MANIFEST MATCHES "--selectivity-percent=0.1" OR
   NOT READ_MANIFEST MATCHES "--projection-state=canonical-only")
  message(FATAL_ERROR "read matrix options were not applied: ${READ_MANIFEST}")
endif()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}"
    --build-dir "${BUILD_DIR}"
    --phase release-calibration
    --duration-seconds 1
    --facts-per-txn auto-turning-point
    --output "${OUTPUT}/auto"
  RESULT_VARIABLE AUTO_RC OUTPUT_VARIABLE AUTO_OUT ERROR_VARIABLE AUTO_ERR)
if(AUTO_RC EQUAL 0 OR NOT AUTO_ERR MATCHES "turning-point")
  message(FATAL_ERROR "auto turning-point must fail without an artifact: ${AUTO_ERR}\n${AUTO_OUT}")
endif()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}"
    --build-dir "${BUILD_DIR}"
    --phase mixed-30-minute
    --duration-seconds 1
    --facts-per-txn auto-turning-point
    --writers 8
    --readers 1
    --degrees 1
    --selectivities 1
    --output "${OUTPUT}/mixed"
  RESULT_VARIABLE MIXED_RC OUTPUT_VARIABLE MIXED_OUT ERROR_VARIABLE MIXED_ERR)
if(MIXED_RC EQUAL 0 OR NOT MIXED_ERR MATCHES "turning-point")
  message(FATAL_ERROR "mixed auto turning-point must fail without an artifact: ${MIXED_ERR}\n${MIXED_OUT}")
endif()

file(MAKE_DIRECTORY "${OUTPUT}/turning-point-input")
file(WRITE "${OUTPUT}/turning-point-input/turning-point.json"
  "{ \"facts_per_txn\" : 64, \"source_summary\" : \"peak-candidate\" }\n")
execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}"
    --phase mixed-30-minute --duration-seconds 1 --facts-per-txn auto-turning-point
    --writers 1 --readers 1 --degrees 1 --selectivities 1
    --input "${OUTPUT}/turning-point-input" --output "${OUTPUT}/mixed-valid"
  RESULT_VARIABLE MIXED_VALID_RC OUTPUT_VARIABLE MIXED_VALID_OUT ERROR_VARIABLE MIXED_VALID_ERR)
if(NOT MIXED_VALID_RC MATCHES "^[01]$")
  message(FATAL_ERROR "mixed auto turning-point rejected a valid artifact: ${MIXED_VALID_ERR}\n${MIXED_VALID_OUT}")
endif()
file(READ "${OUTPUT}/mixed-valid/commands.manifest" MIXED_VALID_MANIFEST)
if(NOT MIXED_VALID_MANIFEST MATCHES "--facts-per-txn=64")
  message(FATAL_ERROR "mixed campaign did not consume the selected turning point: ${MIXED_VALID_MANIFEST}")
endif()

foreach(MALFORMED_TURNING_POINT IN ITEMS
    "{\"facts_per_txn\":-64}"
    "{\"facts_per_txn\":64} trailing"
    "{\"facts_per_txn\":64,\"facts_per_txn\":128}"
    "{\"facts_per_txn\":64,}")
  string(RANDOM LENGTH 4 ALPHABET 0123456789 TURNING_POINT_TOKEN)
  set(TURNING_POINT_DIR "${OUTPUT}/turning-point-invalid-${TURNING_POINT_TOKEN}")
  file(MAKE_DIRECTORY "${TURNING_POINT_DIR}")
  file(WRITE "${TURNING_POINT_DIR}/turning-point.json" "${MALFORMED_TURNING_POINT}\n")
  execute_process(
    COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}"
      --phase mixed-30-minute --duration-seconds 1 --facts-per-txn auto-turning-point
      --input "${TURNING_POINT_DIR}" --output "${TURNING_POINT_DIR}/run"
    RESULT_VARIABLE TURNING_POINT_RC ERROR_VARIABLE TURNING_POINT_ERR)
  if(TURNING_POINT_RC EQUAL 0 OR NOT TURNING_POINT_ERR MATCHES "invalid turning-point")
    message(FATAL_ERROR "malformed turning-point artifact was accepted: ${MALFORMED_TURNING_POINT} (${TURNING_POINT_RC}) ${TURNING_POINT_ERR}")
  endif()
endforeach()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}" --phase read-cold
    --duration-seconds 1 --readers 1 --degrees 1 --selectivities 1
    --projection-states canonical --output "${OUTPUT}/property"
  RESULT_VARIABLE PROPERTY_RC OUTPUT_VARIABLE PROPERTY_OUT ERROR_VARIABLE PROPERTY_ERR)
if(NOT PROPERTY_RC EQUAL 0)
  message(FATAL_ERROR "property-filter campaign failed: ${PROPERTY_ERR}\n${PROPERTY_OUT}")
endif()
file(READ "${OUTPUT}/property/commands.manifest" PROPERTY_MANIFEST)
if(NOT PROPERTY_MANIFEST MATCHES "--operation=property-filter")
  message(FATAL_ERROR "property-filter is missing from read matrix: ${PROPERTY_MANIFEST}")
endif()
file(READ "${OUTPUT}/property/summary.csv" PROPERTY_SUMMARY)
string(REGEX MATCH "read-cold,cold-property-filter-[^,]*,0,true," PROPERTY_SUCCESS "${PROPERTY_SUMMARY}")
if(NOT PROPERTY_SUCCESS)
  message(FATAL_ERROR "property-filter did not produce a successful hard-gated result: ${PROPERTY_SUMMARY}")
endif()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}" --phase write-idle-five-repeats
    --duration-seconds 1 --facts-per-txn "," --output "${OUTPUT}/empty"
  RESULT_VARIABLE EMPTY_RC ERROR_VARIABLE EMPTY_ERR)
if(NOT EMPTY_RC EQUAL 2 OR NOT EMPTY_ERR MATCHES "empty")
  message(FATAL_ERROR "empty CSV component was accepted: ${EMPTY_RC} ${EMPTY_ERR}")
endif()

foreach(MALFORMED_CSV IN ITEMS "1," ",1" "1,,2")
  string(RANDOM LENGTH 4 ALPHABET 0123456789 MALFORMED_TOKEN)
  execute_process(
    COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}" --phase write-idle-five-repeats
      --duration-seconds 1 --facts-per-txn "${MALFORMED_CSV}" --output "${OUTPUT}/malformed-${MALFORMED_TOKEN}"
    RESULT_VARIABLE MALFORMED_RC ERROR_VARIABLE MALFORMED_ERR)
  if(NOT MALFORMED_RC EQUAL 2 OR NOT MALFORMED_ERR MATCHES "empty")
    message(FATAL_ERROR "malformed CSV component was accepted (${MALFORMED_CSV}): ${MALFORMED_RC} ${MALFORMED_ERR}")
  endif()
endforeach()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}" --output
  RESULT_VARIABLE MISSING_RC ERROR_VARIABLE MISSING_ERR)
if(NOT MISSING_RC EQUAL 2 OR NOT MISSING_ERR MATCHES "requires a value")
  message(FATAL_ERROR "missing option value was not rejected with exit 2: ${MISSING_RC} ${MISSING_ERR}")
endif()

file(MAKE_DIRECTORY "${OUTPUT}/input")
file(WRITE "${OUTPUT}/input/run.csv" "header\nrow\n")
foreach(UNSUPPORTED_PHASE IN ITEMS reopen-verification space-audit)
  execute_process(
    COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}" --phase "${UNSUPPORTED_PHASE}"
      --duration-seconds 1 --input "${OUTPUT}/input" --output "${OUTPUT}/${UNSUPPORTED_PHASE}"
    RESULT_VARIABLE UNSUPPORTED_RC OUTPUT_VARIABLE UNSUPPORTED_OUT ERROR_VARIABLE UNSUPPORTED_ERR)
  file(READ "${OUTPUT}/${UNSUPPORTED_PHASE}/summary.csv" UNSUPPORTED_SUMMARY)
  if(UNSUPPORTED_RC EQUAL 0 OR NOT UNSUPPORTED_SUMMARY MATCHES "${UNSUPPORTED_PHASE},input-artifacts,1,false,artifact audit failed,")
    message(FATAL_ERROR "${UNSUPPORTED_PHASE} did not fail explicitly for invalid artifact input: ${UNSUPPORTED_RC} ${UNSUPPORTED_ERR}\n${UNSUPPORTED_OUT}\n${UNSUPPORTED_SUMMARY}")
  endif()
endforeach()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}"
    --phase write-active-projection-five-repeats --duration-seconds 1
    --facts-per-txn 1 --writers 1 --output "${OUTPUT}/active-missing-root/active-missing-baseline"
  RESULT_VARIABLE ACTIVE_MISSING_RC)
file(READ "${OUTPUT}/active-missing-root/active-missing-baseline/summary.jsonl" ACTIVE_MISSING_SUMMARY)
if(ACTIVE_MISSING_RC EQUAL 0 OR NOT ACTIVE_MISSING_SUMMARY MATCHES "active_projection_overhead.*missing_baseline")
  message(FATAL_ERROR "active phase accepted a missing baseline: ${ACTIVE_MISSING_RC}\n${ACTIVE_MISSING_SUMMARY}")
endif()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}"
    --phase write-idle-five-repeats --duration-seconds 1
    --facts-per-txn 1 --writers 1 --output "${OUTPUT}/active-valid-baseline-source"
  RESULT_VARIABLE ACTIVE_BASELINE_RC)
if(NOT ACTIVE_BASELINE_RC MATCHES "^[01]$" OR NOT EXISTS "${OUTPUT}/write-idle-overhead.csv")
  message(FATAL_ERROR "active baseline source campaign did not produce a usable artifact: ${ACTIVE_BASELINE_RC}")
endif()
execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}"
    --phase write-active-projection-five-repeats --duration-seconds 1
    --facts-per-txn 1 --writers 1 --input "${OUTPUT}/active-valid-baseline-source"
    --output "${OUTPUT}/active-valid-baseline"
  RESULT_VARIABLE ACTIVE_VALID_RC)
file(READ "${OUTPUT}/active-valid-baseline/summary.jsonl" ACTIVE_VALID_SUMMARY)
if(NOT ACTIVE_VALID_RC MATCHES "^[01]$")
  message(FATAL_ERROR "active phase did not complete its actual gate: ${ACTIVE_VALID_RC}\n${ACTIVE_VALID_SUMMARY}")
endif()
if(ACTIVE_VALID_SUMMARY MATCHES "missing_baseline" OR ACTIVE_VALID_SUMMARY MATCHES "invalid_avg_")
  message(FATAL_ERROR "valid baseline was rejected as missing or malformed: ${ACTIVE_VALID_SUMMARY}")
endif()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}"
    --build-dir "${BUILD_DIR}" --phase release-calibration
    --duration-seconds 1 --unknown-option value --output "${OUTPUT}/unknown"
  RESULT_VARIABLE UNKNOWN_RC)
if(UNKNOWN_RC EQUAL 0)
  message(FATAL_ERROR "unknown campaign option was accepted")
endif()
