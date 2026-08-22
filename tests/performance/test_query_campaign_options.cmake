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

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}" --phase read-cold
    --duration-seconds 1 --readers 1 --degrees 1 --selectivities 1
    --projection-states canonical --output "${OUTPUT}/property")
file(READ "${OUTPUT}/property/commands.manifest" PROPERTY_MANIFEST)
if(NOT PROPERTY_MANIFEST MATCHES "--operation=property-filter")
  message(FATAL_ERROR "property-filter is missing from read matrix: ${PROPERTY_MANIFEST}")
endif()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}" --phase write-idle-five-repeats
    --duration-seconds 1 --facts-per-txn "," --output "${OUTPUT}/empty"
  RESULT_VARIABLE EMPTY_RC ERROR_VARIABLE EMPTY_ERR)
if(NOT EMPTY_RC EQUAL 2 OR NOT EMPTY_ERR MATCHES "empty")
  message(FATAL_ERROR "empty CSV component was accepted: ${EMPTY_RC} ${EMPTY_ERR}")
endif()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}" --output
  RESULT_VARIABLE MISSING_RC ERROR_VARIABLE MISSING_ERR)
if(NOT MISSING_RC EQUAL 2 OR NOT MISSING_ERR MATCHES "requires a value")
  message(FATAL_ERROR "missing option value was not rejected with exit 2: ${MISSING_RC} ${MISSING_ERR}")
endif()

file(MAKE_DIRECTORY "${OUTPUT}/input")
file(WRITE "${OUTPUT}/input/run.csv" "header\nrow\n")
execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}" --build-dir "${BUILD_DIR}" --phase reopen-verification
    --duration-seconds 1 --input "${OUTPUT}/input" --output "${OUTPUT}/reopen"
  RESULT_VARIABLE REOPEN_RC ERROR_VARIABLE REOPEN_ERR)
if(REOPEN_RC EQUAL 0 OR NOT REOPEN_ERR MATCHES "not supported")
  message(FATAL_ERROR "reopen phase did not fail explicitly for unsupported cross-artifact verification: ${REOPEN_RC} ${REOPEN_ERR}")
endif()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}"
    --build-dir "${BUILD_DIR}" --phase release-calibration
    --duration-seconds 1 --unknown-option value --output "${OUTPUT}/unknown"
  RESULT_VARIABLE UNKNOWN_RC)
if(UNKNOWN_RC EQUAL 0)
  message(FATAL_ERROR "unknown campaign option was accepted")
endif()
