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
if(NOT AUTO_RC EQUAL 0 OR NOT EXISTS "${OUTPUT}/auto/commands.manifest")
  message(FATAL_ERROR "auto turning-point option contract failed: ${AUTO_ERR}\n${AUTO_OUT}")
endif()
file(READ "${OUTPUT}/auto/commands.manifest" AUTO_MANIFEST)
if(NOT AUTO_MANIFEST MATCHES "--facts-per-txn=64")
  message(FATAL_ERROR "auto turning-point was not resolved in the command: ${AUTO_MANIFEST}")
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
if(NOT MIXED_RC EQUAL 0)
  message(FATAL_ERROR "mixed total-budget contract failed: ${MIXED_ERR}\n${MIXED_OUT}")
endif()
file(READ "${OUTPUT}/mixed/commands.manifest" MIXED_MANIFEST)
string(REGEX MATCHALL "--duration-seconds=1" MIXED_DURATIONS "${MIXED_MANIFEST}")
list(LENGTH MIXED_DURATIONS MIXED_CASE_COUNT)
if(NOT MIXED_CASE_COUNT EQUAL 10)
  message(FATAL_ERROR "mixed campaign did not split duration across ten operations: ${MIXED_MANIFEST}")
endif()

execute_process(
  COMMAND bash "${CEDAR_CAMPAIGN}"
    --build-dir "${BUILD_DIR}" --phase release-calibration
    --duration-seconds 1 --unknown-option value --output "${OUTPUT}/unknown"
  RESULT_VARIABLE UNKNOWN_RC)
if(UNKNOWN_RC EQUAL 0)
  message(FATAL_ERROR "unknown campaign option was accepted")
endif()
