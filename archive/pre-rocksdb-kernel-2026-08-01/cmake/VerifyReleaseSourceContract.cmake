cmake_minimum_required(VERSION 3.14)

if(NOT DEFINED SOURCE_ROOT OR SOURCE_ROOT STREQUAL "" OR
   NOT DEFINED OUTPUT_ROOT OR OUTPUT_ROOT STREQUAL "")
  message(FATAL_ERROR "SOURCE_ROOT and OUTPUT_ROOT are required")
endif()

get_filename_component(SOURCE_ROOT_REAL "${SOURCE_ROOT}" REALPATH)
get_filename_component(OUTPUT_ROOT_ABSOLUTE "${OUTPUT_ROOT}" ABSOLUTE)
if(NOT IS_DIRECTORY "${SOURCE_ROOT_REAL}")
  message(FATAL_ERROR "SOURCE_ROOT is not a directory: ${SOURCE_ROOT}")
endif()
get_filename_component(OUTPUT_PARENT "${OUTPUT_ROOT_ABSOLUTE}" DIRECTORY)
get_filename_component(OUTPUT_NAME "${OUTPUT_ROOT_ABSOLUTE}" NAME)
file(MAKE_DIRECTORY "${OUTPUT_PARENT}")
file(GLOB STRANDED_BACKUPS LIST_DIRECTORIES true
     "${OUTPUT_PARENT}/.${OUTPUT_NAME}.backup-*")
list(SORT STRANDED_BACKUPS)
if(NOT EXISTS "${OUTPUT_ROOT_ABSOLUTE}" AND STRANDED_BACKUPS)
  list(LENGTH STRANDED_BACKUPS STRANDED_BACKUP_COUNT)
  if(NOT STRANDED_BACKUP_COUNT EQUAL 1)
    message(FATAL_ERROR "multiple stranded source-contract backups require review")
  endif()
  list(GET STRANDED_BACKUPS 0 STRANDED_BACKUP)
  execute_process(COMMAND "${CMAKE_COMMAND}" -E rename
                  "${STRANDED_BACKUP}" "${OUTPUT_ROOT_ABSOLUTE}"
                  RESULT_VARIABLE RECOVERY_RESULT)
  if(NOT RECOVERY_RESULT EQUAL 0)
    message(FATAL_ERROR "failed to recover stranded source-contract output")
  endif()
endif()

set(AUTHORITATIVE_SPECIFICATIONS
    docs/superpowers/specs/2026-08-01-cedar-rocksdb-kernel-design.md)

file(GLOB_RECURSE SOURCE_SCOPE_FILES
     LIST_DIRECTORIES false RELATIVE "${SOURCE_ROOT_REAL}"
     "${SOURCE_ROOT_REAL}/include/*"
     "${SOURCE_ROOT_REAL}/src/*"
     "${SOURCE_ROOT_REAL}/benchmarks/*")
set(SCANNED_SOURCE_FILES)
foreach(RELATIVE_PATH IN LISTS SOURCE_SCOPE_FILES)
  get_filename_component(SOURCE_NAME "${RELATIVE_PATH}" NAME)
  if(SOURCE_NAME STREQUAL ".DS_Store")
    continue()
  endif()
  if(NOT RELATIVE_PATH MATCHES "\\.(c|cc|cpp|cxx|h|hh|hpp)$")
    message(FATAL_ERROR
            "unsupported production source extension: ${RELATIVE_PATH}")
  endif()
  list(APPEND SCANNED_SOURCE_FILES "${RELATIVE_PATH}")
endforeach()
list(SORT SCANNED_SOURCE_FILES)

set(CLAIM_FILES README.md ${AUTHORITATIVE_SPECIFICATIONS})
foreach(REQUIRED_FILE IN LISTS CLAIM_FILES)
  if(NOT EXISTS "${SOURCE_ROOT_REAL}/${REQUIRED_FILE}")
    message(FATAL_ERROR "required source-contract input is missing: ${REQUIRED_FILE}")
  endif()
endforeach()
set(SCANNED_INPUT_FILES ${SCANNED_SOURCE_FILES} ${CLAIM_FILES})
list(REMOVE_DUPLICATES SCANNED_INPUT_FILES)
list(SORT SCANNED_INPUT_FILES)
foreach(RELATIVE_PATH IN LISTS SCANNED_INPUT_FILES)
  if(RELATIVE_PATH MATCHES "(^|/)\\.\\.(/|$)" OR
     IS_DIRECTORY "${SOURCE_ROOT_REAL}/${RELATIVE_PATH}")
    message(FATAL_ERROR "source-contract input escapes source root: ${RELATIVE_PATH}")
  endif()
  get_filename_component(INPUT_REAL
                         "${SOURCE_ROOT_REAL}/${RELATIVE_PATH}" REALPATH)
  string(FIND "${INPUT_REAL}/" "${SOURCE_ROOT_REAL}/" INPUT_ROOT_PREFIX)
  if(NOT INPUT_ROOT_PREFIX EQUAL 0)
    message(FATAL_ERROR "source-contract input escapes source root: ${RELATIVE_PATH}")
  endif()
endforeach()

set(EXPECTED_ALL_MUTATION_FILES
    benchmarks/cedar_bench_pair.cc
    benchmarks/cedar_production_campaign.cc
    src/benchmark/artifact_reader.cc
    src/benchmark/artifact_writer.cc
    src/benchmark/cedar_tg.cc
    src/benchmark/fault_campaign.cc
    src/benchmark/regression_gate.cc
    src/benchmark/run_manifest.cc
    src/blob/blob_store.cc
    src/columnar/sst.cc
    src/fact/fact_store.cc
    src/index/index_sidecar.cc
    src/statistics/stats_snapshot.cc
    src/storage/sst_compaction.cc
    src/storage/version_set.cc
    src/tcypher/runtime/query_spill.cc
    src/tcypher/storage/temporal_scan.cc
    src/transaction/commit_timeline.cc
    src/transaction/database_format.cc
    src/transaction/decision_log.cc
    src/transaction/logical_id_allocator.cc
    src/transaction/transaction_coordinator.cc)
set(EXPECTED_DATABASE_DURABLE_WRITERS
    src/blob/blob_store.cc
    src/columnar/sst.cc
    src/fact/fact_store.cc
    src/index/index_sidecar.cc
    src/statistics/stats_snapshot.cc
    src/storage/sst_compaction.cc
    src/storage/version_set.cc
    src/transaction/commit_timeline.cc
    src/transaction/database_format.cc
    src/transaction/decision_log.cc
    src/transaction/logical_id_allocator.cc
    src/transaction/transaction_coordinator.cc)
set(EXPECTED_ALL_DELETE_FILES
    benchmarks/cedar_bench_pair.cc
    benchmarks/cedar_production_campaign.cc
    src/benchmark/artifact_reader.cc
    src/benchmark/artifact_writer.cc
    src/benchmark/cedar_tg.cc
    src/benchmark/regression_gate.cc
    src/benchmark/run_manifest.cc
    src/blob/blob_store.cc
    src/columnar/sst.cc
    src/index/index_sidecar.cc
    src/statistics/stats_snapshot.cc
    src/storage/sst_compaction.cc
    src/tcypher/runtime/query_spill.cc
    src/tcypher/storage/temporal_scan.cc
    src/transaction/database_format.cc
    src/transaction/decision_log.cc
    src/transaction/transaction_coordinator.cc)
set(EXPECTED_MANIFEST_PUBLICATION_FILES
    src/index/index_catalog.cc
    src/storage/sst_compaction.cc
    src/storage/sst_flush.cc
    src/storage/version_set.cc
    src/transaction/transaction_coordinator.cc)
set(EXPECTED_PERSISTENT_DELETE_FILES
    src/blob/blob_store.cc
    src/columnar/sst.cc
    src/statistics/stats_snapshot.cc
    src/storage/sst_compaction.cc
    src/transaction/decision_log.cc
    src/transaction/transaction_coordinator.cc)
set(EXPECTED_RETAINED_STATS_TYPES
    BenchmarkStorageStats
    BlobStoreStats
    CacheStats
    EventRingStats
    IndexHealthStats
    PageCompressionStats
    PhysicalHashJoinPlanningStats
    QueryOperatorResourceStats
    SstCursorStats
    SstReadStats
    SstStreamingWriteStats
    StorageRuntimeStats
    TcypherExecutionStats
    TelemetryAggregatorStats
    TemporalScanCursorStats
    WorkExecutionStats)

foreach(EXPECTED_LIST
        EXPECTED_ALL_MUTATION_FILES EXPECTED_ALL_DELETE_FILES
        EXPECTED_DATABASE_DURABLE_WRITERS
        EXPECTED_MANIFEST_PUBLICATION_FILES EXPECTED_PERSISTENT_DELETE_FILES
        EXPECTED_RETAINED_STATS_TYPES)
  list(SORT ${EXPECTED_LIST})
endforeach()

set(ACTUAL_ALL_MUTATION_FILES)
set(ACTUAL_ALL_DELETE_FILES)
set(ACTUAL_MANIFEST_PUBLICATION_FILES)
set(ACTUAL_RETAINED_STATS_TYPES)
set(FORBIDDEN_DIRECT_DIAGNOSTICS)
set(FORBIDDEN_DUPLICATE_METRICS)

foreach(RELATIVE_PATH IN LISTS SCANNED_SOURCE_FILES)
  file(READ "${SOURCE_ROOT_REAL}/${RELATIVE_PATH}" CONTENT)

  string(REGEX MATCH
     "(::(open|write|pwrite|fsync|fdatasync|rename|unlink)|(^|[^A-Za-z0-9_.>:])(open|write|pwrite|fsync|fdatasync|rename|unlink))[ \t\r\n]*\\(|std::(ofstream|fstream)|std::filesystem::(remove|remove_all|rename|copy_file|create_directories)[ \t\r\n]*\\("
     MUTATION_MATCH "${CONTENT}")
  if(NOT MUTATION_MATCH STREQUAL "")
    if(DEFINED SOURCE_CONTRACT_DEBUG AND SOURCE_CONTRACT_DEBUG)
      message(STATUS "mutation owner ${RELATIVE_PATH}: ${MUTATION_MATCH}")
    endif()
    list(APPEND ACTUAL_ALL_MUTATION_FILES "${RELATIVE_PATH}")
  endif()
  if(CONTENT MATCHES
     "(::unlink|(^|[^A-Za-z0-9_.>:])unlink)[ \t\r\n]*\\(|std::filesystem::(remove|remove_all)[ \t\r\n]*\\(")
    list(APPEND ACTUAL_ALL_DELETE_FILES "${RELATIVE_PATH}")
  endif()
  if(RELATIVE_PATH MATCHES "^(src|benchmarks)/" AND
     CONTENT MATCHES "ApplyEdit[ \t\r\n]*\\(")
    list(APPEND ACTUAL_MANIFEST_PUBLICATION_FILES "${RELATIVE_PATH}")
  endif()

  string(REGEX MATCHALL
         "(struct|class)[ \t\r\n]+[A-Za-z_][A-Za-z0-9_]*Stats([^A-Za-z0-9_]|$)"
         STATS_DECLARATIONS "${CONTENT}")
  foreach(DECLARATION IN LISTS STATS_DECLARATIONS)
    string(REGEX REPLACE
           "^(struct|class)[ \t\r\n]+([A-Za-z_][A-Za-z0-9_]*Stats)([^A-Za-z0-9_]|$)"
           "\\2" STATS_TYPE "${DECLARATION}")
    if(NOT STATS_TYPE STREQUAL "")
      list(APPEND ACTUAL_RETAINED_STATS_TYPES "${STATS_TYPE}")
    endif()
  endforeach()

  if(CONTENT MATCHES "(^|[^A-Za-z0-9_])(printf|fprintf)[ \t\r\n]*\\(")
    list(APPEND FORBIDDEN_DIRECT_DIAGNOSTICS "${RELATIVE_PATH}: direct printf/fprintf")
  endif()
  if(CONTENT MATCHES
     "(average_latency_ms|avg_latency_ms|cache_hit_rate|cache_hit_ratio)")
    list(APPEND FORBIDDEN_DUPLICATE_METRICS "${RELATIVE_PATH}: ad-hoc latency/cache metric")
  endif()
endforeach()

list(REMOVE_DUPLICATES ACTUAL_ALL_MUTATION_FILES)
list(REMOVE_DUPLICATES ACTUAL_ALL_DELETE_FILES)
list(REMOVE_DUPLICATES ACTUAL_MANIFEST_PUBLICATION_FILES)
list(REMOVE_DUPLICATES ACTUAL_RETAINED_STATS_TYPES)
list(SORT ACTUAL_ALL_MUTATION_FILES)
list(SORT ACTUAL_ALL_DELETE_FILES)
list(SORT ACTUAL_MANIFEST_PUBLICATION_FILES)
list(SORT ACTUAL_RETAINED_STATS_TYPES)
list(SORT FORBIDDEN_DIRECT_DIAGNOSTICS)
list(SORT FORBIDDEN_DUPLICATE_METRICS)

function(REQUIRE_EXACT_INVENTORY LABEL EXPECTED_VARIABLE ACTUAL_VARIABLE)
  set(EXPECTED "${${EXPECTED_VARIABLE}}")
  set(ACTUAL "${${ACTUAL_VARIABLE}}")
  if(NOT EXPECTED STREQUAL ACTUAL)
    message(FATAL_ERROR
      "${LABEL} inventory drifted\nexpected: ${EXPECTED}\nactual: ${ACTUAL}")
  endif()
endfunction()

REQUIRE_EXACT_INVENTORY("filesystem mutation owner"
                        EXPECTED_ALL_MUTATION_FILES ACTUAL_ALL_MUTATION_FILES)
REQUIRE_EXACT_INVENTORY("direct delete owner"
                        EXPECTED_ALL_DELETE_FILES ACTUAL_ALL_DELETE_FILES)
REQUIRE_EXACT_INVENTORY("Manifest publication owner"
                        EXPECTED_MANIFEST_PUBLICATION_FILES
                        ACTUAL_MANIFEST_PUBLICATION_FILES)
REQUIRE_EXACT_INVENTORY("retained Stats type"
                        EXPECTED_RETAINED_STATS_TYPES ACTUAL_RETAINED_STATS_TYPES)

# Persistent deletes intentionally exclude temporary-file cleanup and query
# spill cleanup. The exact all-mutation inventory above still fails if any such
# owner appears or disappears without review.
set(ACTUAL_PERSISTENT_DELETE_FILES ${EXPECTED_PERSISTENT_DELETE_FILES})
foreach(DELETE_FILE IN LISTS ACTUAL_PERSISTENT_DELETE_FILES)
  list(FIND ACTUAL_ALL_DELETE_FILES "${DELETE_FILE}" DELETE_OWNER_INDEX)
  if(DELETE_OWNER_INDEX EQUAL -1)
    message(FATAL_ERROR "persistent delete owner is not a mutation owner: ${DELETE_FILE}")
  endif()
endforeach()

set(FORBIDDEN_UNBOUND_PERFORMANCE_CLAIMS)
foreach(RELATIVE_PATH IN LISTS CLAIM_FILES)
  file(READ "${SOURCE_ROOT_REAL}/${RELATIVE_PATH}" CONTENT)
  string(REPLACE ";" "\\;" ESCAPED_CONTENT "${CONTENT}")
  string(REPLACE "\n" ";" CONTENT_LINES "${ESCAPED_CONTENT}")
  set(LINE_NUMBER 0)
  foreach(CONTENT_LINE IN LISTS CONTENT_LINES)
    math(EXPR LINE_NUMBER "${LINE_NUMBER} + 1")
    string(TOLOWER "${CONTENT_LINE}" LOWER_CONTENT_LINE)
    if(LOWER_CONTENT_LINE MATCHES
       "[0-9]+(\\.[0-9]+)?[ \t]*(ops/s|requests/s|rows/s|events/s|qps|tps|mb/s|gb/s|ms|us|ns)")
      set(HAS_ARTIFACT_PATH false)
      if(LOWER_CONTENT_LINE MATCHES "results/[a-z0-9._/-]+")
        set(HAS_ARTIFACT_PATH true)
      endif()
      set(HAS_ARTIFACT_SHA false)
      string(REGEX MATCHALL "[0-9a-f]+" HEX_RUNS "${LOWER_CONTENT_LINE}")
      foreach(HEX_RUN IN LISTS HEX_RUNS)
        string(LENGTH "${HEX_RUN}" HEX_RUN_LENGTH)
        if(HEX_RUN_LENGTH EQUAL 64)
          set(HAS_ARTIFACT_SHA true)
        endif()
      endforeach()
      if(NOT HAS_ARTIFACT_PATH OR NOT HAS_ARTIFACT_SHA)
        list(APPEND FORBIDDEN_UNBOUND_PERFORMANCE_CLAIMS
             "${RELATIVE_PATH}:${LINE_NUMBER}: ${CONTENT_LINE}")
      endif()
    endif()
  endforeach()
endforeach()
list(SORT FORBIDDEN_UNBOUND_PERFORMANCE_CLAIMS)

foreach(FORBIDDEN_LIST
        FORBIDDEN_DIRECT_DIAGNOSTICS FORBIDDEN_DUPLICATE_METRICS
        FORBIDDEN_UNBOUND_PERFORMANCE_CLAIMS)
  if(${FORBIDDEN_LIST})
    message(FATAL_ERROR "${FORBIDDEN_LIST} is nonempty: ${${FORBIDDEN_LIST}}")
  endif()
endforeach()

string(RANDOM LENGTH 16 ALPHABET 0123456789abcdef STAGING_SUFFIX)
set(STAGING_ROOT "${OUTPUT_PARENT}/.${OUTPUT_NAME}.staging-${STAGING_SUFFIX}")
set(BACKUP_ROOT "${OUTPUT_PARENT}/.${OUTPUT_NAME}.backup-${STAGING_SUFFIX}")
file(MAKE_DIRECTORY "${STAGING_ROOT}")

function(WRITE_LIST_FILE FILE_NAME LIST_VARIABLE)
  set(TEXT "")
  foreach(VALUE IN LISTS ${LIST_VARIABLE})
    string(APPEND TEXT "${VALUE}\n")
  endforeach()
  file(WRITE "${STAGING_ROOT}/${FILE_NAME}" "${TEXT}")
endfunction()

WRITE_LIST_FILE(durable-writer-files.txt EXPECTED_DATABASE_DURABLE_WRITERS)
WRITE_LIST_FILE(manifest-publication-files.txt EXPECTED_MANIFEST_PUBLICATION_FILES)
WRITE_LIST_FILE(persistent-delete-files.txt ACTUAL_PERSISTENT_DELETE_FILES)
WRITE_LIST_FILE(retained-stats-types.txt ACTUAL_RETAINED_STATS_TYPES)
WRITE_LIST_FILE(forbidden-direct-diagnostics.txt FORBIDDEN_DIRECT_DIAGNOSTICS)
WRITE_LIST_FILE(forbidden-duplicate-metrics.txt FORBIDDEN_DUPLICATE_METRICS)
WRITE_LIST_FILE(forbidden-unbound-performance-claims.txt
                FORBIDDEN_UNBOUND_PERFORMANCE_CLAIMS)
set(SOURCE_INPUT_TEXT "")
foreach(RELATIVE_PATH IN LISTS SCANNED_INPUT_FILES)
  file(SHA256 "${SOURCE_ROOT_REAL}/${RELATIVE_PATH}" INPUT_SHA256)
  string(APPEND SOURCE_INPUT_TEXT "${INPUT_SHA256}  ${RELATIVE_PATH}\n")
endforeach()
file(WRITE "${STAGING_ROOT}/source-input-files.txt" "${SOURCE_INPUT_TEXT}")

set(RAW_OUTPUT_FILES
    durable-writer-files.txt
    forbidden-direct-diagnostics.txt
    forbidden-duplicate-metrics.txt
    forbidden-unbound-performance-claims.txt
    manifest-publication-files.txt
    persistent-delete-files.txt
    retained-stats-types.txt
    source-input-files.txt)
set(JSON "{\n  \"schema_version\": 1,\n  \"files\": {\n")
list(LENGTH RAW_OUTPUT_FILES RAW_OUTPUT_COUNT)
set(RAW_OUTPUT_INDEX 0)
foreach(FILE_NAME IN LISTS RAW_OUTPUT_FILES)
  math(EXPR RAW_OUTPUT_INDEX "${RAW_OUTPUT_INDEX} + 1")
  file(SHA256 "${STAGING_ROOT}/${FILE_NAME}" FILE_SHA256)
  file(STRINGS "${STAGING_ROOT}/${FILE_NAME}" FILE_LINES)
  list(LENGTH FILE_LINES FILE_COUNT)
  string(APPEND JSON
         "    \"${FILE_NAME}\": {\"count\": ${FILE_COUNT}, \"sha256\": \"${FILE_SHA256}\"}")
  if(NOT RAW_OUTPUT_INDEX EQUAL RAW_OUTPUT_COUNT)
    string(APPEND JSON ",")
  endif()
  string(APPEND JSON "\n")
endforeach()
string(APPEND JSON "  }\n}\n")
file(WRITE "${STAGING_ROOT}/source-contract.json" "${JSON}")

# Publish only after every check and every hash succeeds. If publication itself
# fails, restore the previous accepted directory.
if(EXISTS "${OUTPUT_ROOT_ABSOLUTE}")
  execute_process(COMMAND "${CMAKE_COMMAND}" -E rename
                  "${OUTPUT_ROOT_ABSOLUTE}" "${BACKUP_ROOT}"
                  RESULT_VARIABLE BACKUP_RESULT)
  if(NOT BACKUP_RESULT EQUAL 0)
    file(REMOVE_RECURSE "${STAGING_ROOT}")
    message(FATAL_ERROR "failed to preserve previous source-contract output")
  endif()
endif()
if(DEFINED SOURCE_CONTRACT_TEST_FAIL_AFTER_BACKUP AND
   SOURCE_CONTRACT_TEST_FAIL_AFTER_BACKUP)
  execute_process(COMMAND "${CMAKE_COMMAND}" -E rename
                  "${BACKUP_ROOT}" "${OUTPUT_ROOT_ABSOLUTE}"
                  RESULT_VARIABLE INJECTED_RESTORE_RESULT)
  file(REMOVE_RECURSE "${STAGING_ROOT}")
  if(NOT INJECTED_RESTORE_RESULT EQUAL 0)
    message(FATAL_ERROR
            "injected publication failure also failed to restore accepted output")
  endif()
  message(FATAL_ERROR "injected publication failure")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -E rename
                "${STAGING_ROOT}" "${OUTPUT_ROOT_ABSOLUTE}"
                RESULT_VARIABLE PUBLISH_RESULT)
if(NOT PUBLISH_RESULT EQUAL 0)
  set(RESTORE_RESULT 0)
  if(EXISTS "${BACKUP_ROOT}")
    execute_process(COMMAND "${CMAKE_COMMAND}" -E rename
                    "${BACKUP_ROOT}" "${OUTPUT_ROOT_ABSOLUTE}"
                    RESULT_VARIABLE RESTORE_RESULT)
  endif()
  file(REMOVE_RECURSE "${STAGING_ROOT}")
  if(NOT RESTORE_RESULT EQUAL 0)
    message(FATAL_ERROR
            "failed to publish and failed to restore source-contract output")
  endif()
  message(FATAL_ERROR "failed to publish source-contract output")
endif()
file(GLOB COMPLETED_BACKUPS LIST_DIRECTORIES true
     "${OUTPUT_PARENT}/.${OUTPUT_NAME}.backup-*")
foreach(COMPLETED_BACKUP IN LISTS COMPLETED_BACKUPS)
  file(REMOVE_RECURSE "${COMPLETED_BACKUP}")
endforeach()
