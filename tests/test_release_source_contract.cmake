cmake_minimum_required(VERSION 3.14)

if(NOT DEFINED SCANNER OR SCANNER STREQUAL "" OR
   NOT DEFINED TEST_ROOT OR TEST_ROOT STREQUAL "")
  message(FATAL_ERROR "SCANNER and TEST_ROOT are required")
endif()
if(NOT EXISTS "${SCANNER}")
  message(FATAL_ERROR "scanner is missing: ${SCANNER}")
endif()

set(FIXTURE_SOURCE "${TEST_ROOT}/source")
set(FIXTURE_OUTPUT "${TEST_ROOT}/accepted-output")

set(MUTATION_FILES
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
set(DELETE_FILES
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
set(PUBLICATION_FILES
    src/index/index_catalog.cc
    src/storage/sst_compaction.cc
    src/storage/sst_flush.cc
    src/storage/version_set.cc
    src/transaction/transaction_coordinator.cc)
set(CLAIM_FILES
    README.md
    docs/superpowers/specs/2026-07-27-cedar-atomic-commit-design.md
    docs/superpowers/specs/2026-08-01-cedar-rocksdb-kernel-design.md)

function(WRITE_FIXTURE_FILE RELATIVE_PATH CONTENT)
  get_filename_component(PARENT "${FIXTURE_SOURCE}/${RELATIVE_PATH}" DIRECTORY)
  file(MAKE_DIRECTORY "${PARENT}")
  file(WRITE "${FIXTURE_SOURCE}/${RELATIVE_PATH}" "${CONTENT}")
endfunction()

function(APPEND_FIXTURE_FILE RELATIVE_PATH CONTENT)
  get_filename_component(PARENT "${FIXTURE_SOURCE}/${RELATIVE_PATH}" DIRECTORY)
  file(MAKE_DIRECTORY "${PARENT}")
  file(APPEND "${FIXTURE_SOURCE}/${RELATIVE_PATH}" "${CONTENT}")
endfunction()

function(PREPARE_FIXTURE)
  file(REMOVE_RECURSE "${FIXTURE_SOURCE}")
  foreach(RELATIVE_PATH IN LISTS MUTATION_FILES)
    WRITE_FIXTURE_FILE("${RELATIVE_PATH}"
                       "void mutation_owner() { ::fsync(0); }\n")
  endforeach()
  foreach(RELATIVE_PATH IN LISTS DELETE_FILES)
    APPEND_FIXTURE_FILE("${RELATIVE_PATH}"
                        "void delete_owner() { ::unlink(\"temporary\"); }\n")
  endforeach()
  foreach(RELATIVE_PATH IN LISTS PUBLICATION_FILES)
    if(RELATIVE_PATH STREQUAL "src/storage/version_set.cc")
      APPEND_FIXTURE_FILE("${RELATIVE_PATH}"
                          "void VersionSet::ApplyEdit() {}\n")
    else()
      APPEND_FIXTURE_FILE("${RELATIVE_PATH}"
                          "void publish() { versions.ApplyEdit(); }\n")
    endif()
  endforeach()
  foreach(RELATIVE_PATH IN LISTS CLAIM_FILES)
    WRITE_FIXTURE_FILE("${RELATIVE_PATH}" "# source-contract fixture\n")
  endforeach()
  WRITE_FIXTURE_FILE(
      "include/cedar/fixture_stats.h"
      "struct BenchmarkStorageStats {};\nstruct BlobStoreStats {};\nstruct CacheStats {};\nstruct EventRingStats {};\nstruct IndexHealthStats {};\nstruct PageCompressionStats {};\nstruct PhysicalHashJoinPlanningStats {};\nstruct QueryOperatorResourceStats {};\nstruct SstCursorStats {};\nstruct SstReadStats {};\nstruct SstStreamingWriteStats {};\nstruct StorageRuntimeStats {};\nstruct TcypherExecutionStats {};\nstruct TelemetryAggregatorStats {};\nstruct TemporalScanCursorStats {};\nstruct WorkExecutionStats {};\n")
endfunction()

function(OUTPUT_DIGEST RESULT_VARIABLE)
  file(GLOB OUTPUT_FILES LIST_DIRECTORIES false
       RELATIVE "${FIXTURE_OUTPUT}" "${FIXTURE_OUTPUT}/*")
  list(SORT OUTPUT_FILES)
  set(MATERIAL "")
  foreach(RELATIVE_PATH IN LISTS OUTPUT_FILES)
    file(SHA256 "${FIXTURE_OUTPUT}/${RELATIVE_PATH}" FILE_HASH)
    string(APPEND MATERIAL "${RELATIVE_PATH}:${FILE_HASH}\n")
  endforeach()
  string(SHA256 DIGEST "${MATERIAL}")
  set(${RESULT_VARIABLE} "${DIGEST}" PARENT_SCOPE)
endfunction()

function(EXPECT_SCANNER_FAILURE LABEL EXPECTED_ERROR)
  OUTPUT_DIGEST(BEFORE_DIGEST)
  execute_process(
      COMMAND "${CMAKE_COMMAND}"
              -DSOURCE_ROOT=${FIXTURE_SOURCE}
              -DOUTPUT_ROOT=${FIXTURE_OUTPUT}
              -P ${SCANNER}
      RESULT_VARIABLE SCANNER_RESULT
      OUTPUT_VARIABLE SCANNER_STDOUT
      ERROR_VARIABLE SCANNER_STDERR)
  if(SCANNER_RESULT EQUAL 0)
    message(FATAL_ERROR "${LABEL}: scanner unexpectedly succeeded")
  endif()
  set(SCANNER_LOG "${SCANNER_STDOUT}\n${SCANNER_STDERR}")
  if(NOT SCANNER_LOG MATCHES "${EXPECTED_ERROR}")
    message(FATAL_ERROR
            "${LABEL}: wrong failure\nexpected: ${EXPECTED_ERROR}\n${SCANNER_LOG}")
  endif()
  OUTPUT_DIGEST(AFTER_DIGEST)
  if(NOT BEFORE_DIGEST STREQUAL AFTER_DIGEST)
    message(FATAL_ERROR "${LABEL}: failed scan replaced accepted output bytes")
  endif()
endfunction()

file(REMOVE_RECURSE "${TEST_ROOT}")
PREPARE_FIXTURE()
execute_process(
    COMMAND "${CMAKE_COMMAND}"
            -DSOURCE_ROOT=${FIXTURE_SOURCE}
            -DOUTPUT_ROOT=${FIXTURE_OUTPUT}
            -P ${SCANNER}
    RESULT_VARIABLE BASELINE_RESULT
    OUTPUT_VARIABLE BASELINE_STDOUT
    ERROR_VARIABLE BASELINE_STDERR)
if(NOT BASELINE_RESULT EQUAL 0)
  message(FATAL_ERROR
          "baseline fixture failed\n${BASELINE_STDOUT}\n${BASELINE_STDERR}")
endif()
if(NOT EXISTS "${FIXTURE_OUTPUT}/source-input-files.txt")
  message(FATAL_ERROR "baseline output does not bind scanned input files")
endif()
file(STRINGS "${FIXTURE_OUTPUT}/source-input-files.txt" BOUND_INPUT_LINES)
list(LENGTH BOUND_INPUT_LINES BOUND_INPUT_COUNT)
if(NOT BOUND_INPUT_COUNT EQUAL 28)
  message(FATAL_ERROR
          "baseline output binds ${BOUND_INPUT_COUNT} inputs instead of 28")
endif()

PREPARE_FIXTURE()
WRITE_FIXTURE_FILE("src/unapproved_writer.cc"
                   "void unapproved() { ::fsync(0); }\n")
EXPECT_SCANNER_FAILURE("unapproved durable writer"
                       "filesystem mutation owner inventory drifted")

PREPARE_FIXTURE()
WRITE_FIXTURE_FILE("src/unqualified_open_writer.cc"
                   "void unapproved() { open(\"new-file\", O_CREAT, 0600); }\n")
EXPECT_SCANNER_FAILURE("unqualified open writer"
                       "filesystem mutation owner inventory drifted")

PREPARE_FIXTURE()
WRITE_FIXTURE_FILE("src/unapproved_c_writer.c"
                   "void unapproved(void) { fsync(0); }\n")
EXPECT_SCANNER_FAILURE("unapproved C durable writer"
                       "filesystem mutation owner inventory drifted")

PREPARE_FIXTURE()
WRITE_FIXTURE_FILE("src/direct_diagnostic.cc"
                   "void diagnose() { fprintf(stderr, \"bad\"); }\n")
EXPECT_SCANNER_FAILURE("direct fprintf"
                       "FORBIDDEN_DIRECT_DIAGNOSTICS is nonempty")

PREPARE_FIXTURE()
WRITE_FIXTURE_FILE("src/duplicate_metric.cc"
                   "double average_latency_ms = 1.0;\n")
EXPECT_SCANNER_FAILURE("duplicate average latency metric"
                       "FORBIDDEN_DUPLICATE_METRICS is nonempty")

PREPARE_FIXTURE()
WRITE_FIXTURE_FILE("README.md" "Measured throughput: 1000 ops/s.\n")
EXPECT_SCANNER_FAILURE("unbound README throughput"
                       "FORBIDDEN_UNBOUND_PERFORMANCE_CLAIMS is nonempty")

PREPARE_FIXTURE()
WRITE_FIXTURE_FILE(
    "README.md"
    "Reached 1000 REQUESTS/S; artifact and evidence will be added later.\n")
EXPECT_SCANNER_FAILURE("misleading performance evidence keywords"
                       "FORBIDDEN_UNBOUND_PERFORMANCE_CLAIMS is nonempty")

PREPARE_FIXTURE()
file(WRITE "${TEST_ROOT}/outside.cc" "void outside() {}\n")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E create_symlink
            "${TEST_ROOT}/outside.cc" "${FIXTURE_SOURCE}/src/linked.cc"
    RESULT_VARIABLE SYMLINK_RESULT)
if(NOT SYMLINK_RESULT EQUAL 0)
  message(FATAL_ERROR "failed to create source-root escape fixture")
endif()
EXPECT_SCANNER_FAILURE("source symlink escape" "input escapes source root")

PREPARE_FIXTURE()
OUTPUT_DIGEST(BEFORE_STRANDED_RECOVERY_DIGEST)
set(STRANDED_BACKUP "${TEST_ROOT}/.accepted-output.backup-stranded-fixture")
file(REMOVE_RECURSE "${STRANDED_BACKUP}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E rename
            "${FIXTURE_OUTPUT}" "${STRANDED_BACKUP}"
    RESULT_VARIABLE STRAND_RESULT)
if(NOT STRAND_RESULT EQUAL 0 OR EXISTS "${FIXTURE_OUTPUT}")
  message(FATAL_ERROR "failed to strand accepted output fixture")
endif()
execute_process(
    COMMAND "${CMAKE_COMMAND}"
            -DSOURCE_ROOT=${FIXTURE_SOURCE}
            -DOUTPUT_ROOT=${FIXTURE_OUTPUT}
            -P ${SCANNER}
    RESULT_VARIABLE RECOVERY_RESULT
    OUTPUT_VARIABLE RECOVERY_STDOUT
    ERROR_VARIABLE RECOVERY_STDERR)
if(NOT RECOVERY_RESULT EQUAL 0)
  message(FATAL_ERROR
          "stranded backup recovery failed\n${RECOVERY_STDOUT}\n${RECOVERY_STDERR}")
endif()
if(NOT EXISTS "${FIXTURE_OUTPUT}" OR EXISTS "${STRANDED_BACKUP}")
  message(FATAL_ERROR "stranded backup was not recovered and retired")
endif()
OUTPUT_DIGEST(AFTER_STRANDED_RECOVERY_DIGEST)
if(NOT BEFORE_STRANDED_RECOVERY_DIGEST STREQUAL
       AFTER_STRANDED_RECOVERY_DIGEST)
  message(FATAL_ERROR "stranded backup recovery changed accepted bytes")
endif()

PREPARE_FIXTURE()
OUTPUT_DIGEST(BEFORE_PUBLISH_FAILURE_DIGEST)
execute_process(
    COMMAND "${CMAKE_COMMAND}"
            -DSOURCE_ROOT=${FIXTURE_SOURCE}
            -DOUTPUT_ROOT=${FIXTURE_OUTPUT}
            -DSOURCE_CONTRACT_TEST_FAIL_AFTER_BACKUP=ON
            -P ${SCANNER}
    RESULT_VARIABLE PUBLISH_FAILURE_RESULT
    OUTPUT_VARIABLE PUBLISH_FAILURE_STDOUT
    ERROR_VARIABLE PUBLISH_FAILURE_STDERR)
if(PUBLISH_FAILURE_RESULT EQUAL 0)
  message(FATAL_ERROR "injected publication failure unexpectedly succeeded")
endif()
set(PUBLISH_FAILURE_LOG
    "${PUBLISH_FAILURE_STDOUT}\n${PUBLISH_FAILURE_STDERR}")
if(NOT PUBLISH_FAILURE_LOG MATCHES "injected publication failure")
  message(FATAL_ERROR
          "wrong injected publication failure\n${PUBLISH_FAILURE_LOG}")
endif()
OUTPUT_DIGEST(AFTER_PUBLISH_FAILURE_DIGEST)
if(NOT BEFORE_PUBLISH_FAILURE_DIGEST STREQUAL AFTER_PUBLISH_FAILURE_DIGEST)
  message(FATAL_ERROR "publication failure did not restore accepted output")
endif()
