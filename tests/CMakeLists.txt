include(GoogleTest)

add_test(NAME PublicHeaderContract
    COMMAND ${CMAKE_COMMAND}
        -DCEDAR_SOURCE_DIR=${CMAKE_SOURCE_DIR}
        -P ${CMAKE_SOURCE_DIR}/tests/test_public_header_contract.cmake)

add_test(NAME EmbeddedEngineContract
    COMMAND ${CMAKE_COMMAND}
        -DCEDAR_SOURCE_DIR=${CMAKE_SOURCE_DIR}
        -P ${CMAKE_SOURCE_DIR}/tests/test_embedded_engine_contract.cmake)

add_executable(test_public_header_compile public/test_public_header_compile.cc)
target_link_libraries(test_public_header_compile PRIVATE cedar_core)
add_test(NAME PublicHeaderCompile COMMAND test_public_header_compile)

if(GTest_FOUND)
    set(GTEST_TARGET GTest::gtest)
    set(GTEST_MAIN_TARGET GTest::gtest_main)
else()
    set(GTEST_TARGET gtest)
    set(GTEST_MAIN_TARGET gtest_main)
endif()

add_executable(test_query_types query/test_query_types.cc)
target_link_libraries(test_query_types PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_types)

add_executable(test_query_part_scope query/test_query_part_scope.cc)
target_link_libraries(test_query_part_scope PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_part_scope TEST_PREFIX "QueryPartScope.")

add_executable(test_query_read_context query/test_query_read_context.cc)
target_include_directories(test_query_read_context PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_query_read_context PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_read_context TEST_PREFIX "QueryReadContext.")

add_executable(test_query_read_path_baseline query/test_query_read_path_baseline.cc)
target_link_libraries(test_query_read_path_baseline PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_read_path_baseline TEST_PREFIX "QueryReadBaseline.")

add_executable(test_indexed_read_baseline query/test_indexed_read_baseline.cc)
target_link_libraries(test_indexed_read_baseline PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_indexed_read_baseline TEST_PREFIX "IndexedReadBaseline.")

add_executable(test_query_plan_outcome query/test_query_plan_outcome.cc)
target_include_directories(test_query_plan_outcome PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_query_plan_outcome PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_plan_outcome TEST_PREFIX "QueryPlanOutcome.")

add_executable(test_cypher_parser cypher/test_cypher_parser.cc)
target_link_libraries(test_cypher_parser PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_cypher_parser TEST_PREFIX "CypherParser.")

add_executable(test_cypher_binder cypher/test_cypher_binder.cc)
target_link_libraries(test_cypher_binder PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_cypher_binder TEST_PREFIX "CypherBinder.")

add_executable(test_cypher_compile_execute cypher/test_cypher_compile_execute.cc)
target_link_libraries(test_cypher_compile_execute PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_cypher_compile_execute TEST_PREFIX "CypherCompiler.")

add_executable(test_cypher_embedded_api cypher/test_cypher_embedded_api.cc)
target_link_libraries(test_cypher_embedded_api PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_cypher_embedded_api TEST_PREFIX "CypherEmbedded.")

add_executable(test_cypher_writes cypher/test_cypher_writes.cc)
target_link_libraries(test_cypher_writes PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_cypher_writes TEST_PREFIX "CypherWrite.")

add_executable(test_tcypher_semantic_matrix cypher/test_tcypher_semantic_matrix.cc)
target_link_libraries(test_tcypher_semantic_matrix PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_tcypher_semantic_matrix TEST_PREFIX "TCypherSemanticMatrix.")

add_executable(test_cypher_session cypher/test_cypher_session.cc)
target_link_libraries(test_cypher_session PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_cypher_session TEST_PREFIX "CypherSession.")

add_executable(test_cypher_access_profile cypher/test_cypher_access_profile.cc)
target_link_libraries(test_cypher_access_profile PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_cypher_access_profile TEST_PREFIX "CypherAccessProfile.")

add_executable(test_schema_manifest cypher/test_schema_manifest.cc)
target_link_libraries(test_schema_manifest PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_schema_manifest TEST_PREFIX "SchemaManifest.")

add_executable(test_schema_provider cypher/test_schema_provider.cc)
target_link_libraries(test_schema_provider PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_schema_provider TEST_PREFIX "SchemaProvider.")

add_executable(test_server_lifecycle server/test_server_lifecycle.cc)
target_link_libraries(test_server_lifecycle PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_server_lifecycle TEST_PREFIX "Server.")

add_executable(test_bolt_protocol server/test_bolt_protocol.cc)
target_link_libraries(test_bolt_protocol PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_bolt_protocol TEST_PREFIX "Bolt.")

add_executable(test_query_differential query/test_query_differential.cc)
target_include_directories(test_query_differential PRIVATE
    ${CMAKE_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_query_differential PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_differential TEST_PREFIX "QueryDifferential.")

add_executable(test_logical_plan query/test_logical_plan.cc)
target_include_directories(test_logical_plan PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_logical_plan PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_logical_plan)

add_executable(test_query_canonical query/test_query_canonical.cc)
target_include_directories(test_query_canonical PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_query_canonical PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_canonical)

add_executable(test_query_lifecycle query/test_query_lifecycle.cc)
target_include_directories(test_query_lifecycle PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_query_lifecycle PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_lifecycle TEST_PREFIX "QueryLifecycle.")

add_executable(test_query_relational query/test_query_relational.cc)
target_include_directories(test_query_relational PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_query_relational PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_relational TEST_PREFIX "QueryRelational.")

add_executable(test_query_complexity_regressions
  query/test_query_complexity_regressions.cc)
target_include_directories(test_query_complexity_regressions
  PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_query_complexity_regressions
  PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_complexity_regressions
  TEST_PREFIX "QueryComplexity.")

add_executable(test_query_resources query/test_query_resources.cc)
target_include_directories(test_query_resources PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_query_resources PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_resources TEST_PREFIX "QueryResource.")

add_executable(test_projection_format query/test_projection_format.cc)
target_include_directories(test_projection_format PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_projection_format PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_projection_format)

add_executable(test_projection_store query/test_projection_store.cc)
target_include_directories(test_projection_store PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_projection_store PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_projection_store)

add_executable(test_property_index_builder query/test_property_index_builder.cc)
target_include_directories(test_property_index_builder PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_property_index_builder PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_property_index_builder)

add_executable(test_indexed_property_runtime query/test_indexed_property_runtime.cc)
target_include_directories(test_indexed_property_runtime PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_indexed_property_runtime PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_indexed_property_runtime)

add_executable(test_projection_page_reader query/test_projection_page_reader.cc)
target_include_directories(test_projection_page_reader PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_projection_page_reader PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_projection_page_reader TEST_PREFIX "ProjectionPageReader.")

add_executable(test_query_delta query/test_query_delta.cc)
target_include_directories(test_query_delta PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_query_delta PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_delta)

add_executable(test_query_observability query/test_query_observability.cc)
target_include_directories(test_query_observability PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_query_observability PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_observability TEST_PREFIX "QueryObservability.")

add_executable(test_query_planner query/test_query_planner.cc)
target_include_directories(test_query_planner PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_query_planner PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_planner)

add_executable(test_temporal_expand query/test_temporal_expand.cc)
target_include_directories(test_temporal_expand PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_temporal_expand PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_temporal_expand TEST_PREFIX "TemporalExpand.")

add_executable(test_coexisting_path query/test_coexisting_path.cc)
target_include_directories(test_coexisting_path PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_coexisting_path PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_coexisting_path TEST_PREFIX "CoexistingPath.")

add_executable(test_temporal_journey query/test_temporal_journey.cc)
target_include_directories(test_temporal_journey PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_temporal_journey PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_temporal_journey TEST_PREFIX "TemporalJourney.")

set(CEDAR_TEST_LIBS
    cedar_core
    ${GTEST_MAIN_TARGET}
)

add_executable(test_temporal_model query/test_temporal_model.cc)
target_include_directories(test_temporal_model PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_temporal_model PRIVATE ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_temporal_model)

add_executable(test_fact_chain_cursor query/test_fact_chain_cursor.cc)
target_include_directories(test_fact_chain_cursor PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_fact_chain_cursor PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_fact_chain_cursor TEST_PREFIX "FactChainCursor.")

add_executable(test_read_catalog query/test_read_catalog.cc)
target_include_directories(test_read_catalog PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_read_catalog PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_read_catalog TEST_PREFIX "ReadCatalog.")

add_executable(test_read_binding_capsule query/test_read_binding_capsule.cc)
target_include_directories(test_read_binding_capsule PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_read_binding_capsule PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_read_binding_capsule TEST_PREFIX "ReadBindingCapsule.")

function(configure_cedar_parquet_test target)
    target_include_directories(${target} PRIVATE
        ${CMAKE_SOURCE_DIR}/src/engine/rocksdb)
    target_compile_definitions(${target} PRIVATE
        ROCKSDB_PLATFORM_POSIX
        ROCKSDB_LIB_IO_POSIX
        OS_MACOSX
        NDEBUG
        HAVE_FULLFSYNC
        LZ4
        ZSTD
        CEDAR_ZSTD_NO_DICTIONARY_TRAINING)
    target_link_libraries(${target} PRIVATE cedar_rocksdb ${GTEST_MAIN_TARGET})
    gtest_discover_tests(${target})
endfunction()

set(CEDAR_EXTERNAL_PARQUET_ROOT "" CACHE PATH
    "Pinned Apache Arrow/Parquet cache root used only for interop tests")
if(CEDAR_EXTERNAL_PARQUET_ROOT)
    if(NOT EXISTS "${CEDAR_EXTERNAL_PARQUET_ROOT}/cedar-arrow-parquet-manifest.txt")
        message(FATAL_ERROR "CEDAR_EXTERNAL_PARQUET_ROOT must contain the pinned manifest")
    endif()
    set(Arrow_ROOT "${CEDAR_EXTERNAL_PARQUET_ROOT}/install")
    find_package(Parquet 25.0.0 CONFIG REQUIRED NO_DEFAULT_PATH
        PATHS "${CEDAR_EXTERNAL_PARQUET_ROOT}/install/lib/cmake/Parquet")
    add_executable(test_cedar_parquet_external_interop
        ${CMAKE_SOURCE_DIR}/tests/test_cedar_parquet_external_interop.cc)
    configure_cedar_parquet_test(test_cedar_parquet_external_interop)
    target_compile_options(test_cedar_parquet_external_interop PRIVATE -ULZ4 -UZSTD)
    target_link_libraries(test_cedar_parquet_external_interop
        PRIVATE Parquet::parquet_static)
endif()

add_executable(test_rocksdb_dependency test_rocksdb_dependency.cc)
target_link_libraries(test_rocksdb_dependency
    PRIVATE cedar_rocksdb ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_rocksdb_dependency)

add_executable(test_rocksdb_cedar_kernel
    test_rocksdb_cedar_kernel.cc
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/test_util/sync_point.cc
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/test_util/sync_point_impl.cc)
target_include_directories(test_rocksdb_cedar_kernel PRIVATE
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb)
target_compile_definitions(test_rocksdb_cedar_kernel PRIVATE
    ROCKSDB_PLATFORM_POSIX
    ROCKSDB_LIB_IO_POSIX
    OS_MACOSX
    HAVE_FULLFSYNC
    LZ4
    ZSTD
    CEDAR_ZSTD_NO_DICTIONARY_TRAINING)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_definitions(test_rocksdb_cedar_kernel PRIVATE
        CEDAR_ROCKSDB_SYNC_POINTS=1)
endif()
target_link_libraries(test_rocksdb_cedar_kernel
    PRIVATE cedar_rocksdb ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_rocksdb_cedar_kernel)

add_executable(test_cedar_parquet_kernel
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/parquet_kernel_test.cc
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/compact_protocol.cc
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/parquet_metadata.cc
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/parquet_plain_page.cc)
configure_cedar_parquet_test(test_cedar_parquet_kernel)

add_executable(test_cedar_parquet_format
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/cedar_parquet_format_test.cc
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/cedar_parquet_format.cc)
configure_cedar_parquet_test(test_cedar_parquet_format)

add_executable(test_cedar_parquet_table_builder
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/cedar_parquet_table_builder_test.cc
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/cedar_parquet_table_builder.cc
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/parquet_metadata.cc
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/parquet_plain_page.cc)
configure_cedar_parquet_test(test_cedar_parquet_table_builder)

add_executable(test_cedar_parquet_table_reader
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/cedar_parquet_table_reader_test.cc
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/cedar_parquet_table_reader.cc
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/parquet_metadata.cc
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/parquet_plain_page.cc)
configure_cedar_parquet_test(test_cedar_parquet_table_reader)

add_executable(test_cedar_parquet_table_factory
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/table/cedar_parquet/cedar_parquet_table_factory_test.cc)
configure_cedar_parquet_test(test_cedar_parquet_table_factory)

add_executable(test_partitioned_version_radix_memtable
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc)
configure_cedar_parquet_test(test_partitioned_version_radix_memtable)

add_executable(test_fact_model storage/test_fact_model.cc)
target_link_libraries(test_fact_model ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_fact_model)

add_executable(test_fact_codec storage/test_fact_codec.cc)
target_link_libraries(test_fact_codec ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_fact_codec)

add_executable(test_fact_event_canonical storage/test_fact_event_canonical.cc)
target_include_directories(test_fact_event_canonical PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_fact_event_canonical ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_fact_event_canonical TEST_PREFIX "FactEventCanonical.")

add_executable(test_fact_store storage/test_fact_store.cc)
target_include_directories(test_fact_store PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_fact_store
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_fact_store)

add_executable(test_fact_store_commit storage/test_fact_store_commit.cc)
target_include_directories(test_fact_store_commit PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_fact_store_commit
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_fact_store_commit)

add_executable(test_fact_store_metadata storage/test_fact_store_metadata.cc)
target_include_directories(test_fact_store_metadata PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_fact_store_metadata
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_fact_store_metadata)

add_executable(test_kernel_interface public/test_kernel_interface.cc)
target_link_libraries(test_kernel_interface
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_kernel_interface)

add_executable(test_kernel_snapshot kernel/test_kernel_snapshot.cc)
target_include_directories(test_kernel_snapshot PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/tests)
target_link_libraries(test_kernel_snapshot
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_kernel_snapshot)

add_executable(test_columnar_fact_scan storage/test_columnar_fact_scan.cc)
target_include_directories(test_columnar_fact_scan PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/engine/rocksdb)
target_link_libraries(test_columnar_fact_scan
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_columnar_fact_scan)

add_executable(test_canonical_fact_reader storage/test_canonical_fact_reader.cc)
target_link_libraries(test_canonical_fact_reader PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_canonical_fact_reader TEST_PREFIX "CanonicalFactReader.")

add_executable(test_kernel_mutations kernel/test_kernel_mutations.cc)
target_include_directories(test_kernel_mutations PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_kernel_mutations
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_kernel_mutations)

add_executable(test_snapshot_transactions kernel/test_snapshot_transactions.cc)
target_include_directories(test_snapshot_transactions PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_snapshot_transactions
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_snapshot_transactions)

add_executable(test_strict_transactions kernel/test_strict_transactions.cc)
target_include_directories(test_strict_transactions PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_strict_transactions
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_strict_transactions)

add_executable(test_kernel_commit kernel/test_kernel_commit.cc)
target_include_directories(test_kernel_commit PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_kernel_commit
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_kernel_commit)

add_executable(test_adaptive_epoch_controller
    kernel/test_adaptive_epoch_controller.cc)
target_include_directories(test_adaptive_epoch_controller PRIVATE
    ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_adaptive_epoch_controller
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_adaptive_epoch_controller)

add_executable(test_epoch_completion kernel/test_epoch_completion.cc)
target_include_directories(test_epoch_completion PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_epoch_completion
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_epoch_completion)

add_executable(test_async_submission_executor kernel/test_async_submission_executor.cc)
target_include_directories(test_async_submission_executor PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_async_submission_executor
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_async_submission_executor)

add_executable(test_group_commit_planner storage/test_group_commit_planner.cc)
target_include_directories(test_group_commit_planner PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_group_commit_planner
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_group_commit_planner)

add_executable(test_version_validation_cache storage/test_version_validation_cache.cc)
target_include_directories(test_version_validation_cache PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_version_validation_cache
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_version_validation_cache)

add_executable(test_recent_fact_write_index storage/test_recent_fact_write_index.cc)
target_include_directories(test_recent_fact_write_index PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_recent_fact_write_index
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_recent_fact_write_index)

add_executable(test_pressure_controller kernel/test_pressure_controller.cc)
target_link_libraries(test_pressure_controller
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_pressure_controller)

add_executable(test_maintenance_policy kernel/test_maintenance_policy.cc)
target_include_directories(test_maintenance_policy PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_maintenance_policy
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_maintenance_policy)

add_executable(test_maintenance_controller kernel/test_maintenance_controller.cc)
target_include_directories(test_maintenance_controller PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_maintenance_controller
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_maintenance_controller)

add_executable(test_rocksdb_profile storage/test_rocksdb_profile.cc)
target_include_directories(test_rocksdb_profile PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_rocksdb_profile
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_rocksdb_profile)

add_executable(test_kernel_bench_options performance/test_kernel_bench_options.cc)
target_include_directories(test_kernel_bench_options PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(test_kernel_bench_options
    PRIVATE cedar_kernel_bench_options ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_kernel_bench_options)

add_executable(test_query_bench_options performance/test_query_bench_options.cc)
target_include_directories(test_query_bench_options PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(test_query_bench_options PRIVATE cedar_query_bench_options ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_bench_options TEST_PREFIX "QueryBench.")

add_executable(test_query_bench_workload performance/test_query_bench_workload.cc)
target_include_directories(test_query_bench_workload PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(test_query_bench_workload
    PRIVATE cedar_query_bench_workload cedar_query_bench_options ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_bench_workload TEST_PREFIX "QueryBench.")

if(TARGET cedar_query_bench)
  add_test(NAME QueryBenchmarkCsvContract COMMAND ${CMAKE_COMMAND}
      -DCEDAR_BENCHMARK=$<TARGET_FILE:cedar_query_bench>
      -P ${CMAKE_CURRENT_SOURCE_DIR}/performance/test_query_benchmark_csv.cmake)
  add_test(NAME QueryCampaignOptionsContract COMMAND ${CMAKE_COMMAND}
      -DCEDAR_CAMPAIGN=${CMAKE_SOURCE_DIR}/benchmarks/run_cedar_query_campaign.sh
      -DCEDAR_BENCHMARK=$<TARGET_FILE:cedar_query_bench>
      -P ${CMAKE_CURRENT_SOURCE_DIR}/performance/test_query_campaign_options.cmake)
  add_test(NAME QueryArtifactAuditContract COMMAND ${CMAKE_COMMAND}
      -DCEDAR_CAMPAIGN=${CMAKE_SOURCE_DIR}/benchmarks/run_cedar_query_campaign.sh
      -DCEDAR_BENCHMARK=$<TARGET_FILE:cedar_query_bench>
      -P ${CMAKE_CURRENT_SOURCE_DIR}/performance/test_query_artifact_audit.cmake)
endif()

add_executable(test_kernel_bounded_benchmark performance/test_kernel_bounded_benchmark.cc)
target_include_directories(test_kernel_bounded_benchmark PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(test_kernel_bounded_benchmark
    PRIVATE cedar_kernel_bench_workload ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_kernel_bounded_benchmark)

if(TARGET cedar_kernel_bench)
    add_test(NAME KernelBenchmarkCsvContract
        COMMAND ${CMAKE_COMMAND}
            -DCEDAR_BENCHMARK=$<TARGET_FILE:cedar_kernel_bench>
            -P ${CMAKE_CURRENT_SOURCE_DIR}/performance/test_kernel_benchmark_csv.cmake)
endif()

add_executable(test_commit_publisher storage/test_commit_publisher.cc)
target_include_directories(test_commit_publisher PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_commit_publisher
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_commit_publisher)

add_executable(test_temporal_neighborhood storage/test_temporal_neighborhood.cc)
target_include_directories(test_temporal_neighborhood PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_temporal_neighborhood
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_temporal_neighborhood)

add_executable(test_kernel_lifecycle kernel/test_kernel_lifecycle.cc)
target_link_libraries(test_kernel_lifecycle
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_kernel_lifecycle)

add_executable(test_rocksdb_lifecycle storage/test_rocksdb_lifecycle.cc)
target_include_directories(test_rocksdb_lifecycle PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_rocksdb_lifecycle
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_rocksdb_lifecycle)

add_executable(test_cedar_files tools/test_cedar_files.cc)
target_include_directories(test_cedar_files PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_compile_definitions(test_cedar_files PRIVATE
    CEDAR_BINARY="$<TARGET_FILE:cedar>")
target_link_libraries(test_cedar_files PRIVATE cedar_core ${GTEST_MAIN_TARGET})
add_dependencies(test_cedar_files cedar)
gtest_discover_tests(test_cedar_files)

add_executable(test_vacuum storage/test_vacuum.cc)
target_include_directories(test_vacuum PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_vacuum
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_vacuum)

add_executable(test_recovery_crash_matrix recovery/test_crash_matrix.cc)
target_include_directories(test_recovery_crash_matrix PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_recovery_crash_matrix
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_recovery_crash_matrix)

add_executable(test_query_crash_matrix recovery/test_query_crash_matrix.cc)
target_include_directories(test_query_crash_matrix PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_query_crash_matrix PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_crash_matrix TEST_PREFIX "QueryCrashMatrix.")

add_executable(test_recovery_format recovery/test_format.cc)
target_include_directories(test_recovery_format PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_recovery_format
    PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_recovery_format)

add_test(
    NAME RocksKernelSourceContract
    COMMAND ${CMAKE_COMMAND}
        -DCEDAR_SOURCE_DIR=${CMAKE_SOURCE_DIR}
        -P ${CMAKE_SOURCE_DIR}/cmake/VerifyRocksKernelSourceContract.cmake)

add_test(
    NAME RocksKernelInstallContract
    COMMAND ${CMAKE_COMMAND}
        -DCEDAR_SOURCE_DIR=${CMAKE_SOURCE_DIR}
        -P ${CMAKE_SOURCE_DIR}/cmake/VerifyRocksKernelInstallContract.cmake)

add_test(
    NAME CedarInstallConsumer
    COMMAND ${CMAKE_COMMAND}
        -DCEDAR_SOURCE_DIR=${CMAKE_SOURCE_DIR}
        -P ${CMAKE_SOURCE_DIR}/tests/test_install_consumer.cmake)
