if(NOT DEFINED CEDAR_SOURCE_DIR)
  message(FATAL_ERROR "CEDAR_SOURCE_DIR is required")
endif()

file(GLOB_RECURSE cedar_public_headers
     "${CEDAR_SOURCE_DIR}/include/cedar/*.h")
foreach(header IN LISTS cedar_public_headers)
  file(READ "${header}" contents)
  if(contents MATCHES "#include[ \t]*[<\"]rocksdb/")
    message(FATAL_ERROR "public Cedar header exposes RocksDB: ${header}")
  endif()
endforeach()

file(GLOB_RECURSE cedar_sources "${CEDAR_SOURCE_DIR}/src/*.cc")
set(rocksdb_include_sources)
foreach(source IN LISTS cedar_sources)
  file(REAL_PATH "${source}" source)
  file(READ "${source}" contents)
  if(contents MATCHES "#include[ \t]*[<\"]rocksdb/")
    list(APPEND rocksdb_include_sources "${source}")
  endif()
    foreach(forbidden IN ITEMS DecisionLog ShardPrepareLog TemporalMemTable
          VersionSet CedarDatabase PrepareRecord PrepareReference
          WriteCommittedGroup)
    if(contents MATCHES "${forbidden}")
      message(FATAL_ERROR "legacy storage symbol ${forbidden} found in ${source}")
    endif()
  endforeach()
endforeach()

set(expected_rocksdb_include_sources
    "${CEDAR_SOURCE_DIR}/src/fact/commit_publisher.cc"
    "${CEDAR_SOURCE_DIR}/src/fact/decided_epoch.cc"
    "${CEDAR_SOURCE_DIR}/src/fact/fact_store.cc"
    "${CEDAR_SOURCE_DIR}/src/fact/rocksdb_config.cc")
foreach(source IN LISTS expected_rocksdb_include_sources)
  file(REAL_PATH "${source}" source)
  list(APPEND normalized_expected_rocksdb_include_sources "${source}")
endforeach()
list(SORT rocksdb_include_sources)
list(SORT normalized_expected_rocksdb_include_sources)
if(NOT rocksdb_include_sources STREQUAL normalized_expected_rocksdb_include_sources)
  message(FATAL_ERROR
          "only private FactStore components may include RocksDB; found: ${rocksdb_include_sources}")
endif()

file(READ "${CEDAR_SOURCE_DIR}/CMakeLists.txt" cmake_lists)
foreach(forbidden_path IN ITEMS src/storage/ src/blob/ src/cache/ src/tcypher/
        src/optimizer/ src/runtime/ src/observability/ src/benchmark/
        src/columnar/sst.cc src/projection/ include/cedar/projection/)
  if(cmake_lists MATCHES "${forbidden_path}")
    message(FATAL_ERROR "legacy source is in Cedar build graph: ${forbidden_path}")
  endif()
endforeach()

foreach(forbidden_directory IN ITEMS "${CEDAR_SOURCE_DIR}/src/projection"
        "${CEDAR_SOURCE_DIR}/include/cedar/projection")
  if(EXISTS "${forbidden_directory}")
    message(FATAL_ERROR "projection sidecar source remains: ${forbidden_directory}")
  endif()
endforeach()
