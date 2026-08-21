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

file(GLOB_RECURSE cedar_sources
     "${CEDAR_SOURCE_DIR}/src/core/*.cc"
     "${CEDAR_SOURCE_DIR}/src/kernel/*.cc"
     "${CEDAR_SOURCE_DIR}/src/runtime/*.cc"
     "${CEDAR_SOURCE_DIR}/src/storage/*.cc"
     "${CEDAR_SOURCE_DIR}/src/types/*.cc")
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
    "${CEDAR_SOURCE_DIR}/src/storage/rocks/commit_publisher.cc"
    "${CEDAR_SOURCE_DIR}/src/storage/rocks/decided_epoch.cc"
    "${CEDAR_SOURCE_DIR}/src/storage/rocks/rocks_adapter.cc"
    "${CEDAR_SOURCE_DIR}/src/storage/rocks/rocksdb_config.cc"
    "${CEDAR_SOURCE_DIR}/src/storage/rocks/storage_file_inspection.cc")
foreach(source IN LISTS expected_rocksdb_include_sources)
  file(REAL_PATH "${source}" source)
  list(APPEND normalized_expected_rocksdb_include_sources "${source}")
endforeach()
list(SORT rocksdb_include_sources)
list(SORT normalized_expected_rocksdb_include_sources)
if(NOT rocksdb_include_sources STREQUAL normalized_expected_rocksdb_include_sources)
  message(FATAL_ERROR
          "only private Rocks adapter components may include RocksDB; found: ${rocksdb_include_sources}")
endif()

foreach(directory IN ITEMS "${CEDAR_SOURCE_DIR}/src/kernel"
        "${CEDAR_SOURCE_DIR}/src/storage/facts")
  file(GLOB_RECURSE implementation_files "${directory}/*.cc" "${directory}/*.h")
  foreach(implementation_file IN LISTS implementation_files)
    file(READ "${implementation_file}" implementation_contents)
    if(implementation_contents MATCHES "#include[ \\t]*[<\\\"]rocksdb/" OR
       implementation_contents MATCHES "rocksdb::")
      message(FATAL_ERROR
              "RocksDB use escaped the private adapter: ${implementation_file}")
    endif()
  endforeach()
endforeach()

file(READ "${CEDAR_SOURCE_DIR}/CMakeLists.txt" cmake_lists)
foreach(forbidden_path IN ITEMS src/blob/ src/cache/ src/tcypher/
        src/optimizer/ src/observability/ src/benchmark/
        src/columnar/sst.cc src/projection/ include/cedar/projection/)
  if(cmake_lists MATCHES "${forbidden_path}")
    message(FATAL_ERROR "legacy source is in Cedar build graph: ${forbidden_path}")
  endif()
endforeach()

if(EXISTS "${CEDAR_SOURCE_DIR}/archive/pre-rocksdb-kernel-2026-08-01")
  message(FATAL_ERROR "retired pre-Kernel source archive remains in Cedar")
endif()

if(NOT EXISTS "${CEDAR_SOURCE_DIR}/third_party/cedar_codecs/PROVENANCE.md")
  message(FATAL_ERROR "Cedar codec provenance is missing")
endif()

set(embedded_engine_root "${CEDAR_SOURCE_DIR}/src/engine/rocksdb")
if(NOT EXISTS "${embedded_engine_root}/CMakeLists.txt" OR
   NOT EXISTS "${embedded_engine_root}/PROVENANCE.md" OR
   NOT EXISTS "${embedded_engine_root}/LICENSE.Apache")
  message(FATAL_ERROR "embedded Cedar engine source or provenance is missing")
endif()
if(EXISTS "${embedded_engine_root}/.git")
  message(FATAL_ERROR "embedded Cedar engine contains nested Git metadata")
endif()
if(NOT EXISTS "${CEDAR_SOURCE_DIR}/src/engine/cedar/README.md" OR
   NOT EXISTS "${CEDAR_SOURCE_DIR}/src/engine/cedar/MAINTENANCE.md")
  message(FATAL_ERROR "Cedar engine extension ownership record is missing")
endif()
if(EXISTS "${CEDAR_SOURCE_DIR}/third_party/rocksdb")
  message(FATAL_ERROR "retired RocksDB submodule source remains")
endif()
file(READ "${CEDAR_SOURCE_DIR}/.gitmodules" cedar_gitmodules)
if(cedar_gitmodules MATCHES "third_party/rocksdb")
  message(FATAL_ERROR "RocksDB submodule declaration remains")
endif()

foreach(forbidden_directory IN ITEMS "${CEDAR_SOURCE_DIR}/src/projection"
        "${CEDAR_SOURCE_DIR}/include/cedar/projection")
  if(EXISTS "${forbidden_directory}")
    message(FATAL_ERROR "projection sidecar source remains: ${forbidden_directory}")
  endif()
endforeach()
