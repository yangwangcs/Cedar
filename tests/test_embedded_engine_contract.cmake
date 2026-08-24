if(NOT DEFINED CEDAR_SOURCE_DIR)
  message(FATAL_ERROR "CEDAR_SOURCE_DIR is required")
endif()

set(engine_root "${CEDAR_SOURCE_DIR}/src/engine/rocksdb")
if(NOT EXISTS "${engine_root}/CMakeLists.txt")
  message(FATAL_ERROR "embedded Cedar engine source is missing")
endif()

if(EXISTS "${engine_root}/.git")
  message(FATAL_ERROR "embedded Cedar engine must not contain nested Git metadata")
endif()

if(EXISTS "${CEDAR_SOURCE_DIR}/third_party/rocksdb")
  message(FATAL_ERROR "RocksDB must not remain a Cedar submodule")
endif()

if(NOT EXISTS "${engine_root}/PROVENANCE.md")
  message(FATAL_ERROR "embedded Cedar engine provenance is missing")
endif()

if(NOT EXISTS "${engine_root}/LICENSE.Apache")
  message(FATAL_ERROR "embedded Cedar engine license is missing")
endif()

if(NOT EXISTS "${engine_root}/cmake/modules/ReadVersion.cmake")
  message(FATAL_ERROR "embedded Cedar engine CMake module is missing")
endif()

if(NOT EXISTS "${engine_root}/docs/README.md")
  message(FATAL_ERROR "embedded Cedar engine source documentation is missing")
endif()

if(NOT EXISTS "${CEDAR_SOURCE_DIR}/src/engine/cedar/README.md")
  message(FATAL_ERROR "Cedar engine extension ownership record is missing")
endif()

if(NOT EXISTS "${CEDAR_SOURCE_DIR}/src/engine/cedar/MAINTENANCE.md")
  message(FATAL_ERROR "Cedar engine maintenance procedure is missing")
endif()

file(READ "${CEDAR_SOURCE_DIR}/.gitmodules" gitmodules)
if(gitmodules MATCHES "third_party/rocksdb")
  message(FATAL_ERROR "RocksDB submodule declaration remains in .gitmodules")
endif()

# An engine source edit must make the parent build reconfigure, otherwise its
# content-addressed cache key can silently keep linking the previous library.
file(READ "${CEDAR_SOURCE_DIR}/cmake/CedarRocksDB.cmake" rocksdb_build)
if(NOT rocksdb_build MATCHES "GLOB_RECURSE[ \\t\\r\\n]+CEDAR_ENGINE_SOURCE_FILES[ \\t\\r\\n]+CONFIGURE_DEPENDS")
  message(FATAL_ERROR "embedded engine glob must detect added and removed sources")
endif()
string(FIND "${rocksdb_build}"
       "set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS"
       configure_depends_position)
if(configure_depends_position EQUAL -1)
  message(FATAL_ERROR "embedded engine source edits must trigger reconfiguration")
endif()
