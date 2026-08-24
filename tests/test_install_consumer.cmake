if(NOT DEFINED CEDAR_SOURCE_DIR)
  message(FATAL_ERROR "CEDAR_SOURCE_DIR is required")
endif()

set(CEDAR_INSTALL_PREFIX "${CEDAR_SOURCE_DIR}/build-install-consumer-prefix")
set(CEDAR_CONSUMER_BUILD "${CEDAR_SOURCE_DIR}/build-install-consumer")

# These are disposable test outputs. Removing the exact directories prevents
# a prior package (for example one containing an old engine archive name) from
# making this contract pass without a fresh install.
file(REMOVE_RECURSE "${CEDAR_INSTALL_PREFIX}"
                    "${CEDAR_CONSUMER_BUILD}"
                    "${CEDAR_SOURCE_DIR}/build-install-producer")

execute_process(
  COMMAND "${CMAKE_COMMAND}" -S "${CEDAR_SOURCE_DIR}" -B
          "${CEDAR_SOURCE_DIR}/build-install-producer"
          -DBUILD_TESTS=OFF -DBUILD_BENCHMARKS=OFF
          -DCMAKE_BUILD_TYPE=Debug
  RESULT_VARIABLE configure_result)
if(NOT configure_result EQUAL 0)
  message(FATAL_ERROR "failed to configure Cedar install producer")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build
          "${CEDAR_SOURCE_DIR}/build-install-producer" --parallel 2
  RESULT_VARIABLE build_result)
if(NOT build_result EQUAL 0)
  message(FATAL_ERROR "failed to build Cedar install producer")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" --install
          "${CEDAR_SOURCE_DIR}/build-install-producer" --prefix
          "${CEDAR_INSTALL_PREFIX}"
  RESULT_VARIABLE install_result)
if(NOT install_result EQUAL 0)
  message(FATAL_ERROR "failed to install Cedar")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" -S
          "${CEDAR_SOURCE_DIR}/tests/public/install_consumer" -B
          "${CEDAR_CONSUMER_BUILD}"
          -DCedar_DIR=${CEDAR_INSTALL_PREFIX}/lib/cmake/Cedar
          -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON
          -DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF
  RESULT_VARIABLE consumer_configure_result)
if(NOT consumer_configure_result EQUAL 0)
  message(FATAL_ERROR "failed to configure Cedar install consumer")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build "${CEDAR_CONSUMER_BUILD}" --parallel 2
  RESULT_VARIABLE consumer_build_result)
if(NOT consumer_build_result EQUAL 0)
  message(FATAL_ERROR "failed to build Cedar install consumer")
endif()
execute_process(
  COMMAND "${CEDAR_CONSUMER_BUILD}/cedar_install_consumer"
  RESULT_VARIABLE consumer_run_result)
if(NOT consumer_run_result EQUAL 0)
  message(FATAL_ERROR "Cedar install consumer failed: ${consumer_run_result}")
endif()

set(CEDAR_PACKAGE_DIR "${CEDAR_INSTALL_PREFIX}/lib/cmake/Cedar")
if(NOT IS_DIRECTORY "${CEDAR_PACKAGE_DIR}")
  message(FATAL_ERROR "Cedar package directory is missing")
endif()
file(GLOB CEDAR_PACKAGE_FILES "${CEDAR_PACKAGE_DIR}/*")
if(CEDAR_PACKAGE_FILES STREQUAL "")
  message(FATAL_ERROR "Cedar package files are missing")
endif()

set(CEDAR_SOURCE_DIR_LOWER "${CEDAR_SOURCE_DIR}")
string(TOLOWER "${CEDAR_SOURCE_DIR_LOWER}" CEDAR_SOURCE_DIR_LOWER)
set(CEDAR_INSTALL_FORBIDDEN_PATTERNS
    "rocksdb"
    "src/query"
    "src/engine"
    "cedar_rocksdb"
    "rocksdb::"
    "columnfamilyhandle"
    "writebatch"
    "readoptions"
    "dboptions")
foreach(CEDAR_PACKAGE_FILE IN LISTS CEDAR_PACKAGE_FILES)
  if(IS_DIRECTORY "${CEDAR_PACKAGE_FILE}")
    continue()
  endif()
  file(READ "${CEDAR_PACKAGE_FILE}" CEDAR_PACKAGE_CONTENT)
  string(TOLOWER "${CEDAR_PACKAGE_CONTENT}" CEDAR_PACKAGE_LOWER)
  foreach(CEDAR_FORBIDDEN_PATTERN IN LISTS CEDAR_INSTALL_FORBIDDEN_PATTERNS)
    if(CEDAR_PACKAGE_LOWER MATCHES "${CEDAR_FORBIDDEN_PATTERN}")
      message(FATAL_ERROR
        "Forbidden implementation symbol '${CEDAR_FORBIDDEN_PATTERN}' "
        "is present in installed package file ${CEDAR_PACKAGE_FILE}")
    endif()
  endforeach()
  if(CEDAR_PACKAGE_LOWER MATCHES "${CEDAR_SOURCE_DIR_LOWER}" OR
     CEDAR_PACKAGE_LOWER MATCHES "/users/|/private/var/")
    message(FATAL_ERROR
      "Absolute source/build path is present in installed package file "
      "${CEDAR_PACKAGE_FILE}")
  endif()
endforeach()

file(GLOB CEDAR_TARGET_EXPORTS "${CEDAR_PACKAGE_DIR}/CedarTargets*.cmake")
if(CEDAR_TARGET_EXPORTS STREQUAL "")
  message(FATAL_ERROR "Cedar target export files are missing")
endif()
set(CEDAR_TARGET_NAMES)
foreach(CEDAR_TARGET_EXPORT IN LISTS CEDAR_TARGET_EXPORTS)
  file(READ "${CEDAR_TARGET_EXPORT}" CEDAR_TARGET_CONTENT)
  string(REGEX MATCHALL "add_library\\(Cedar::[A-Za-z0-9_]+"
         CEDAR_TARGET_MATCHES "${CEDAR_TARGET_CONTENT}")
  list(APPEND CEDAR_TARGET_NAMES ${CEDAR_TARGET_MATCHES})
endforeach()
list(LENGTH CEDAR_TARGET_NAMES CEDAR_TARGET_COUNT)
if(NOT CEDAR_TARGET_COUNT EQUAL 1 OR
   NOT "${CEDAR_TARGET_NAMES}" MATCHES "add_library\\(Cedar::cedar$")
  message(FATAL_ERROR
    "Installed package must export exactly Cedar::cedar; found: "
    "${CEDAR_TARGET_NAMES}")
endif()

foreach(CEDAR_REQUIRED_FILE
    "${CEDAR_INSTALL_PREFIX}/lib/libcedar_core.a"
    "${CEDAR_INSTALL_PREFIX}/lib/cedar/internal/libcedar_engine.a"
    "${CEDAR_INSTALL_PREFIX}/lib/cedar/internal/liblz4.a"
    "${CEDAR_INSTALL_PREFIX}/lib/cedar/internal/libzstd.a")
  if(NOT EXISTS "${CEDAR_REQUIRED_FILE}")
    message(FATAL_ERROR "Installed Cedar artifact is missing: ${CEDAR_REQUIRED_FILE}")
  endif()
endforeach()

set(CEDAR_INSTALLED_HEADERS)
file(GLOB_RECURSE CEDAR_INSTALLED_HEADERS
     "${CEDAR_INSTALL_PREFIX}/include/cedar/*.h")
foreach(CEDAR_INSTALLED_HEADER IN LISTS CEDAR_INSTALLED_HEADERS)
  file(READ "${CEDAR_INSTALLED_HEADER}" CEDAR_HEADER_CONTENT)
  string(TOLOWER "${CEDAR_HEADER_CONTENT}" CEDAR_HEADER_LOWER)
  foreach(CEDAR_FORBIDDEN_PATTERN IN LISTS CEDAR_INSTALL_FORBIDDEN_PATTERNS)
    if(CEDAR_HEADER_LOWER MATCHES "${CEDAR_FORBIDDEN_PATTERN}")
      message(FATAL_ERROR
        "Forbidden implementation symbol '${CEDAR_FORBIDDEN_PATTERN}' "
        "is present in installed header ${CEDAR_INSTALLED_HEADER}")
    endif()
  endforeach()
  if(CEDAR_HEADER_CONTENT MATCHES
     "#[ \t]*include[ \t]*[<\"][^>\"]*internal/")
    message(FATAL_ERROR "Installed header exposes an internal include: ${CEDAR_INSTALLED_HEADER}")
  endif()
endforeach()

message(STATUS "Cedar installed public consumer and package contract passed")
