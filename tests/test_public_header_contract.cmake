if(NOT DEFINED CEDAR_SOURCE_DIR)
  message(FATAL_ERROR "CEDAR_SOURCE_DIR is required")
endif()

set(CEDAR_PUBLIC_INCLUDE_DIR "${CEDAR_SOURCE_DIR}/include/cedar")
if(NOT IS_DIRECTORY "${CEDAR_PUBLIC_INCLUDE_DIR}")
  message(FATAL_ERROR "Cedar public include directory is missing")
endif()

file(GLOB_RECURSE CEDAR_PUBLIC_HEADERS
     "${CEDAR_PUBLIC_INCLUDE_DIR}/*.h")
if(CEDAR_PUBLIC_HEADERS STREQUAL "")
  message(FATAL_ERROR "No Cedar public headers were found")
endif()

set(CEDAR_FORBIDDEN_PUBLIC_PATTERNS
    "rocksdb/"
    "RocksDb"
    "CedarMaintenance"
    "FactStoreMaintenance"
    "WalDurableCallback")

foreach(CEDAR_PUBLIC_HEADER IN LISTS CEDAR_PUBLIC_HEADERS)
  file(READ "${CEDAR_PUBLIC_HEADER}" CEDAR_PUBLIC_HEADER_CONTENT)
  foreach(CEDAR_FORBIDDEN_PATTERN IN LISTS CEDAR_FORBIDDEN_PUBLIC_PATTERNS)
    if(CEDAR_PUBLIC_HEADER_CONTENT MATCHES "${CEDAR_FORBIDDEN_PATTERN}")
      message(FATAL_ERROR
        "Forbidden implementation symbol '${CEDAR_FORBIDDEN_PATTERN}' "
        "is exposed by ${CEDAR_PUBLIC_HEADER}")
    endif()
  endforeach()
endforeach()

message(STATUS "Cedar public header contract passed")
