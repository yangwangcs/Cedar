include_guard(GLOBAL)

set(CEDAR_ROCKSDB_SOURCE_DIR "${CMAKE_SOURCE_DIR}/third_party/rocksdb")
if(NOT EXISTS "${CEDAR_ROCKSDB_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "Cedar requires the pinned third_party/rocksdb submodule")
endif()

set(CEDAR_ROCKSDB_CACHE_DIR "$ENV{CEDAR_ROCKSDB_CACHE_DIR}"
    CACHE PATH "Directory for reusable pinned RocksDB static libraries")
set(CEDAR_ROCKSDB_CACHE_ROOT "${CEDAR_ROCKSDB_CACHE_DIR}"
    CACHE PATH "Directory for reusable pinned RocksDB static libraries")
set(CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL "1" CACHE STRING
    "Maximum parallel compile jobs for the reusable RocksDB build")
if(NOT CEDAR_ROCKSDB_CACHE_ROOT)
    if(APPLE)
        set(CEDAR_ROCKSDB_CACHE_ROOT "$ENV{HOME}/Library/Caches/Cedar/rocksdb")
    else()
        set(CEDAR_ROCKSDB_CACHE_ROOT "$ENV{HOME}/.cache/cedar/rocksdb")
    endif()
endif()

find_package(Threads REQUIRED)
find_package(Git REQUIRED)
execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${CEDAR_ROCKSDB_SOURCE_DIR}" rev-parse HEAD
    RESULT_VARIABLE CEDAR_ROCKSDB_REVISION_RESULT
    OUTPUT_VARIABLE CEDAR_ROCKSDB_REVISION
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT CEDAR_ROCKSDB_REVISION_RESULT EQUAL 0)
    message(FATAL_ERROR "Cannot determine the pinned RocksDB revision")
endif()

# Cedar carries a small maintained extension to RocksDB.  The source tree is a
# submodule, so the submodule revision alone does not change when this worktree
# has local adapter edits.  Include every tracked and untracked source change
# in the cache key to prevent linking an older static library that lacks the
# Cedar durable-WAL callback API.
execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${CEDAR_ROCKSDB_SOURCE_DIR}" diff --binary HEAD
    RESULT_VARIABLE CEDAR_ROCKSDB_PATCH_RESULT
    OUTPUT_VARIABLE CEDAR_ROCKSDB_PATCH
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT CEDAR_ROCKSDB_PATCH_RESULT EQUAL 0)
    message(FATAL_ERROR "Cannot determine the Cedar RocksDB patch digest")
endif()
execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${CEDAR_ROCKSDB_SOURCE_DIR}" ls-files --others --exclude-standard
    RESULT_VARIABLE CEDAR_ROCKSDB_UNTRACKED_RESULT
    OUTPUT_VARIABLE CEDAR_ROCKSDB_UNTRACKED_FILES
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT CEDAR_ROCKSDB_UNTRACKED_RESULT EQUAL 0)
    message(FATAL_ERROR "Cannot enumerate untracked Cedar RocksDB sources")
endif()
string(REPLACE "\n" ";" CEDAR_ROCKSDB_UNTRACKED_FILES
       "${CEDAR_ROCKSDB_UNTRACKED_FILES}")
foreach(CEDAR_ROCKSDB_UNTRACKED_FILE IN LISTS CEDAR_ROCKSDB_UNTRACKED_FILES)
    if(CEDAR_ROCKSDB_UNTRACKED_FILE STREQUAL "")
        continue()
    endif()
    file(SHA256 "${CEDAR_ROCKSDB_SOURCE_DIR}/${CEDAR_ROCKSDB_UNTRACKED_FILE}"
         CEDAR_ROCKSDB_UNTRACKED_FILE_SHA256)
    string(APPEND CEDAR_ROCKSDB_PATCH
           "|${CEDAR_ROCKSDB_UNTRACKED_FILE}:${CEDAR_ROCKSDB_UNTRACKED_FILE_SHA256}")
endforeach()
string(SHA256 CEDAR_ROCKSDB_PATCH_DIGEST "${CEDAR_ROCKSDB_PATCH}")

set(CEDAR_ROCKSDB_PROFILE "native")
set(CEDAR_ROCKSDB_SANITIZER_FLAG "")
if(CEDAR_ENABLE_ASAN)
    set(CEDAR_ROCKSDB_PROFILE "asan")
    set(CEDAR_ROCKSDB_SANITIZER_FLAG "-fsanitize=address")
elseif(CEDAR_ENABLE_UBSAN)
    set(CEDAR_ROCKSDB_PROFILE "ubsan")
    set(CEDAR_ROCKSDB_SANITIZER_FLAG "-fsanitize=undefined")
elseif(CEDAR_ENABLE_TSAN)
    set(CEDAR_ROCKSDB_PROFILE "tsan")
    set(CEDAR_ROCKSDB_SANITIZER_FLAG "-fsanitize=thread")
endif()

# These inputs deliberately describe the pinned Cedar codec policy and source
# identities. A cache built with a different policy must never be reused.
# Native, sanitizer, and release profiles use optimized RocksDB. A Cedar Debug
# build deliberately keeps RocksDB assertions and SyncPoints so integration
# regressions can exercise real stall/recovery interleavings.
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CEDAR_ROCKSDB_BUILD_TYPE "Debug")
else()
    set(CEDAR_ROCKSDB_BUILD_TYPE "RelWithDebInfo")
endif()
set(CEDAR_ROCKSDB_CODEC_FLAGS
    "WITH_LZ4=ON|WITH_ZSTD=ON|WITH_SNAPPY=OFF|WITH_ZLIB=OFF|WITH_BZ2=OFF")
set(CEDAR_LZ4_SOURCE_DIR
    "${CMAKE_SOURCE_DIR}/third_party/cedar_codecs/lz4")
set(CEDAR_ZSTD_SOURCE_DIR
    "${CMAKE_SOURCE_DIR}/third_party/cedar_codecs/zstd")
if(NOT EXISTS "${CEDAR_LZ4_SOURCE_DIR}/lib/lz4.c" OR
   NOT EXISTS "${CEDAR_ZSTD_SOURCE_DIR}/lib/zstd.h")
    message(FATAL_ERROR "Pinned Cedar LZ4 and Zstd sources are required")
endif()
file(GLOB_RECURSE CEDAR_CODEC_SOURCE_FILES
     "${CEDAR_LZ4_SOURCE_DIR}/lib/*"
     "${CEDAR_ZSTD_SOURCE_DIR}/lib/*")
list(SORT CEDAR_CODEC_SOURCE_FILES)
set(CEDAR_ROCKSDB_CODEC_SOURCES "")
foreach(CEDAR_CODEC_SOURCE IN LISTS CEDAR_CODEC_SOURCE_FILES)
    if(IS_DIRECTORY "${CEDAR_CODEC_SOURCE}")
        continue()
    endif()
    file(SHA256 "${CEDAR_CODEC_SOURCE}" CEDAR_CODEC_SOURCE_SHA256)
    file(RELATIVE_PATH CEDAR_CODEC_SOURCE_RELATIVE
         "${CMAKE_SOURCE_DIR}" "${CEDAR_CODEC_SOURCE}")
    string(APPEND CEDAR_ROCKSDB_CODEC_SOURCES
           "|${CEDAR_CODEC_SOURCE_RELATIVE}:${CEDAR_CODEC_SOURCE_SHA256}")
endforeach()

set(CEDAR_ROCKSDB_TOOLCHAIN_FLAGS "")
string(APPEND CEDAR_ROCKSDB_TOOLCHAIN_FLAGS
       " -DCEDAR_ZSTD_NO_DICTIONARY_TRAINING")
if(APPLE)
    execute_process(COMMAND xcrun --sdk macosx --show-sdk-path
                    OUTPUT_VARIABLE CEDAR_ROCKSDB_SDK_PATH
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(CEDAR_ROCKSDB_SDK_CXX_INCLUDE
        "${CEDAR_ROCKSDB_SDK_PATH}/usr/include/c++/v1")
    if(EXISTS "${CEDAR_ROCKSDB_SDK_CXX_INCLUDE}/cstdint" AND
       NOT EXISTS "/usr/include/c++/v1/cstdint")
        string(APPEND CEDAR_ROCKSDB_TOOLCHAIN_FLAGS
               " -isystem${CEDAR_ROCKSDB_SDK_CXX_INCLUDE}")
    endif()
endif()

string(SHA256 CEDAR_ROCKSDB_FINGERPRINT_INPUT
    "${CEDAR_ROCKSDB_REVISION}|${CEDAR_ROCKSDB_PATCH_DIGEST}|${CEDAR_ROCKSDB_CODEC_SOURCES}|${CEDAR_ROCKSDB_CODEC_FLAGS}|${CEDAR_ROCKSDB_BUILD_TYPE}|${CMAKE_CXX_COMPILER}|${CMAKE_CXX_COMPILER_ID}|${CMAKE_CXX_COMPILER_VERSION}|${CMAKE_SYSTEM_NAME}|${CMAKE_SYSTEM_PROCESSOR}|${CMAKE_OSX_ARCHITECTURES}|${CMAKE_OSX_DEPLOYMENT_TARGET}|${CEDAR_ROCKSDB_PROFILE}")
string(SUBSTRING "${CEDAR_ROCKSDB_FINGERPRINT_INPUT}" 0 20 CEDAR_ROCKSDB_FINGERPRINT)
set(CEDAR_ROCKSDB_PREFIX
    "${CEDAR_ROCKSDB_CACHE_ROOT}/${CEDAR_ROCKSDB_REVISION}-${CEDAR_ROCKSDB_FINGERPRINT}")
set(CEDAR_ROCKSDB_LIBRARY "${CEDAR_ROCKSDB_PREFIX}/lib/librocksdb.a")
set(CEDAR_ROCKSDB_INCLUDE_DIR "${CEDAR_ROCKSDB_PREFIX}/include")
set(CEDAR_ROCKSDB_MANIFEST "${CEDAR_ROCKSDB_PREFIX}/cedar-rocksdb-manifest.txt")
set(CEDAR_ROCKSDB_CODEC_PREFIX "${CEDAR_ROCKSDB_PREFIX}/codecs")
set(CEDAR_ROCKSDB_CODEC_INCLUDE_DIR "${CEDAR_ROCKSDB_CODEC_PREFIX}/include")
set(CEDAR_ROCKSDB_LZ4_LIBRARY "${CEDAR_ROCKSDB_CODEC_PREFIX}/lib/liblz4.a")
set(CEDAR_ROCKSDB_ZSTD_LIBRARY "${CEDAR_ROCKSDB_CODEC_PREFIX}/lib/libzstd.a")

function(cedar_build_pinned_static_library name source_root include_root)
    file(GLOB_RECURSE codec_c_sources "${source_root}/*.c")
    list(SORT codec_c_sources)
    if(codec_c_sources STREQUAL "")
        message(FATAL_ERROR "No C sources found for pinned ${name}")
    endif()
    set(codec_object_dir "${CEDAR_ROCKSDB_CODEC_PREFIX}/obj/${name}")
    file(MAKE_DIRECTORY "${codec_object_dir}")
    set(codec_objects)
    foreach(codec_source IN LISTS codec_c_sources)
        file(RELATIVE_PATH codec_relative "${source_root}" "${codec_source}")
        string(REPLACE "/" "_" codec_object_name "${codec_relative}")
        string(REPLACE ".c" ".o" codec_object_name "${codec_object_name}")
        set(codec_object "${codec_object_dir}/${codec_object_name}")
        execute_process(
            COMMAND "${CMAKE_C_COMPILER}" -O2 -fPIC
                    -I"${include_root}" -I"${include_root}/common"
                    -I"${include_root}/compress" -I"${include_root}/decompress"
                    -c "${codec_source}" -o "${codec_object}"
            RESULT_VARIABLE codec_compile_result)
        if(NOT codec_compile_result EQUAL 0)
            message(FATAL_ERROR "Failed to compile pinned ${name}: ${codec_source}")
        endif()
        list(APPEND codec_objects "${codec_object}")
    endforeach()
    set(codec_library "${CEDAR_ROCKSDB_CODEC_PREFIX}/lib/lib${name}.a")
    file(MAKE_DIRECTORY "${CEDAR_ROCKSDB_CODEC_PREFIX}/lib")
    execute_process(COMMAND "${CMAKE_AR}" qc "${codec_library}" ${codec_objects}
                    RESULT_VARIABLE codec_archive_result)
    if(NOT codec_archive_result EQUAL 0)
        message(FATAL_ERROR "Failed to archive pinned ${name}")
    endif()
    execute_process(COMMAND "${CMAKE_RANLIB}" "${codec_library}"
                    RESULT_VARIABLE codec_ranlib_result)
    if(NOT codec_ranlib_result EQUAL 0)
        message(FATAL_ERROR "Failed to index pinned ${name}")
    endif()
endfunction()

function(cedar_build_pinned_codecs)
    if(EXISTS "${CEDAR_ROCKSDB_LZ4_LIBRARY}" AND
       EXISTS "${CEDAR_ROCKSDB_ZSTD_LIBRARY}")
        return()
    endif()
    file(MAKE_DIRECTORY "${CEDAR_ROCKSDB_CODEC_INCLUDE_DIR}")
    file(COPY "${CEDAR_LZ4_SOURCE_DIR}/lib/lz4.h"
              "${CEDAR_LZ4_SOURCE_DIR}/lib/lz4frame.h"
              "${CEDAR_LZ4_SOURCE_DIR}/lib/lz4hc.h"
         DESTINATION "${CEDAR_ROCKSDB_CODEC_INCLUDE_DIR}")
    file(COPY "${CEDAR_ZSTD_SOURCE_DIR}/lib/zstd.h"
              "${CEDAR_ZSTD_SOURCE_DIR}/lib/zstd_errors.h"
         DESTINATION "${CEDAR_ROCKSDB_CODEC_INCLUDE_DIR}")
    cedar_build_pinned_static_library(lz4 "${CEDAR_LZ4_SOURCE_DIR}/lib"
                                      "${CEDAR_LZ4_SOURCE_DIR}/lib")
    cedar_build_pinned_static_library(zstd "${CEDAR_ZSTD_SOURCE_DIR}/lib"
                                      "${CEDAR_ZSTD_SOURCE_DIR}/lib")
endfunction()

set(CEDAR_ROCKSDB_CACHE_LOCK_HELD OFF)
if(NOT EXISTS "${CEDAR_ROCKSDB_LIBRARY}" OR
   NOT EXISTS "${CEDAR_ROCKSDB_MANIFEST}")
    file(MAKE_DIRECTORY "${CEDAR_ROCKSDB_CACHE_ROOT}")
    file(LOCK "${CEDAR_ROCKSDB_CACHE_ROOT}/.build.lock" GUARD PROCESS TIMEOUT 600)
    set(CEDAR_ROCKSDB_CACHE_LOCK_HELD ON)
endif()
if(NOT EXISTS "${CEDAR_ROCKSDB_LIBRARY}" OR
   NOT EXISTS "${CEDAR_ROCKSDB_MANIFEST}")
    set(CEDAR_ROCKSDB_BUILD_DIR "${CEDAR_ROCKSDB_PREFIX}/build")
    file(MAKE_DIRECTORY "${CEDAR_ROCKSDB_BUILD_DIR}")
    cedar_build_pinned_codecs()
    set(CEDAR_ROCKSDB_CONFIGURE_COMMAND
        "${CMAKE_COMMAND}" -S "${CEDAR_ROCKSDB_SOURCE_DIR}" -B "${CEDAR_ROCKSDB_BUILD_DIR}"
        -DCMAKE_BUILD_TYPE=${CEDAR_ROCKSDB_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${CEDAR_ROCKSDB_PREFIX}
        -DROCKSDB_BUILD_SHARED=OFF
        -DWITH_BENCHMARK=OFF
        -DWITH_BENCHMARK_TOOLS=OFF
        -DWITH_CORE_TOOLS=OFF
        -DWITH_EXAMPLES=OFF
        -DWITH_GFLAGS=OFF
        -DWITH_JEMALLOC=OFF
        -DWITH_JNI=OFF
        -DWITH_LIBURING=OFF
        -DWITH_LZ4=ON
        -DWITH_SNAPPY=OFF
        -DWITH_TESTS=OFF
        -DWITH_TOOLS=OFF
        -DWITH_TRACE_TOOLS=OFF
        -DWITH_ZLIB=OFF
        -DWITH_ZSTD=ON
        -DWITH_BZ2=OFF)
    list(APPEND CEDAR_ROCKSDB_CONFIGURE_COMMAND
        "-Dlz4_ROOT_DIR=${CEDAR_ROCKSDB_CODEC_PREFIX}"
        "-Dzstd_ROOT_DIR=${CEDAR_ROCKSDB_CODEC_PREFIX}")
    if(CMAKE_OSX_ARCHITECTURES)
        list(APPEND CEDAR_ROCKSDB_CONFIGURE_COMMAND
            "-DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}")
    endif()
    if(CMAKE_OSX_DEPLOYMENT_TARGET)
        list(APPEND CEDAR_ROCKSDB_CONFIGURE_COMMAND
            "-DCMAKE_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}")
    endif()
    if(CEDAR_ROCKSDB_SANITIZER_FLAG)
        string(APPEND CEDAR_ROCKSDB_TOOLCHAIN_FLAGS
               " ${CEDAR_ROCKSDB_SANITIZER_FLAG} -fno-omit-frame-pointer")
    endif()
    if(CEDAR_ROCKSDB_TOOLCHAIN_FLAGS)
        list(APPEND CEDAR_ROCKSDB_CONFIGURE_COMMAND
            "-DCMAKE_CXX_FLAGS=${CEDAR_ROCKSDB_TOOLCHAIN_FLAGS}"
            "-DCMAKE_C_FLAGS=${CEDAR_ROCKSDB_TOOLCHAIN_FLAGS}"
            "-DCMAKE_EXE_LINKER_FLAGS=${CEDAR_ROCKSDB_SANITIZER_FLAG}")
    endif()
    if(CEDAR_ENABLE_UBSAN)
        list(APPEND CEDAR_ROCKSDB_CONFIGURE_COMMAND -DWITH_UBSAN=ON)
    endif()

    message(STATUS "Building reusable RocksDB static library (${CEDAR_ROCKSDB_PROFILE}) at ${CEDAR_ROCKSDB_PREFIX}")
    execute_process(COMMAND ${CEDAR_ROCKSDB_CONFIGURE_COMMAND}
                    RESULT_VARIABLE CEDAR_ROCKSDB_CONFIGURE_RESULT)
    if(NOT CEDAR_ROCKSDB_CONFIGURE_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to configure reusable RocksDB static library")
    endif()
    execute_process(COMMAND "${CMAKE_COMMAND}" --build "${CEDAR_ROCKSDB_BUILD_DIR}"
                    --target rocksdb --parallel ${CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL}
                    RESULT_VARIABLE CEDAR_ROCKSDB_BUILD_RESULT)
    if(NOT CEDAR_ROCKSDB_BUILD_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to build reusable RocksDB static library")
    endif()
    execute_process(COMMAND "${CMAKE_COMMAND}" --install "${CEDAR_ROCKSDB_BUILD_DIR}"
                    RESULT_VARIABLE CEDAR_ROCKSDB_INSTALL_RESULT)
    if(NOT CEDAR_ROCKSDB_INSTALL_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to install reusable RocksDB static library")
    endif()
    file(WRITE "${CEDAR_ROCKSDB_MANIFEST}"
         "revision=${CEDAR_ROCKSDB_REVISION}\npatch=${CEDAR_ROCKSDB_PATCH_DIGEST}\ncodec_sources=${CEDAR_ROCKSDB_CODEC_SOURCES}\ncodec_flags=${CEDAR_ROCKSDB_CODEC_FLAGS}\ncodec_linkage=lz4:${CEDAR_ROCKSDB_LZ4_LIBRARY}|zstd:${CEDAR_ROCKSDB_ZSTD_LIBRARY}\nbuild_type=${CEDAR_ROCKSDB_BUILD_TYPE}\nfingerprint=${CEDAR_ROCKSDB_FINGERPRINT}\nlinkage=static\n")
endif()
if(CEDAR_ROCKSDB_CACHE_LOCK_HELD)
    file(LOCK "${CEDAR_ROCKSDB_CACHE_ROOT}/.build.lock" RELEASE)
endif()

if(NOT EXISTS "${CEDAR_ROCKSDB_LIBRARY}" OR
   NOT EXISTS "${CEDAR_ROCKSDB_INCLUDE_DIR}/rocksdb/db.h" OR
   NOT EXISTS "${CEDAR_ROCKSDB_MANIFEST}" OR
   NOT EXISTS "${CEDAR_ROCKSDB_LZ4_LIBRARY}" OR
   NOT EXISTS "${CEDAR_ROCKSDB_ZSTD_LIBRARY}")
    message(FATAL_ERROR "Reusable RocksDB cache is incomplete: ${CEDAR_ROCKSDB_PREFIX}")
endif()

add_library(cedar_rocksdb_static STATIC IMPORTED GLOBAL)
set_target_properties(cedar_rocksdb_static PROPERTIES
    IMPORTED_LOCATION "${CEDAR_ROCKSDB_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${CEDAR_ROCKSDB_INCLUDE_DIR}")

add_library(cedar_rocksdb INTERFACE)
target_link_libraries(cedar_rocksdb INTERFACE cedar_rocksdb_static Threads::Threads
                      "${CEDAR_ROCKSDB_LZ4_LIBRARY}" "${CEDAR_ROCKSDB_ZSTD_LIBRARY}")
target_compile_definitions(cedar_rocksdb INTERFACE
    CEDAR_ROCKSDB_VERSION="11.1.2"
    CEDAR_ROCKSDB_WITH_PRODUCTION_CODECS=1)
