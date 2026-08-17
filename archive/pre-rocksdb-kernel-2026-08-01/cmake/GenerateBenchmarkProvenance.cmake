if(NOT DEFINED SOURCE_DIR OR NOT DEFINED OUTPUT_FILE)
  message(FATAL_ERROR "SOURCE_DIR and OUTPUT_FILE are required")
endif()

if(NOT DEFINED GIT_EXECUTABLE OR GIT_EXECUTABLE STREQUAL "")
  set(GIT_EXECUTABLE git)
endif()

execute_process(
    COMMAND "${GIT_EXECUTABLE}" rev-parse --verify HEAD
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE commit_result
    OUTPUT_VARIABLE source_commit
    ERROR_QUIET
    OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(
    COMMAND "${GIT_EXECUTABLE}" status --porcelain --untracked-files=normal
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE status_result
    OUTPUT_VARIABLE worktree_status
    ERROR_QUIET
    OUTPUT_STRIP_TRAILING_WHITESPACE)

string(LENGTH "${source_commit}" commit_length)
if(NOT commit_result EQUAL 0 OR NOT commit_length EQUAL 40 OR
   NOT source_commit MATCHES "^[0-9a-fA-F]+$")
  set(source_commit "unknown")
  set(source_dirty 1)
elseif(NOT status_result EQUAL 0 OR NOT worktree_status STREQUAL "")
  set(source_dirty 1)
else()
  set(source_dirty 0)
endif()

get_filename_component(output_directory "${OUTPUT_FILE}" DIRECTORY)
file(MAKE_DIRECTORY "${output_directory}")
set(temporary_file "${OUTPUT_FILE}.tmp")
file(WRITE "${temporary_file}"
    "#ifndef CEDAR_BENCHMARK_BUILD_PROVENANCE_H_\n"
    "#define CEDAR_BENCHMARK_BUILD_PROVENANCE_H_\n"
    "#define CEDAR_SOURCE_COMMIT \"${source_commit}\"\n"
    "#define CEDAR_SOURCE_DIRTY ${source_dirty}\n"
    "#endif  // CEDAR_BENCHMARK_BUILD_PROVENANCE_H_\n")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E copy_if_different
            "${temporary_file}" "${OUTPUT_FILE}"
    RESULT_VARIABLE copy_result)
file(REMOVE "${temporary_file}")
if(NOT copy_result EQUAL 0)
  message(FATAL_ERROR "failed to publish benchmark provenance header")
endif()
