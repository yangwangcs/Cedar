if(NOT DEFINED GENERATOR OR NOT DEFINED TEST_ROOT)
  message(FATAL_ERROR "GENERATOR and TEST_ROOT are required")
endif()

file(REMOVE_RECURSE "${TEST_ROOT}")
file(REMOVE_RECURSE "${TEST_ROOT}-output")
file(MAKE_DIRECTORY "${TEST_ROOT}")

function(run_checked)
  execute_process(
      COMMAND ${ARGV}
      RESULT_VARIABLE result
      OUTPUT_VARIABLE output
      ERROR_VARIABLE error)
  if(NOT result EQUAL 0)
    message(FATAL_ERROR "command failed (${result}): ${ARGV}\n${output}${error}")
  endif()
endfunction()

run_checked(git init "${TEST_ROOT}")
run_checked(git -C "${TEST_ROOT}" config user.email cedar-test@example.invalid)
run_checked(git -C "${TEST_ROOT}" config user.name Cedar-Test)
file(WRITE "${TEST_ROOT}/tracked.txt" "clean\n")
run_checked(git -C "${TEST_ROOT}" add tracked.txt)
run_checked(git -C "${TEST_ROOT}" commit -m initial)

set(output_header "${TEST_ROOT}-output/build_provenance.h")
run_checked(${CMAKE_COMMAND}
    -DSOURCE_DIR=${TEST_ROOT}
    -DOUTPUT_FILE=${output_header}
    -P ${GENERATOR})
file(READ "${output_header}" clean_header)
if(NOT clean_header MATCHES "#define CEDAR_SOURCE_DIRTY 0")
  message(FATAL_ERROR "clean repository was not recorded as clean")
endif()
string(REGEX MATCH
    "#define CEDAR_SOURCE_COMMIT \"([0-9a-fA-F]+)\""
    commit_match "${clean_header}")
string(LENGTH "${CMAKE_MATCH_1}" commit_length)
if(NOT commit_match OR NOT commit_length EQUAL 40)
  message(FATAL_ERROR "clean repository did not record a source commit")
endif()

file(WRITE "${TEST_ROOT}/untracked.txt" "untracked\n")
run_checked(${CMAKE_COMMAND}
    -DSOURCE_DIR=${TEST_ROOT}
    -DOUTPUT_FILE=${output_header}
    -P ${GENERATOR})
file(READ "${output_header}" untracked_header)
if(NOT untracked_header MATCHES "#define CEDAR_SOURCE_DIRTY 1")
  message(FATAL_ERROR "untracked content was not recorded as dirty")
endif()

run_checked(git -C "${TEST_ROOT}" add untracked.txt)
run_checked(${CMAKE_COMMAND}
    -DSOURCE_DIR=${TEST_ROOT}
    -DOUTPUT_FILE=${output_header}
    -P ${GENERATOR})
file(READ "${output_header}" staged_header)
if(NOT staged_header MATCHES "#define CEDAR_SOURCE_DIRTY 1")
  message(FATAL_ERROR "staged content was not recorded as dirty")
endif()
