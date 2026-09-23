if(NOT DEFINED CEDAR_RADIX_FORMAT_FIXTURE)
  message(FATAL_ERROR "CEDAR_RADIX_FORMAT_FIXTURE is required")
endif()

foreach(args IN ITEMS "" "unknown;/tmp/cedar-radix-fixture-invalid"
                      "verify;relative-path")
  execute_process(
      COMMAND "${CEDAR_RADIX_FORMAT_FIXTURE}" ${args}
      RESULT_VARIABLE result
      OUTPUT_QUIET ERROR_QUIET)
  if(result EQUAL 0)
    message(FATAL_ERROR "invalid CLI invocation succeeded: ${args}")
  endif()
endforeach()
