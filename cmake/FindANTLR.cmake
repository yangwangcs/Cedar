# Find ANTLR4
# This module finds antlr4 executable and runtime libraries

find_program(ANTLR_EXECUTABLE NAMES antlr4 antlr4.jar 
    PATHS /usr/local/bin /usr/bin ${CMAKE_SOURCE_DIR}/tools)

if(NOT ANTLR_EXECUTABLE)
    message(WARNING "ANTLR4 executable not found. Cypher support will be disabled.")
    set(ANTLR_FOUND FALSE)
else()
    set(ANTLR_FOUND TRUE)
    message(STATUS "Found ANTLR4: ${ANTLR_EXECUTABLE}")
endif()

# Function to generate parser from grammar
function(antlr4_generate grammar_file namespace output_dir)
    if(NOT ANTLR_FOUND)
        return()
    endif()
    
    get_filename_component(grammar_name ${grammar_file} NAME_WE)
    
    set(${namespace}_SOURCES
        ${output_dir}/${grammar_name}Lexer.cpp
        ${output_dir}/${grammar_name}Parser.cpp
        ${output_dir}/${grammar_name}Visitor.cpp
        ${output_dir}/${grammar_name}BaseVisitor.cpp
    )
    
    set(${namespace}_HEADERS
        ${output_dir}/${grammar_name}Lexer.h
        ${output_dir}/${grammar_name}Parser.h
        ${output_dir}/${grammar_name}Visitor.h
        ${output_dir}/${grammar_name}BaseVisitor.h
    )
    
    add_custom_command(
        OUTPUT ${${namespace}_SOURCES} ${${namespace}_HEADERS}
        COMMAND ${ANTLR_EXECUTABLE} -Dlanguage=Cpp -visitor -listener
                -package ${namespace}
                -o ${output_dir}
                ${grammar_file}
        DEPENDS ${grammar_file}
        COMMENT "Generating ANTLR4 parser for ${grammar_name}"
    )
    
    set(${namespace}_SOURCES ${${namespace}_SOURCES} PARENT_SCOPE)
    set(${namespace}_HEADERS ${${namespace}_HEADERS} PARENT_SCOPE)
endfunction()
