# ─────────────────────────────────────────────────────────────────────────────
# Compiler Warnings Configuration
# ─────────────────────────────────────────────────────────────────────────────

function(entropy_set_compiler_warnings)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options(
            -Wall
            -Wextra
            -Wpedantic
            -Wshadow
            -Wnon-virtual-dtor
            -Wold-style-cast
            -Wcast-align
            -Wunused
            -Woverloaded-virtual
            -Wconversion
            -Wsign-conversion
            -Wnull-dereference
            -Wdouble-promotion
            -Wformat=2
            -Wimplicit-fallthrough
        )

        if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
            add_compile_options(
                -Wmisleading-indentation
                -Wduplicated-cond
                -Wduplicated-branches
                -Wlogical-op
                -Wuseless-cast
            )
        endif()

        # Treat warnings as errors in CI
        if(DEFINED ENV{CI})
            add_compile_options(-Werror)
        endif()

    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
        add_compile_options(
            /W4
            /permissive-
            /w14640   # thread un-safe static member initialization
            /w14826   # conversion from 'type1' to 'type2' is sign-extended
            /w14254   # operator: conversion from 'type1' to 'type2', possible loss of data
            /w14263   # member function does not override any base class virtual member function
            /w14265   # class has virtual functions, but destructor is not virtual
            /w14287   # unsigned/negative constant mismatch
            /w14296   # expression is always true/false
            /w14311   # pointer truncation from 'type' to 'type'
            /w14545   # expression before comma evaluates to a function which is missing an argument list
            /w14546   # function call before comma missing argument list
            /w14547   # operator before comma has no effect; expected operator with side-effect
            /w14549   # operator before comma has no effect; did you intend 'operator'?
            /w14555   # expression has no effect; expected expression with side-effect
            /w14619   # pragma warning: there is no warning number 'number'
            /w14640   # thread un-safe static member initialization
            /w14826   # conversion is sign-extended
            /w14905   # wide string literal cast to 'LPSTR'
            /w14906   # string literal cast to 'LPWSTR'
            /w14928   # illegal copy-initialization; more than one user-defined conversion has been implicitly applied
        )

        # Treat warnings as errors in CI
        if(DEFINED ENV{CI})
            add_compile_options(/WX)
        endif()
    endif()
endfunction()

# Function to disable warnings for third-party targets
function(entropy_disable_warnings target)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${target} PRIVATE -w)
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
        target_compile_options(${target} PRIVATE /W0)
    endif()
endfunction()
