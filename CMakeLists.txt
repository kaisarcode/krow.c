cmake_minimum_required(VERSION 3.14)
project(krow C)

set(CMAKE_C_STANDARD 11)

# Output binary to the root directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR})

# Performance optimizations
if(NOT MSVC)
    add_compile_options(-O3 -march=native)
    add_definitions(-D_GNU_SOURCE)
else()
    add_compile_options(/O2)
endif()

# Main executable
add_executable(krow 
    krow.c 
    libkrow.c
)

# Include directories
target_include_directories(krow PRIVATE .)

# System libraries
if(UNIX)
    target_link_libraries(krow pthread m)
endif()
