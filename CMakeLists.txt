cmake_minimum_required(VERSION 3.14)
project(krow C)

set(CMAKE_C_STANDARD 11)

find_package(Threads REQUIRED)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR})

if(NOT MSVC)
    add_compile_options(-O3 -march=native -Wall -Wextra -Werror)
    add_definitions(-D_GNU_SOURCE)
else()
    add_compile_options(/O2 /W3 /WX)
endif()

add_executable(krow krow.c libkrow.c)
target_include_directories(krow PRIVATE .)
target_link_libraries(krow PRIVATE Threads::Threads)

if(UNIX)
    target_link_libraries(krow PRIVATE m)
endif()
