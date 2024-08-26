cmake_minimum_required(VERSION 3.22)
cmake_policy(VERSION 3.22...3.30)
project(crt-http-benchmark CXX)

add_executable(${PROJECT_NAME} main.cpp)

install(TARGETS ${PROJECT_NAME}
    DESTINATION ${CMAKE_INSTALL_BINDIR})

set_target_properties(${PROJECT_NAME} PROPERTIES
    CXX_STANDARD 20)

if(NOT MSVC)
    target_compile_options(${PROJECT_NAME} PRIVATE "-fno-exceptions;-fno-rtti")
endif()

find_package(aws-c-http REQUIRED)

target_link_libraries(${PROJECT_NAME} AWS::aws-c-http)
