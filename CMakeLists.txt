cmake_minimum_required(VERSION 3.20)

project(EventStreamLab LANGUAGES CXX)

add_library(esl_common STATIC
    src/common/clock.cpp
    src/common/cli.cpp
    src/common/protocol.cpp
    src/common/socket.cpp
    src/common/workload.cpp
)

target_include_directories(esl_common
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_features(esl_common PUBLIC cxx_std_20)

if(WIN32)
    target_link_libraries(esl_common PUBLIC ws2_32)
endif()

add_executable(esl_producer
    src/producer/main.cpp
)
target_link_libraries(esl_producer PRIVATE esl_common)

add_executable(esl_server
    src/server/main.cpp
)
target_link_libraries(esl_server PRIVATE esl_common)

add_executable(esl_consumer
    src/consumer/main.cpp
)
target_link_libraries(esl_consumer PRIVATE esl_common)

add_executable(esl_protocol_test
    tests/protocol_test.cpp
)
target_link_libraries(esl_protocol_test PRIVATE esl_common)

add_executable(esl_clock_test
    tests/clock_test.cpp
)
target_link_libraries(esl_clock_test PRIVATE esl_common)

add_executable(esl_workload_test
    tests/workload_test.cpp
)
target_link_libraries(esl_workload_test PRIVATE esl_common)

add_executable(esl_cli_test
    tests/cli_test.cpp
)
target_link_libraries(esl_cli_test PRIVATE esl_common)

find_package(Threads REQUIRED)
add_executable(esl_socket_io_test
    tests/socket_io_test.cpp
)
target_link_libraries(esl_socket_io_test PRIVATE esl_common Threads::Threads)

enable_testing()
add_test(NAME protocol_test COMMAND esl_protocol_test)
add_test(NAME clock_test COMMAND esl_clock_test)
add_test(NAME workload_test COMMAND esl_workload_test)
add_test(NAME cli_test COMMAND esl_cli_test)
add_test(NAME socket_io_test COMMAND esl_socket_io_test)
