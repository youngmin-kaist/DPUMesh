cmake_minimum_required(VERSION 3.16)
project(host_pb_ser_bench CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS hello.proto)

add_executable(host_pb_ser_bench
    host_pb_ser_bench.cc
    ${PROTO_SRCS}
    ${PROTO_HDRS}
)

target_include_directories(host_pb_ser_bench PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_options(host_pb_ser_bench PRIVATE
    -O3
    -DNDEBUG
    -march=native
    -Wall
    -Wextra
)

target_link_libraries(host_pb_ser_bench PRIVATE
    protobuf::libprotobuf
)