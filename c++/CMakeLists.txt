cmake_minimum_required(VERSION 3.16)

project(replicant CXX)

find_program(CCACHE ccache)
if(CCACHE)
  message("using ccache")
  set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE})
else()
  message("ccache not found")
endif()

include(cmake/common.cmake)

# Proto file
get_filename_component(proto "paxosrpc.proto" ABSOLUTE)
get_filename_component(proto_path "${proto}" PATH)

# Generated sources
set(proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/paxosrpc.pb.cc")
set(proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/paxosrpc.pb.h")
set(grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/paxosrpc.grpc.pb.cc")
set(grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/paxosrpc.grpc.pb.h")
add_custom_command(
      OUTPUT "${proto_srcs}" "${proto_hdrs}" "${grpc_srcs}" "${grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${proto}"
      DEPENDS "${proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

add_library(grpc_proto
  ${grpc_srcs}
  ${grpc_hdrs}
  ${proto_srcs}
  ${proto_hdrs})
target_link_libraries(grpc_proto
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

set(CMAKE_CXX_STANDARD 17)
add_compile_options(-Wall -Wextra -pedantic -Werror -g -ggdb -fsanitize=thread)
add_executable(replicant replicant.cc paxos.cc paxosrpc.cc memstore.cc main.cc)

add_link_options(-fsanitize=thread)

find_package(glog 0.6.0 REQUIRED)
find_package(gflags REQUIRED)

target_link_options(replicant PRIVATE -fsanitize=thread)

target_link_libraries(replicant
  gflags
  glog::glog
  grpc_proto
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# tests
include(FetchContent)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
FetchContent_MakeAvailable(googletest)
enable_testing()

add_executable(
  log_test
  log_test.cc
)
target_link_libraries(
  log_test
  gtest_main
)

include(GoogleTest)
gtest_discover_tests(log_test)
