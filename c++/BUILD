load("@com_github_grpc_grpc//bazel:grpc_build_system.bzl", "grpc_proto_library")

cc_library(
    name = "command",
    srcs = ["command.cc"],
    hdrs = ["command.h"],
)

cc_library(
    name = "instance",
    srcs = ["instance.cc"],
    hdrs = ["instance.h"],
    deps = [
        ":command",
    ],
)

cc_library(
    name = "memkvstore",
    srcs = ["memkvstore.cc"],
    hdrs = ["memkvstore.h", "kvstore.h"],
    deps = [
        ":command",
        "@com_github_google_glog//:glog",
    ],
)

cc_library(
    name = "log",
    srcs = ["log.cc"],
    hdrs = ["log.h"],
    deps = [
        ":command",
        ":instance",
        ":memkvstore",
        "@com_github_google_glog//:glog",
    ],
)

grpc_proto_library(
    name = "multipaxosrpc",
    srcs = ["multipaxos.proto"],
)

cc_library(
    name = "multipaxos",
    srcs = ["multipaxos.cc"],
    hdrs = ["multipaxos.h", "json.h", "json_fwd.h"],
    deps = [
        ":command",
        ":instance",
        ":memkvstore",
        ":log",
        ":multipaxosrpc",
        "@com_github_google_glog//:glog",
    ],
)

cc_binary(
    name = "replicant",
    srcs = ["main.cc", "replicant.cc", "replicant.h"],
    deps = [
        ":command",
        ":instance",
        ":memkvstore",
        ":log",
        ":multipaxosrpc",
        ":multipaxos",
        "@com_github_google_glog//:glog",
        "@com_github_grpc_grpc//:grpc++",
    ],

)

cc_test(
    name = "log_test",
    size = "small",
    srcs = ["log_test.cc"],
    deps = [
        ":log",
        "@com_google_googletest//:gtest_main"
    ],
)

cc_test(
    name = "memkvstore_test",
    size = "small",
    srcs = ["memkvstore_test.cc"],
    deps = [
        ":memkvstore",
        "@com_google_googletest//:gtest_main"
    ],
)

cc_test(
    name = "multipaxos_test",
    size = "small",
    srcs = ["multipaxos.cc", "multipaxos_test.cc"],
    deps = [
        ":multipaxos",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
    ],
)
