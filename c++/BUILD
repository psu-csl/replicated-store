load("@com_github_grpc_grpc//bazel:grpc_build_system.bzl", "grpc_proto_library")
load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

cc_library(
    name = "memkvstore",
    srcs = ["memkvstore.cc"],
    hdrs = ["memkvstore.h", "kvstore.h"],
    deps = [
        ":multipaxos_cc_proto",
        "@com_github_google_glog//:glog",
    ],
)

cc_library(
    name = "log",
    srcs = ["log.cc"],
    hdrs = ["log.h"],
    deps = [
        ":memkvstore",
        "@com_github_google_glog//:glog",
    ],
)

cc_library(
    name = "test_util",
    srcs = ["test_util.cc"],
    hdrs = ["test_util.h"],
    deps = [
        ":multipaxos_cc_proto",
    ],
)

proto_library(
    name = "multipaxos_proto",
    srcs = ["multipaxos.proto"],
)

cc_proto_library(
    name = "multipaxos_cc_proto",
    deps = [":multipaxos_proto"],
)

cc_grpc_library(
    name = "multipaxos_cc_grpc",
    srcs = [":multipaxos_proto"],
    grpc_only = True,
    deps = [":multipaxos_cc_proto"],
)

cc_library(
    name = "multipaxos",
    srcs = ["multipaxos.cc"],
    hdrs = ["multipaxos.h", "json.h", "json_fwd.h"],
    deps = [
        ":memkvstore",
        ":log",
        ":multipaxos_cc_grpc",
        "@com_github_google_glog//:glog",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_binary(
    name = "replicant",
    srcs = ["main.cc", "replicant.cc", "replicant.h"],
    deps = [
        ":memkvstore",
        ":log",
        ":multipaxos_cc_grpc",
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
        ":test_util",
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
