cc_library(
    name = "kvstore",
    srcs = ["kvstore.cc"],
    hdrs = ["kvstore.h", "json.h", "json_fwd.h", "memkvstore.h", "rocksdbstore.h", "msg.h"],
    deps = [
        "@com_github_google_glog//:glog",
    ],
)

cc_library(
    name = "memkvstore",
    srcs = ["memkvstore.cc"],
    hdrs = ["memkvstore.h"],
    deps = [
        ":kvstore",
        "@com_github_google_glog//:glog",
    ],
)

cc_library(
    name = "rocksdbstore",
    srcs = ["rocksdbstore.cc"],
    hdrs = ["rocksdbstore.h"],
    deps = [
        ":kvstore",
        "@com_github_google_glog//:glog",
        "@librocksdb//:librocksdb",
    ]
)

cc_library(
    name = "log",
    srcs = ["log.cc"],
    hdrs = ["log.h"],
    deps = [
        ":memkvstore",
        ":rocksdbstore",
        "@com_github_google_glog//:glog",
    ],
)

cc_library(
    name = "test_util",
    srcs = ["test_util.cc"],
    hdrs = ["test_util.h", "msg.h", "json.h"],
    deps = [
        "@com_github_google_glog//:glog",
    ],
)


cc_library(
    name = "multipaxos",
    srcs = ["multipaxos.cc", "tcp.cc"],
    hdrs = ["multipaxos.h", "json.h", "json_fwd.h", "tcp.h", "atomicops.h", "readerwriterqueue.h"],
    deps = [
        ":memkvstore",
        ":rocksdbstore",
        ":log",
        "@com_github_google_glog//:glog",
    ],
)

cc_library(
    name = "client_manager",
    srcs = ["client_manager.cc", "client.cc"],
    hdrs = ["client_manager.h", "client.h"],
    deps = [
        ":multipaxos",
        "@com_github_google_glog//:glog",
    ],
)

cc_binary(
    name = "replicant",
    srcs = ["main.cc", "replicant.cc", "replicant.h", "peer_server.h", "peer_server.cc"],
    deps = [
        ":memkvstore",
        ":rocksdbstore",
        ":log",
        ":multipaxos",
        ":client_manager",
        "@com_github_google_glog//:glog",
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
    srcs = ["multipaxos.cc", "multipaxos_test.cc", "peer_server.h", "peer_server.cc",],
    deps = [
        ":client_manager",
        ":multipaxos",
        ":test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "replicant_test",
    size = "small",
    srcs = ["replicant.cc", "replicant_test.cc"],
    deps = [
        ":client_manager",
        ":replicant",
        ":multipaxos",
        ":test_util",
        ":log",
        "@com_google_googletest//:gtest_main",
    ],
)
