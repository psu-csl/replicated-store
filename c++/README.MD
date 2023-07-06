Replicated store in C++ with grpc communication.

## Build

1. Install [protocol buffers](https://protobuf.dev/overview/).
2. Install [rocksdb](https://github.com/facebook/rocksdb/blob/master/INSTALL.md).
3. Build replicant
```
bazel build --config rel :replicant
```
or replace `rel` with `dbg` for a debug build.

## Run
./bazel-bin/replicant -id <u>ID</u> -config-path <u>PATH_TO_CONFIG_FILE</u> 
-num-threads <u>NUMBER_OF_ASIO_THREADS</u>

- <u>NUMBER_OF_ASIO_THREADS</u>: number of threads for running asio callbacks
```
./bazel-bin/replicant -id 0 -config-path config.json -num-threads 64
```