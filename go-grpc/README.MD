Replicated store in Go with grpc communication.

## Build

1. Install protocol buffer compiler, rocksdb
2. Compile protocol buffer
```
mkdir -p multipaxos/comm

protoc --go_out=multipaxos/comm --go_opt=paths=source_relative \
    --go-grpc_out=multipaxos/comm --go-grpc_opt=paths=source_relative \
    ./multipaxosrpc.proto 
```
3. Build replicant
```
CGO_CFLAGS="-I/path/to/rocksdb/include" \
CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd" \
go build -o bin/replicant main/main.go
```
or if prerequisites are in linker paths \
```
go build -o bin/replicant main/main.go
```

## Run
./bin/replicant -id <u>ID</u> -c <u>PATH-TO-CONFIG</u> \
```
./bin/replicant -id 0 -c ../c++/config.json
```