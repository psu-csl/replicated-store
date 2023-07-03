Replicated store in Go with tcp communication.

## Build

1. Install rocksdb
2. Build replicant \
```
go build -o bin/replicant main/main.go
```

## Run
./bin/replicant -id <u>ID</u> -c <u>PATH-TO-CONFIG</u> \
```
./bin/replicant -id 0 -c ../c++/config.json
```