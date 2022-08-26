package store

import pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"

const (
	KeyNotFound string = "key not found"
	Empty              = ""
)

type KVResult struct {
	Ok    bool
	Value string
}

type KVStore interface {
	Get(key string) *string
	Put(key string, value string) bool
	Del(key string) bool
	Execute(cmd *pb.Command) KVResult
}