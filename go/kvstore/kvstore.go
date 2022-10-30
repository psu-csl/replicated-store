package kvstore

import pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"

const (
	NotFound string = "key not found"
	Empty           = ""
)

type KVResult struct {
	Ok    bool
	Value string
}

type KVStore interface {
	Get(key string) *string
	Put(key string, value string) bool
	Del(key string) bool
}

func Execute(cmd *pb.Command, store KVStore) KVResult {
	if cmd.Type == pb.CommandType_GET {
		value := store.Get(cmd.Key)
		if value != nil {
			return KVResult{Ok: true, Value: *value}
		} else {
			return KVResult{Ok: false, Value: NotFound}
		}
	}

	if cmd.Type == pb.CommandType_PUT {
		if store.Put(cmd.Key, cmd.Value) {
			return KVResult{Ok: true, Value: Empty}
		}
		return KVResult{Ok: false, Value: NotFound}
	}

	if cmd.Type != pb.CommandType_DEL {
		panic("Command type not Del")
	}

	if store.Del(cmd.Key) {
		return KVResult{Ok: true, Value: Empty}
	}
	return KVResult{Ok: false, Value: NotFound}
}
