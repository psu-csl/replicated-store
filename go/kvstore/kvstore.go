package kvstore

import (
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/network"
	logger "github.com/sirupsen/logrus"
)

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
	Close()
}

func CreateStore(config config.Config) KVStore {
	if config.Store == "rocksdb" {
		return NewRocksDBKVStore(config.DbPath)
	} else if config.Store == "mem" {
		return NewMemKVStore()
	} else {
		logger.Panic("no match kvstore")
		return nil
	}
}

func Execute(cmd *pb.Command, store KVStore) KVResult {
	if cmd.Type == pb.Get {
		value := store.Get(cmd.Key)
		if value != nil {
			return KVResult{Ok: true, Value: *value}
		} else {
			return KVResult{Ok: false, Value: NotFound}
		}
	}

	if cmd.Type == pb.Put {
		if store.Put(cmd.Key, cmd.Value) {
			return KVResult{Ok: true, Value: Empty}
		}
		return KVResult{Ok: false, Value: NotFound}
	}

	if cmd.Type != pb.Del {
		panic("Command type not Del")
	}

	if store.Del(cmd.Key) {
		return KVResult{Ok: true, Value: Empty}
	}
	return KVResult{Ok: false, Value: NotFound}
}
