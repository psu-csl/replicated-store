package store

import (
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
)

const (
	KeyNotFound string = "key not found"
	Empty              = ""
)

type Result struct {
	Ok    bool
	Value string
}

type MemKVStore struct {
	store map[string]string
}

func NewMemKVStore() *MemKVStore {
	s := MemKVStore{
		store: make(map[string]string),
	}
	return &s
}

func (s *MemKVStore) Get(key string) *string {
	if value, ok := s.store[key]; ok {
		return &value
	} else {
		return nil
	}
}

func (s *MemKVStore) Put(key string, value string) bool {
	s.store[key] = value
	return true
}

func (s *MemKVStore) Del(key string) bool {
	if _, ok := s.store[key]; ok {
		delete(s.store, key)
		return true
	} else {
		return false
	}
}

func (s *MemKVStore) Execute(cmd *pb.Command) Result {
	if cmd.Type == pb.CommandType_GET {
		value := s.Get(cmd.Key)
		if value != nil {
			return Result{Ok: true, Value: *value}
		} else {
			return Result{Ok: false, Value: KeyNotFound}
		}
	}

	if cmd.Type == pb.CommandType_PUT {
		s.Put(cmd.Key, cmd.Value)
		return Result{Ok: true, Value: Empty}
	}

	if cmd.Type != pb.CommandType_DEL {
		panic("Command type not Del")
	}

	if s.Del(cmd.Key) {
		return Result{Ok: true, Value: Empty}
	}
	return Result{Ok: false, Value: KeyNotFound}
}
