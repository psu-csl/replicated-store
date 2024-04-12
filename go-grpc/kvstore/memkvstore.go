package kvstore

import (
	"bytes"
	"encoding/gob"
	logger "github.com/sirupsen/logrus"
)

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

func (s *MemKVStore) Close() {}

func (s *MemKVStore) MakeSnapshot() ([]byte, error) {
	buffer := &bytes.Buffer{}
	err := gob.NewEncoder(buffer).Encode(s.store)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (s *MemKVStore) RestoreSnapshot(data []byte) {
	buffer := bytes.NewBuffer(data)
	err := gob.NewDecoder(buffer).Decode(&s.store)
	if err != nil {
		logger.Error(err)
		return
	}
}
