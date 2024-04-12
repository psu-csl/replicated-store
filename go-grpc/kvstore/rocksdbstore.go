package kvstore

import (
	"bytes"
	"github.com/linxGnu/grocksdb"
	logger "github.com/sirupsen/logrus"
)

type RocksDBStore struct {
	db *grocksdb.DB
	ro *grocksdb.ReadOptions
	wo *grocksdb.WriteOptions
}

func NewRocksDBKVStore(path string) *RocksDBStore {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		logger.Panic(err)
	}
	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	return &RocksDBStore{
		db: db,
		ro: ro,
		wo: wo,
	}
}

func (s *RocksDBStore) Get(key string) *string {
	if valueSlice, err := s.db.Get(s.ro, []byte(key)); err == nil {
		value := string(valueSlice.Data())
		return &value
	} else {
		return nil
	}
}

func (s *RocksDBStore) Put(key string, value string) bool {
	if err := s.db.Put(s.wo, []byte(key), []byte(value)); err == nil {
		return true
	} else {
		return false
	}
}

func (s *RocksDBStore) Del(key string) bool {
	if err := s.db.Delete(s.wo, []byte(key)); err == nil {
		return true
	} else {
		return false
	}
}

func (s *RocksDBStore) Close() {
	s.db.Close()
}

func (s *RocksDBStore) MakeSnapshot() ([]byte, error) {
	buffer := bytes.Buffer{}
	return buffer.Bytes(), nil
}

func (s *RocksDBStore) RestoreSnapshot([]byte) {}
