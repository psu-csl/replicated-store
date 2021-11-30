package store

import (
	"errors"
	"sync"
)

type Store struct {
	store sync.Map
}

func NewStore() *Store {
	s := Store{
		store: sync.Map{},
	}
	return &s
}

func (s *Store) Get(key string) (string, error) {
	if val, ok := s.store.Load(key); ok {
		return val.(string), nil
	} else {
		return "", errors.New("item not found")
	}
}

func (s *Store) Put(key string, val string) error {
	s.store.Store(key, val)
	return nil
}

func (s *Store) Del(key string) error {
	if _, ok := s.store.Load(key); ok {
		s.store.Delete(key)
		return nil
	} else {
		return errors.New("item not found")
	}
}
