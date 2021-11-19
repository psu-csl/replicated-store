package store

import (
	"errors"
	"github.com/psu-csl/replicated-store/go/operation"
	"sync"
)

type Store struct {
	store        sync.Map
}

func NewStore() *Store {
	s := Store{
		store: sync.Map{},
	}
	return &s
}

func (s *Store) ApplyCommand(cmd operation.Command) operation.CommandResult {
	result := operation.CommandResult{
		CommandID:    cmd.CommandID,
		IsSuccess:    true,
		Value:        "",
		Error:        "",
	}
	switch cmd.Type {
	case "Put":
		err := s.put(cmd.Key, cmd.Value)
		if err != nil {
			result.IsSuccess = false
			result.Error = err.Error()
		}
		return result
	case "Get":
		val, err := s.get(cmd.Key)
		result.Value = val
		if err != nil {
			result.IsSuccess = false
			result.Error = err.Error()
		}
		return result
	case "Delete":
		err := s.delete(cmd.Key)
		if err != nil {
			result.IsSuccess = false
			result.Error = err.Error()
		}
		return result
	default:
		result.IsSuccess = false
		result.Error ="command type not found"
		return result
	}
}

func (s *Store) get(key string) (string, error) {
	if val, ok := s.store.Load(key); ok {
		return val.(string), nil
	} else {
		return "", errors.New("item not found")
	}
}

func (s *Store) put(key string, val string) error {
	s.store.Store(key, val)
	return nil
}

func (s *Store) delete(key string) error {
	if _, ok := s.store.Load(key); ok {
		s.store.Delete(key)
		return nil
	} else {
		return errors.New("item not found")
	}
}