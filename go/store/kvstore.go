package store

import (
	"errors"
	"github.com/psu-csl/replicated-store/go/operation"
)

type Store struct {
	store        map[string]string
}

func NewStore() *Store {
	s := Store{
		store: make(map[string]string),
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
	if val, ok := s.store[key]; ok {
		return val, nil
	} else {
		return "", errors.New("item not found")
	}
}

func (s *Store) put(key string, val string) error {
	s.store[key] = val
	return nil
}

func (s *Store) delete(key string) error {
	if _, ok := s.store[key]; ok {
		delete(s.store, key)
		return nil
	} else {
		return errors.New("item not found")
	}
}