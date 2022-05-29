package store

import (
	"github.com/psu-csl/replicated-store/go/command"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	key1 string = "foo"
	val1 string = "bar"
	key2 string = "baz"
	val2 string = "qux"
)

func TestMemKVStore_GetPutDel(t *testing.T) {
	store := NewMemKVStore()

	// Get a non-exist key
	assert.Nil(t, store.Get(key1), "Expect key %v to be nil, but not", key1)

	//Delete a non-exist key
	assert.False(t, store.Del(key1), "Expect Del key %v to be false, but true", key1)

	// Put then Get
	assert.True(t, store.Put(key1, val1))
	actualVal := *store.Get(key1)
	assert.Equal(t, val1, actualVal, "Expect %v, but got %v", val1, actualVal)

	assert.True(t, store.Put(key2, val2))
	actualVal = *store.Get(key2)
	assert.Equal(t, val2, actualVal, "Expect %v, but got %v", val2, actualVal)

	// Update an existing key
	assert.True(t, store.Put(key1, val2))
	actualVal = *store.Get(key1)
	assert.Equal(t, val2, actualVal, "Expect %v, but got %v", val1, actualVal)
	actualVal = *store.Get(key2)
	assert.Equal(t, val2, actualVal, "Expect %v, but got %v", val2, actualVal)

	// Delete an existing key
	assert.True(t, store.Del(key1), "Expect Del key %v to be true, but false", key1)
	assert.Nil(t, store.Get(key1), "Expect key %v to be nil, but not", key1)
	actualVal = *store.Get(key2)
	assert.Equal(t, val2, actualVal, "Expect %v, but got %v", val2, actualVal)

	assert.True(t, store.Del(key2), "Expect Del key %v to be true, but false", key1)
	assert.Nil(t, store.Get(key1), "Expect key %v to be nil, but not", key1)
	assert.Nil(t, store.Get(key2), "Expect key %v to be nil, but not", key2)
}


func TestMemKVStore_Execute(t *testing.T) {
	store := NewMemKVStore()

	// Get command for a non-exist key
	{
		getCmd := command.Command{Key: key1, Value: "", Type: command.Get}
		r := store.Execute(getCmd)
		assert.True(t, !r.Ok && r.Value == KeyNotFound)
	}

	// Delete command for a non-exist key
	{
		delCmd := command.Command{Key: key1, Value: "", Type: command.Del}
		r := store.Execute(delCmd)
		assert.True(t, !r.Ok && r.Value == KeyNotFound)
	}

	// Put command then get command
	{
		putCmd := command.Command{Key: key1, Value: val1, Type: command.Put}
		r := store.Execute(putCmd)
		assert.True(t, r.Ok && r.Value == Empty)
		getCmd := command.Command{Key: key1, Value: "", Type: command.Get}
		r = store.Execute(getCmd)
		assert.True(t, r.Ok && r.Value == val1)
	}

	{
		putCmd := command.Command{Key: key2, Value: val2, Type: command.Put}
		r := store.Execute(putCmd)
		assert.True(t, r.Ok && r.Value == Empty)
		getCmd := command.Command{Key: key2, Value: "", Type: command.Get}
		r = store.Execute(getCmd)
		assert.True(t, r.Ok && r.Value == val2)
	}

	// Put command with the same key but a different value
	{
		putCmd := command.Command{Key: key1, Value: val2, Type: command.Put}
		r := store.Execute(putCmd)
		assert.True(t, r.Ok && r.Value == Empty)

		getCmd1 := command.Command{Key: key1, Value: "", Type: command.Get}
		r = store.Execute(getCmd1)
		assert.True(t, r.Ok && r.Value == val2)

		getCmd2 := command.Command{Key: key2, Value: "", Type: command.Get}
		r = store.Execute(getCmd2)
		assert.True(t, r.Ok && r.Value == val2)
	}

	// Delete command for an existing key
	{
		delCmd := command.Command{Key: key1, Value: "", Type: command.Del}
		r := store.Execute(delCmd)
		assert.True(t, r.Ok && r.Value == Empty)

		getCmd1 := command.Command{Key: key1, Value: "", Type: command.Get}
		r = store.Execute(getCmd1)
		assert.False(t, r.Ok)
		assert.Equal(t, KeyNotFound, r.Value, "Expect key not found, but got %v", r.Value)

		getCmd2 := command.Command{Key: key2, Value: "", Type: command.Get}
		r = store.Execute(getCmd2)
		assert.True(t, r.Ok && r.Value == val2)
	}
}
