package kvstore

import (
	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
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

	assert.Nil(t, store.Get(key1))

	assert.False(t, store.Del(key1))

	assert.True(t, store.Put(key1, val1))
	actualVal := *store.Get(key1)
	assert.Equal(t, val1, actualVal)

	assert.True(t, store.Put(key2, val2))
	actualVal = *store.Get(key2)
	assert.Equal(t, val2, actualVal)

	assert.True(t, store.Put(key1, val2))
	actualVal = *store.Get(key1)
	assert.Equal(t, val2, actualVal)
	actualVal = *store.Get(key2)
	assert.Equal(t, val2, actualVal)

	assert.True(t, store.Del(key1))
	assert.Nil(t, store.Get(key1))
	actualVal = *store.Get(key2)
	assert.Equal(t, val2, actualVal)

	assert.True(t, store.Del(key2))
	assert.Nil(t, store.Get(key1))
	assert.Nil(t, store.Get(key2))
}


func TestMemKVStore_Execute(t *testing.T) {
	store := NewMemKVStore()
	getKey1 := &pb.Command{Key: key1, Value: "", Type: pb.CommandType_GET}
	getKey2 := &pb.Command{Key: key2, Value: "", Type: pb.CommandType_GET}
	delKey1 := &pb.Command{Key: key1, Value: "", Type: pb.CommandType_DEL}
	putKey1Val1 := &pb.Command{Key: key1, Value: val1, Type: pb.CommandType_PUT}
	putKey2Val2 := &pb.Command{Key: key2, Value: val2, Type: pb.CommandType_PUT}
	putKey1Val2 := &pb.Command{Key: key1, Value: val2, Type: pb.CommandType_PUT}

	{
		r := Execute(getKey1, store)
		assert.True(t, !r.Ok && r.Value == NotFound)
	}

	{
		r := Execute(delKey1, store)
		assert.True(t, !r.Ok && r.Value == NotFound)
	}

	{
		r1 := Execute(putKey1Val1, store)
		assert.True(t, r1.Ok && r1.Value == Empty)
		r2 := Execute(getKey1, store)
		assert.True(t, r2.Ok && r2.Value == val1)
	}

	{
		r1 := Execute(putKey2Val2, store)
		assert.True(t, r1.Ok && r1.Value == Empty)
		r2 := Execute(getKey2, store)
		assert.True(t, r2.Ok && r2.Value == val2)
	}

	{
		r1 := Execute(putKey1Val2, store)
		assert.True(t, r1.Ok && r1.Value == Empty)

		r2 := Execute(getKey1, store)
		assert.True(t, r2.Ok && r2.Value == val2)

		r3 := Execute(getKey2, store)
		assert.True(t, r3.Ok && r3.Value == val2)
	}

	{
		r1 := Execute(delKey1, store)
		assert.True(t, r1.Ok && r1.Value == Empty)

		r2 := Execute(getKey1, store)
		assert.False(t, r2.Ok)
		assert.Equal(t, NotFound, r2.Value)

		r3 := Execute(getKey2, store)
		assert.True(t, r3.Ok && r3.Value == val2)
	}
}
