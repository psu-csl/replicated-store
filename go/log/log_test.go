package log

import (
	"github.com/psu-csl/replicated-store/go/kvstore"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
	"github.com/psu-csl/replicated-store/go/util"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

var (
	log     *Log
	kvStore *kvstore.MemKVStore
)

func setup() {
	kvStore = kvstore.NewMemKVStore()
	log = NewLog(kvStore)
}

func TestConstructor(t *testing.T) {
	setup()
	assert.EqualValues(t, 0, log.LastExecuted())
	assert.EqualValues(t, 0, log.GlobalLastExecuted())
	assert.False(t, log.IsExecutable())
	assert.Nil(t, log.At(0))
	assert.Nil(t, log.At(-1))
	assert.Nil(t, log.At(3))
}

func TestInsert(t *testing.T) {
	log := make(map[int64]*pb.Instance)
	var (
		index int64 = 1
		ballot int64 = 1
	)
	assert.True(t, Insert(log, util.MakeInstanceWithType(ballot, index,
		pb.CommandType_PUT)))
	assert.Equal(t, pb.CommandType_PUT, log[index].GetCommand().GetType())
	assert.False(t, Insert(log, util.MakeInstanceWithType(ballot, index,
		pb.CommandType_PUT)))
}

func TestInsertUpdateInProgress(t *testing.T) {
	log := make(map[int64]*pb.Instance)
	var (
		index  int64 = 1
		ballot int64 = 1
	)
	assert.True(t, Insert(log, util.MakeInstanceWithType(ballot,
		index, pb.CommandType_PUT)))
	assert.Equal(t, pb.CommandType_PUT, log[index].GetCommand().GetType())
	assert.False(t, Insert(log, util.MakeInstanceWithType(ballot+ 1,
		index, pb.CommandType_DEL)))
	assert.Equal(t, pb.CommandType_DEL, log[index].GetCommand().GetType())
}

func TestInsertUpdateCommitted(t *testing.T) {
	log := make(map[int64]*pb.Instance)
	var (
		index int64 = 1
		ballot int64 = 1
	)
	assert.True(t, Insert(log, util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_COMMITTED, pb.CommandType_PUT)))
	assert.False(t, Insert(log, util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_INPROGRESS, pb.CommandType_PUT)))
}

func TestInsertStale(t *testing.T) {
	log := make(map[int64]*pb.Instance)
	var (
		index  int64 = 1
		ballot int64 = 2
	)
	assert.True(t, Insert(log, util.MakeInstanceWithType(ballot,
		index, pb.CommandType_PUT)))
	assert.Equal(t, pb.CommandType_PUT, log[index].GetCommand().GetType())
	assert.False(t, Insert(log, util.MakeInstanceWithType(ballot - 1,
		index, pb.CommandType_DEL)))
	assert.Equal(t, pb.CommandType_PUT, log[index].GetCommand().GetType())
}

func TestInsertCase2Committed(t *testing.T) {
	log := make(map[int64]*pb.Instance)
	var (
		index int64 = 1
		ballot int64 = 0
	)
	inst1 := util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_COMMITTED, pb.CommandType_PUT)
	inst2 := util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_INPROGRESS, pb.CommandType_DEL)
	assert.True(t, Insert(log, inst1))
	defer expectDeath(t, "Insert case2")
	assert.False(t, Insert(log, inst2))
}

func TestInsertCase2Executed(t *testing.T) {
	log := make(map[int64]*pb.Instance)
	var (
		index int64 = 1
		ballot int64 = 0
	)
	inst1 := util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_EXECUTED, pb.CommandType_PUT)
	inst2 := util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_INPROGRESS, pb.CommandType_DEL)
	assert.True(t, Insert(log, inst1))
	defer expectDeath(t, "Insert case2")
	assert.False(t, Insert(log, inst2))
}

func TestInsertCase3(t *testing.T) {
	log := make(map[int64]*pb.Instance)
	var (
		index int64 = 1
		ballot int64 = 0
	)
	inst1 := util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_INPROGRESS, pb.CommandType_PUT)
	inst2 := util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_INPROGRESS, pb.CommandType_DEL)
	assert.True(t, Insert(log, inst1))
	defer expectDeath(t, "Insert case3")
	assert.False(t, Insert(log, inst2))
}

func TestAppend(t *testing.T) {
	setup()
	log.Append(util.MakeInstance(0, log.AdvanceLastIndex()))
	log.Append(util.MakeInstance(0, log.AdvanceLastIndex()))
	assert.Equal(t, int64(1), log.At(1).GetIndex())
	assert.Equal(t, int64(2), log.At(2).GetIndex())
}

func TestAppendWithGap(t *testing.T) {
	setup()

	var index int64 = 42
	log.Append(util.MakeInstance(0, index))
	assert.Equal(t, index, log.At(index).GetIndex())
	assert.Equal(t, index + 1, log.AdvanceLastIndex())
}

func TestAppendFillGaps(t *testing.T) {
	setup()

	var index int64 = 42
	log.Append(util.MakeInstance(0, index))
	log.Append(util.MakeInstance(0, index - 10))
	assert.Equal(t, index + 1, log.AdvanceLastIndex())
}

func TestAppendHighBallotOverride(t *testing.T) {
	setup()

	var (
		index int64 = 1
		loBallot int64 = 0
		hiBallot int64 = 1
	)
	log.Append(util.MakeInstanceWithType(loBallot, index, pb.CommandType_PUT))
	log.Append(util.MakeInstanceWithType(hiBallot, index, pb.CommandType_DEL))
	assert.Equal(t, pb.CommandType_DEL, log.At(index).GetCommand().Type)
}

func TestAppendLowBallotNoEffect(t *testing.T)  {
	setup()

	var (
		index int64 = 1
		loBallot int64 = 0
		hiBallot int64 = 1
	)
	log.Append(util.MakeInstanceWithType(hiBallot, index, pb.CommandType_PUT))
	log.Append(util.MakeInstanceWithType(loBallot, index, pb.CommandType_DEL))
	assert.Equal(t, pb.CommandType_PUT, log.At(index).GetCommand().Type)
}

func TestCommit(t *testing.T) {
	setup()

	var index1 int64 = 1
	log.Append(util.MakeInstance(0, index1))
	var index2 int64 = 2
	log.Append(util.MakeInstance(0, index2))
	assert.True(t, IsInProgress(log.At(index1)))
	assert.True(t, IsInProgress(log.At(index2)))
	assert.False(t, log.IsExecutable())

	log.Commit(index2)
	assert.True(t, IsInProgress(log.At(index1)))
	assert.True(t, IsCommitted(log.At(index2)))
	assert.False(t, log.IsExecutable())

	log.Commit(index1)
	assert.True(t, IsCommitted(log.At(index1)))
	assert.True(t, IsCommitted(log.At(index2)))
	assert.True(t, log.IsExecutable())
}

func TestCommitBeforeAppend(t *testing.T) {
	setup()

	var index1 int64 = 1
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		log.Commit(index1)
		wg.Done()
	}(&wg)
	time.Sleep(50 * time.Millisecond)
	
	log.Append(util.MakeInstance(0, log.AdvanceLastIndex()))
	wg.Wait()
	assert.True(t, IsCommitted(log.At(index1)))
}

func TestAppendCommitExecute(t *testing.T) {
	setup()
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		log.Execute()
		wg.Done()
	}(&wg)

	var index int64 = 1
	log.Append(util.MakeInstance(0, index))
	log.Commit(index)
	wg.Wait()

	assert.True(t, IsExecuted(log.At(index)))
	assert.Equal(t, index, log.LastExecuted())
}

func TestAppendCommitExecuteOutOfOrder(t *testing.T) {
	setup()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		log.Execute()
		log.Execute()
		log.Execute()
		wg.Done()
	}()

	const (
		index1 int64 = iota + 1
		index2
		index3
	)
	log.Append(util.MakeInstance(0, index1))
	log.Append(util.MakeInstance(0, index2))
	log.Append(util.MakeInstance(0, index3))

	log.Commit(index3)
	log.Commit(index2)
	log.Commit(index1)

	wg.Wait()

	assert.True(t, IsExecuted(log.At(index1)))
	assert.True(t, IsExecuted(log.At(index2)))
	assert.True(t, IsExecuted(log.At(index3)))
	assert.Equal(t, index3, log.LastExecuted())
}

func TestCommitUntil(t *testing.T) {
	setup()
	const (
		ballot int64 = iota
		index1
		index2
		index3
	)

	log.Append(util.MakeInstance(ballot, index1))
	log.Append(util.MakeInstance(ballot, index2))
	log.Append(util.MakeInstance(ballot, index3))

	log.CommitUntil(index2, ballot)

	assert.True(t, IsCommitted(log.At(index1)))
	assert.True(t, IsCommitted(log.At(index2)))
	assert.False(t, IsCommitted(log.At(index3)))
	assert.True(t, log.IsExecutable())

	log.CommitUntil(index3, ballot)
	assert.True(t, IsCommitted(log.At(index3)))
	assert.True(t, log.IsExecutable())
}

func TestCommitUntilHigherBallot(t *testing.T) {
	setup()
	const (
		ballot int64 = iota
		index1
		index2
		index3
	)

	log.Append(util.MakeInstance(ballot, index1))
	log.Append(util.MakeInstance(ballot, index2))
	log.Append(util.MakeInstance(ballot, index3))

	log.CommitUntil(index3, ballot + 1)

	assert.False(t, IsCommitted(log.At(index1)))
	assert.False(t, IsCommitted(log.At(index2)))
	assert.False(t, IsCommitted(log.At(index3)))
	assert.False(t, log.IsExecutable())
}

func TestCommitUntilCase2(t *testing.T) {
	setup()
	const (
		index1 int64 = iota
		index2
		index3
		ballot
	)

	log.Append(util.MakeInstance(ballot, index1))
	log.Append(util.MakeInstance(ballot, index2))
	log.Append(util.MakeInstance(ballot, index3))

	defer expectDeath(t, "Commit until test2 - no panic")
	log.CommitUntil(index3, ballot - 1)
}

func TestCommitUntilWithGap(t *testing.T) {
	setup()
	const (
		ballot int64 = iota
		index1
		_
		index3
		index4
	)

	log.Append(util.MakeInstance(ballot, index1))
	log.Append(util.MakeInstance(ballot, index3))
	log.Append(util.MakeInstance(ballot, index4))

	log.CommitUntil(index4, ballot)

	assert.True(t, IsCommitted(log.At(index1)))
	assert.False(t, IsCommitted(log.At(index3)))
	assert.False(t, IsCommitted(log.At(index4)))
	assert.True(t, log.IsExecutable())
}

func TestAppendCommitUntilExecute(t *testing.T) {
	setup()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		log.Execute()
		log.Execute()
		log.Execute()
		wg.Done()
	}()

	const (
		ballot int64 = iota
		index1
		index2
		index3
	)
	log.Append(util.MakeInstance(ballot, index1))
	log.Append(util.MakeInstance(ballot, index2))
	log.Append(util.MakeInstance(ballot, index3))
	log.CommitUntil(index3, ballot)
	wg.Wait()

	assert.True(t, IsExecuted(log.At(index1)))
	assert.True(t, IsExecuted(log.At(index2)))
	assert.True(t, IsExecuted(log.At(index3)))
	assert.False(t, log.IsExecutable())
}

func TestAppendCommitUntilExecuteTrimUntil(t *testing.T) {
	setup()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		log.Execute()
		log.Execute()
		log.Execute()
		wg.Done()
	}()

	const (
		ballot int64 = iota
		index1
		index2
		index3
	)
	log.Append(util.MakeInstance(ballot, index1))
	log.Append(util.MakeInstance(ballot, index2))
	log.Append(util.MakeInstance(ballot, index3))
	log.CommitUntil(index3, ballot)
	wg.Wait()

	log.TrimUntil(index3)

	assert.Nil(t, log.At(index1))
	assert.Nil(t, log.At(index2))
	assert.Nil(t, log.At(index3))
	assert.Equal(t, index3, log.LastExecuted())
	assert.Equal(t, index3, log.GlobalLastExecuted())
	assert.False(t, log.IsExecutable())
}

func TestAppendAtTrimmedIndex(t *testing.T) {
	setup()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		log.Execute()
		log.Execute()
		wg.Done()
	}()

	const (
		ballot int64 = iota
		index1
		index2
	)
	log.Append(util.MakeInstance(ballot, index1))
	log.Append(util.MakeInstance(ballot, index2))
	log.CommitUntil(index2, ballot)
	wg.Wait()

	log.TrimUntil(index2)

	assert.Nil(t, log.At(index1))
	assert.Nil(t, log.At(index2))
	assert.Equal(t, index2, log.LastExecuted())
	assert.Equal(t, index2, log.GlobalLastExecuted())
	assert.False(t, log.IsExecutable())

	log.Append(util.MakeInstance(ballot, index1))
	log.Append(util.MakeInstance(ballot, index2))
	assert.Nil(t, log.At(index1))
	assert.Nil(t, log.At(index2))
}

func TestInstances(t *testing.T) {
	setup()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		log.Execute()
		log.Execute()
		wg.Done()
	}()

	var ballot int64 = 0
	expect := make([]*pb.Instance, 0, 3)
	assert.Equal(t, expect, log.Instances())

	index1 := log.AdvanceLastIndex()
	expect = append(expect, util.MakeInstance(ballot, index1))
	log.Append(util.MakeInstance(ballot, index1))
	index2 := log.AdvanceLastIndex()
	expect = append(expect, util.MakeInstance(ballot, index2))
	log.Append(util.MakeInstance(ballot, index2))
	index3 := log.AdvanceLastIndex()
	expect = append(expect, util.MakeInstance(ballot, index3))
	log.Append(util.MakeInstance(ballot, index3))

	instances := log.Instances()
	for index, instance := range expect {
		assert.True(t, IsEqualInstance(instance, instances[index]))
	}

	var index int64 = 2
	log.CommitUntil(index, ballot)
	wg.Wait()
	log.TrimUntil(index)

	expect = expect[index:]
	instances = log.Instances()
	for index, instance := range expect {
		assert.True(t, IsEqualInstance(instance, instances[index]))
	}
}

func TestCallingStopUnblocksExecutor(t *testing.T) {
	setup()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, result := log.Execute()
		assert.Nil(t, result)
		wg.Done()
	}()
	log.Stop()
	wg.Wait()
}

func expectDeath(t *testing.T, msg string) {
	if r := recover(); r == nil {
		t.Errorf(msg)
	}
}