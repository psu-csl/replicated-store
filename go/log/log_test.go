package log

import (
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	"github.com/psu-csl/replicated-store/go/consensus/multipaxos/util"
	"github.com/psu-csl/replicated-store/go/store"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	log1   *Log
	store1 *store.MemKVStore
)

func setup() {
	log1 = NewLog()
	store1 = store.NewMemKVStore()
}

func TestConstructor(t *testing.T) {
	setup()
	assert.EqualValues(t, 0, log1.LastExecuted())
	assert.EqualValues(t, 0, log1.GlobalLastExecuted())
	assert.False(t, log1.IsExecutable())
	assert.Nil(t, log1.log[0])
	assert.Nil(t, log1.log[3])
	assert.Nil(t, log1.log[-1])
}

func TestInsert(t *testing.T) {
	instanceLog := make(map[int64]*pb.Instance)
	var (
		index int64 = 1
		ballot int64 = 1
	)
	assert.True(t, Insert(instanceLog, util.MakeInstanceWithType(ballot, index,
		pb.CommandType_PUT)))
	assert.Equal(t, pb.CommandType_PUT, instanceLog[index].GetCommand().GetType())
	assert.False(t, Insert(instanceLog, util.MakeInstanceWithType(ballot, index,
		pb.CommandType_PUT)))
}

func TestInsertUpdateInProgress(t *testing.T) {
	instanceLog := make(map[int64]*pb.Instance)
	var (
		index      int64 = 1
		loBallot   int64 = 1
		highBallot int64 = 2
	)
	assert.True(t, Insert(instanceLog, util.MakeInstanceWithType(loBallot,
		index, pb.CommandType_PUT)))
	assert.Equal(t, pb.CommandType_PUT, instanceLog[index].GetCommand().GetType())
	assert.False(t, Insert(instanceLog, util.MakeInstanceWithType(highBallot,
		index, pb.CommandType_DEL)))
	assert.Equal(t, pb.CommandType_DEL, instanceLog[index].GetCommand().GetType())
}

func TestInsertUpdateCommitted(t *testing.T) {
	instanceLog := make(map[int64]*pb.Instance)
	var (
		index int64 = 1
		ballot int64 = 1
	)
	assert.True(t, Insert(instanceLog, util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_COMMITTED, pb.CommandType_PUT)))
	assert.False(t, Insert(instanceLog, util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_INPROGRESS, pb.CommandType_PUT)))
}

func TestInsertStale(t *testing.T) {
	instanceLog := make(map[int64]*pb.Instance)
	var (
		index      int64 = 1
		loBallot   int64 = 1
		highBallot int64 = 2
	)
	assert.True(t, Insert(instanceLog, util.MakeInstanceWithType(highBallot,
		index, pb.CommandType_PUT)))
	assert.Equal(t, pb.CommandType_PUT, instanceLog[index].GetCommand().GetType())
	assert.False(t, Insert(instanceLog, util.MakeInstanceWithType(loBallot,
		index, pb.CommandType_DEL)))
	assert.Equal(t, pb.CommandType_PUT, instanceLog[index].GetCommand().GetType())
}

func TestInsertCase2Committed(t *testing.T) {
	instanceLog := make(map[int64]*pb.Instance)
	var (
		index int64 = 1
		ballot int64 = 1
	)
	assert.True(t, Insert(instanceLog, util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_COMMITTED, pb.CommandType_PUT)))
	defer expectDeath(t, "Insert case2")
	assert.False(t, Insert(instanceLog, util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_INPROGRESS, pb.CommandType_DEL)))
}

func TestInsertCase2Executed(t *testing.T) {
	instanceLog := make(map[int64]*pb.Instance)
	var (
		index int64 = 1
		ballot int64 = 1
	)
	assert.True(t, Insert(instanceLog, util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_EXECUTED, pb.CommandType_PUT)))
	defer expectDeath(t, "Insert case2")
	assert.False(t, Insert(instanceLog, util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_INPROGRESS, pb.CommandType_DEL)))
}

func TestInsertCase3(t *testing.T) {
	instanceLog := make(map[int64]*pb.Instance)
	var (
		index int64 = 1
		ballot int64 = 1
	)
	assert.True(t, Insert(instanceLog, util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_INPROGRESS, pb.CommandType_PUT)))
	defer expectDeath(t, "Insert case3")
	assert.False(t, Insert(instanceLog, util.MakeInstanceWithAll(ballot, index,
		pb.InstanceState_INPROGRESS, pb.CommandType_DEL)))
}

func TestAppend(t *testing.T) {
	setup()
	log1.Append(util.MakeInstance(0, log1.AdvanceLastIndex()))
	log1.Append(util.MakeInstance(0, log1.AdvanceLastIndex()))
	assert.Equal(t, int64(1), log1.log[1].GetIndex())
	assert.Equal(t, int64(2), log1.log[2].GetIndex())
}

func TestAppendWithGap(t *testing.T) {
	setup()

	var index int64 = 42
	log1.Append(util.MakeInstance(0, index))
	assert.Equal(t, index, log1.log[index].GetIndex())
	assert.Equal(t, index + 1, log1.AdvanceLastIndex())
}

func TestAppendFillGaps(t *testing.T) {
	setup()

	var index int64 = 42
	log1.Append(util.MakeInstance(0, index))
	log1.Append(util.MakeInstance(0, index - 10))
	assert.Equal(t, index + 1, log1.AdvanceLastIndex())
}

func TestAppendHighBallotOverride(t *testing.T) {
	setup()

	var (
		index int64 = 1
		loBallot int64 = 0
		hiBallot int64 = 1
	)
	log1.Append(util.MakeInstanceWithType(loBallot, index, pb.CommandType_PUT))
	log1.Append(util.MakeInstanceWithType(hiBallot, index, pb.CommandType_DEL))
	assert.Equal(t, pb.CommandType_DEL, log1.log[index].GetCommand().Type)
}

func TestAppendLowBallotNoEffect(t *testing.T)  {
	setup()

	var (
		index int64 = 1
		loBallot int64 = 0
		hiBallot int64 = 1
	)
	log1.Append(util.MakeInstanceWithType(hiBallot, index, pb.CommandType_PUT))
	log1.Append(util.MakeInstanceWithType(loBallot, index, pb.CommandType_DEL))
	assert.Equal(t, pb.CommandType_PUT, log1.log[index].GetCommand().Type)
}

func TestCommit(t *testing.T) {
	setup()

	var index1 int64 = 1
	log1.Append(util.MakeInstance(0, index1))
	var index2 int64 = 2
	log1.Append(util.MakeInstance(0, index2))
	assert.True(t, log1.log[index1].GetState() == pb.InstanceState_INPROGRESS)
	assert.True(t, log1.log[index2].GetState() == pb.InstanceState_INPROGRESS)
	assert.False(t, log1.IsExecutable())

	log1.Commit(index2)
	assert.True(t, log1.log[index1].GetState() == pb.InstanceState_INPROGRESS)
	assert.True(t, log1.log[index2].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log1.IsExecutable())

	log1.Commit(index1)
	assert.True(t, log1.log[index1].GetState() == pb.InstanceState_COMMITTED)
	assert.True(t, log1.log[index2].GetState() == pb.InstanceState_COMMITTED)
	assert.True(t, log1.IsExecutable())
}

func TestCommitBeforeAppend(t *testing.T) {
	setup()

	var index1 int64 = 1
	var wg sync.WaitGroup
	wg.Add(1)
	// Do commit first
	go func(wg *sync.WaitGroup) {
		log1.Commit(index1)
		wg.Done()
	}(&wg)
	// Give sufficient time to run the go routine
	time.Sleep(50 * time.Millisecond)
	log1.Append(util.MakeInstance(0, log1.AdvanceLastIndex()))
	wg.Wait()
	assert.True(t, log1.log[index1].GetState() == pb.InstanceState_COMMITTED)
}

func TestAppendCommitExecute(t *testing.T) {
	setup()
	var index int64 = 1
	var done int64 = 0
	var wg sync.WaitGroup
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		for atomic.LoadInt64(&done) != 1 {
			log1.Execute(store1)
		}
		wg.Done()
	}(&wg)
	time.Sleep(50 * time.Millisecond)

	log1.Append(util.MakeInstance(0, index))
	atomic.AddInt64(&done, 1)
	log1.Commit(index)
	wg.Wait()

	assert.True(t, log1.log[index].GetState() == pb.InstanceState_EXECUTED)
	assert.Equal(t, index, log1.LastExecuted())
}

func TestAppendCommitExecuteOutOfOrder(t *testing.T) {
	setup()

	const (
		index1 int64 = iota + 1
		index2
		index3
	)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		log1.Execute(store1)
		log1.Execute(store1)
		log1.Execute(store1)
		wg.Done()
	}()

	log1.Append(util.MakeInstance(0, index1))
	log1.Append(util.MakeInstance(0, index2))
	log1.Append(util.MakeInstance(0, index3))

	log1.Commit(index3)
	log1.Commit(index2)
	log1.Commit(index1)

	wg.Wait()

	assert.True(t, log1.log[index1].GetState() == pb.InstanceState_EXECUTED)
	assert.True(t, log1.log[index2].GetState() == pb.InstanceState_EXECUTED)
	assert.True(t, log1.log[index3].GetState() == pb.InstanceState_EXECUTED)
	assert.Equal(t, index3, log1.LastExecuted())
}

func TestCommitUntil(t *testing.T) {
	setup()
	const (
		ballot int64 = iota
		index1
		index2
		index3
	)

	log1.Append(util.MakeInstance(ballot, index1))
	log1.Append(util.MakeInstance(ballot, index2))
	log1.Append(util.MakeInstance(ballot, index3))

	log1.CommitUntil(index2, ballot)

	assert.True(t, log1.log[index1].GetState() == pb.InstanceState_COMMITTED)
	assert.True(t, log1.log[index2].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log1.log[index3].GetState() == pb.InstanceState_COMMITTED)
	assert.True(t, log1.IsExecutable())

	log1.CommitUntil(index3, ballot)
	assert.True(t, log1.log[index3].GetState() == pb.InstanceState_COMMITTED)
	assert.True(t, log1.IsExecutable())
}

func TestCommitUntilHigherBallot(t *testing.T) {
	setup()
	const (
		ballot int64 = iota
		index1
		index2
		index3
	)

	log1.Append(util.MakeInstance(ballot, index1))
	log1.Append(util.MakeInstance(ballot, index2))
	log1.Append(util.MakeInstance(ballot, index3))

	log1.CommitUntil(index3, ballot+1)

	assert.False(t, log1.log[index1].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log1.log[index2].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log1.log[index3].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log1.IsExecutable())
}

func TestCommitUntilCase2(t *testing.T) {
	setup()
	const (
		index1 int64 = iota
		index2
		index3
		ballot
	)

	log1.Append(util.MakeInstance(ballot, index1))
	log1.Append(util.MakeInstance(ballot, index2))
	log1.Append(util.MakeInstance(ballot, index3))

	defer expectDeath(t, "Commit until test2 - no panic")
	log1.CommitUntil(index3, ballot-1)
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

	log1.Append(util.MakeInstance(ballot, index1))
	log1.Append(util.MakeInstance(ballot, index3))
	log1.Append(util.MakeInstance(ballot, index4))

	log1.CommitUntil(index4, ballot)

	assert.True(t, log1.log[index1].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log1.log[index3].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log1.log[index4].GetState() == pb.InstanceState_COMMITTED)
	assert.True(t, log1.IsExecutable())
}

func TestAppendCommitUntilExecute(t *testing.T) {
	setup()
	const (
		ballot int64 = iota
		index1
		index2
		index3
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		log1.Execute(store1)
		log1.Execute(store1)
		log1.Execute(store1)
		wg.Done()
	}()

	log1.Append(util.MakeInstance(ballot, index1))
	log1.Append(util.MakeInstance(ballot, index2))
	log1.Append(util.MakeInstance(ballot, index3))
	log1.CommitUntil(index3, ballot)
	wg.Wait()

	assert.True(t, log1.log[index1].GetState() == pb.InstanceState_EXECUTED)
	assert.True(t, log1.log[index2].GetState() == pb.InstanceState_EXECUTED)
	assert.True(t, log1.log[index3].GetState() == pb.InstanceState_EXECUTED)
	assert.Equal(t, index3, log1.LastExecuted())
	assert.False(t, log1.IsExecutable())
}

func TestAppendCommitUntilExecuteTrimUntil(t *testing.T) {
	setup()
	const (
		ballot int64 = iota
		index1
		index2
		index3
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		log1.Execute(store1)
		log1.Execute(store1)
		log1.Execute(store1)
		wg.Done()
	}()

	log1.Append(util.MakeInstance(ballot, index1))
	log1.Append(util.MakeInstance(ballot, index2))
	log1.Append(util.MakeInstance(ballot, index3))
	log1.CommitUntil(index3, ballot)
	wg.Wait()

	log1.TrimUntil(index3)

	assert.Nil(t, log1.log[index1])
	assert.Nil(t, log1.log[index2])
	assert.Nil(t, log1.log[index3])
	assert.Equal(t, index3, log1.LastExecuted())
	assert.Equal(t, index3, log1.GlobalLastExecuted())
	assert.False(t, log1.IsExecutable())
}

func TestAppendAtTrimmedIndex(t *testing.T) {
	setup()
	const (
		ballot int64 = iota
		index1
		index2
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		log1.Execute(store1)
		log1.Execute(store1)
		wg.Done()
	}()

	log1.Append(util.MakeInstance(ballot, index1))
	log1.Append(util.MakeInstance(ballot, index2))
	log1.CommitUntil(index2, ballot)
	wg.Wait()

	log1.TrimUntil(index2)

	assert.Nil(t, log1.log[index1])
	assert.Nil(t, log1.log[index2])
	assert.Equal(t, index2, log1.LastExecuted())
	assert.Equal(t, index2, log1.GlobalLastExecuted())
	assert.False(t, log1.IsExecutable())

	log1.Append(util.MakeInstance(ballot, index1))
	log1.Append(util.MakeInstance(ballot, index2))
	assert.Nil(t, log1.log[index1])
	assert.Nil(t, log1.log[index2])
}

func TestLog_InstancesSinceGlobalLastExecuted(t *testing.T) {
	setup()
	var ballot int64 = 0
	const(
		index1 int64 = iota + 1
		index2
		index3
	)

	var wg sync.WaitGroup

	expect := make([]*pb.Instance, 0)
	assert.Equal(t, expect, log1.InstancesSinceGlobalLastExecuted())

	expect = append(expect, util.MakeInstance(ballot, index1))
	log1.Append(util.MakeInstance(ballot, index1))
	expect = append(expect, util.MakeInstance(ballot, index2))
	log1.Append(util.MakeInstance(ballot, index2))
	expect = append(expect, util.MakeInstance(ballot, index3))
	log1.Append(util.MakeInstance(ballot, index3))

	instances := log1.InstancesSinceGlobalLastExecuted()
	assert.Equal(t, expect, instances)

	var index int64 = 2
	log1.CommitUntil(index, ballot)
	assert.Equal(t, expect, instances)

	wg.Add(1)
	go func() {
		log1.Execute(store1)
		log1.Execute(store1)
		wg.Done()
	}()
	wg.Wait()

	log1.TrimUntil(index)
	expect = expect[index:]
	assert.Equal(t, expect, log1.InstancesSinceGlobalLastExecuted())
}

func expectDeath(t *testing.T, msg string) {
	if r := recover(); r == nil {
		t.Errorf(msg)
	}
}