package log

import (
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	"github.com/psu-csl/replicated-store/go/store"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	log_   *Log
	store_ *store.MemKVStore
)

func setup() {
	log_ = NewLog()
	store_ = store.NewMemKVStore()
}

func makeInstance(ballot int64) *pb.Instance {
	return &pb.Instance{Ballot: ballot, Command: &pb.Command{},
		Index: log_.AdvanceLastIndex(), State: pb.InstanceState_INPROGRESS, ClientId: 0}
}

func makeInstanceByIndex(ballot int64, index int64) *pb.Instance {
	return &pb.Instance{Ballot: ballot, Command: &pb.Command{},
		Index: index, State: pb.InstanceState_INPROGRESS, ClientId: 0}
}

func makeInstanceByState(ballot int64, state pb.InstanceState) *pb.Instance {
	return &pb.Instance{Ballot: ballot, Command: &pb.Command{},
		Index: log_.AdvanceLastIndex(), State: state, ClientId: 0}
}

func makeInstanceByIndexAndType(ballot int64, index int64,
	cmdType pb.CommandType) *pb.Instance {
	return &pb.Instance{Ballot: ballot, Command: &pb.Command{Type: cmdType},
		Index: index, State: pb.InstanceState_INPROGRESS, ClientId: 0}
}

func makeInstanceByAll(ballot int64, index int64, state pb.InstanceState,
	cmdType pb.CommandType) *pb.Instance {
	return &pb.Instance{Ballot: ballot, Command: &pb.Command{Type: cmdType},
		Index: index, State: state, ClientId: 0}
}

func TestConstructor(t *testing.T) {
	setup()
	const zero = int64(0)
	assert.Equal(t, zero, log_.LastExecuted(), "expected 0, but got %v\n",
		log_.LastExecuted())
	assert.Equal(t, zero, log_.GlobalLastExecuted())
	assert.False(t, log_.IsExecutable())
	assert.Nil(t, log_.log[0])
	assert.Nil(t, log_.log[3])
	assert.Nil(t, log_.log[-1])
}

func TestAppend(t *testing.T) {
	setup()
	log_.Append(makeInstance(0))
	log_.Append(makeInstance(0))
	assert.Equal(t, int64(1), log_.log[1].GetIndex())
	assert.Equal(t, int64(2), log_.log[2].GetIndex())
}

func TestAppendWithGap(t *testing.T) {
	setup()

	var index int64 = 42
	log_.Append(makeInstanceByIndex(0, index))
	assert.Equal(t, index, log_.log[index].GetIndex())
	assert.Equal(t, index + 1, log_.AdvanceLastIndex())
}

func TestAppendFillGaps(t *testing.T) {
	setup()

	var index int64 = 42
	log_.Append(makeInstanceByIndex(0, index))
	log_.Append(makeInstanceByIndex(0, index - 10))
	assert.Equal(t, index + 1, log_.AdvanceLastIndex())
}

func TestAppendHighBallotOverride(t *testing.T) {
	setup()

	var (
		index int64 = 1
		loBallot int64 = 0
		hiBallot int64 = 1
	)
	log_.Append(makeInstanceByIndexAndType(loBallot, index, pb.CommandType_PUT))
	log_.Append(makeInstanceByIndexAndType(hiBallot, index, pb.CommandType_DEL))
	assert.Equal(t, pb.CommandType_DEL, log_.log[index].GetCommand().Type)
}

func TestAppendLowBallotNoEffect(t *testing.T)  {
	setup()

	var (
		index int64 = 1
		loBallot int64 = 0
		hiBallot int64 = 1
	)
	log_.Append(makeInstanceByIndexAndType(hiBallot, index, pb.CommandType_PUT))
	log_.Append(makeInstanceByIndexAndType(loBallot, index, pb.CommandType_DEL))
	assert.Equal(t, pb.CommandType_PUT, log_.log[index].GetCommand().Type)
}

func TestAppendCase3Committed(t *testing.T)  {
	setup()

	var index int64 = 1
	inst1 := makeInstanceByAll(0, index, pb.InstanceState_COMMITTED, pb.CommandType_PUT)
	inst2 := makeInstanceByAll(0, index, pb.InstanceState_INPROGRESS, pb.CommandType_DEL)
	log_.Append(inst1)

	defer expectDeath(t, "Append case 3")
	log_.Append(inst2)
}

func TestAppendCase3Executed(t *testing.T)  {
	setup()

	var index int64 = 1
	inst1 := makeInstanceByAll(0, index, pb.InstanceState_EXECUTED, pb.CommandType_PUT)
	inst2 := makeInstanceByAll(0, index, pb.InstanceState_INPROGRESS, pb.CommandType_DEL)
	log_.Append(inst1)

	defer expectDeath(t, "Append case 3")
	log_.Append(inst2)
}

func TestAppendCase4(t *testing.T)  {
	setup()

	var index int64 = 1
	inst1 := makeInstanceByAll(0, index, pb.InstanceState_INPROGRESS, pb.CommandType_PUT)
	inst2 := makeInstanceByAll(0, index, pb.InstanceState_INPROGRESS, pb.CommandType_DEL)
	log_.Append(inst1)

	defer expectDeath(t, "Append case 4")
	log_.Append(inst2)
}

func TestCommit(t *testing.T) {
	setup()

	var index1 int64 = 1
	log_.Append(makeInstanceByIndex(0, index1))
	var index2 int64 = 2
	log_.Append(makeInstanceByIndex(0, index2))
	assert.True(t, log_.log[index1].GetState() == pb.InstanceState_INPROGRESS)
	assert.True(t, log_.log[index2].GetState() == pb.InstanceState_INPROGRESS)
	assert.False(t, log_.IsExecutable())

	log_.Commit(index2)
	assert.True(t, log_.log[index1].GetState() == pb.InstanceState_INPROGRESS)
	assert.True(t, log_.log[index2].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log_.IsExecutable())

	log_.Commit(index1)
	assert.True(t, log_.log[index1].GetState() == pb.InstanceState_COMMITTED)
	assert.True(t, log_.log[index2].GetState() == pb.InstanceState_COMMITTED)
	assert.True(t, log_.IsExecutable())
}

func TestCommitBeforeAppend(t *testing.T) {
	setup()

	var index1 int64 = 1
	var wg sync.WaitGroup
	wg.Add(1)
	// Do commit first
	go func(wg *sync.WaitGroup) {
		log_.Commit(index1)
		wg.Done()
	}(&wg)
	// Give sufficient time to run the go routine
	time.Sleep(50 * time.Millisecond)
	log_.Append(makeInstance(0))
	wg.Wait()
	assert.True(t, log_.log[index1].GetState() == pb.InstanceState_COMMITTED)
}

func TestAppendCommitExecute(t *testing.T) {
	setup()
	var index int64 = 1
	var done int64 = 0
	var wg sync.WaitGroup
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		for atomic.LoadInt64(&done) != 1 {
			log_.Execute(store_)
		}
		wg.Done()
	}(&wg)
	time.Sleep(50 * time.Millisecond)

	log_.Append(makeInstanceByIndex(0, index))
	atomic.AddInt64(&done, 1)
	log_.Commit(index)
	wg.Wait()

	assert.True(t, log_.log[index].GetState() == pb.InstanceState_EXECUTED)
	assert.Equal(t, index, log_.LastExecuted())
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
		log_.Execute(store_)
		log_.Execute(store_)
		log_.Execute(store_)
		wg.Done()
	}()

	log_.Append(makeInstanceByIndex(0, index1))
	log_.Append(makeInstanceByIndex(0, index2))
	log_.Append(makeInstanceByIndex(0, index3))

	log_.Commit(index3)
	log_.Commit(index2)
	log_.Commit(index1)

	wg.Wait()

	assert.True(t, log_.log[index1].GetState() == pb.InstanceState_EXECUTED)
	assert.True(t, log_.log[index2].GetState() == pb.InstanceState_EXECUTED)
	assert.True(t, log_.log[index3].GetState() == pb.InstanceState_EXECUTED)
	assert.Equal(t, index3, log_.LastExecuted())
}

func TestCommitUntil(t *testing.T) {
	setup()
	const (
		ballot int64 = iota
		index1
		index2
		index3
	)

	log_.Append(makeInstanceByIndex(ballot, index1))
	log_.Append(makeInstanceByIndex(ballot, index2))
	log_.Append(makeInstanceByIndex(ballot, index3))

	log_.CommitUntil(index2, ballot)

	assert.True(t, log_.log[index1].GetState() == pb.InstanceState_COMMITTED)
	assert.True(t, log_.log[index2].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log_.log[index3].GetState() == pb.InstanceState_COMMITTED)
	assert.True(t, log_.IsExecutable())

	log_.CommitUntil(index3, ballot)
	assert.True(t, log_.log[index3].GetState() == pb.InstanceState_COMMITTED)
	assert.True(t, log_.IsExecutable())
}

func TestCommitUntilHigherBallot(t *testing.T) {
	setup()
	const (
		ballot int64 = iota
		index1
		index2
		index3
	)

	log_.Append(makeInstanceByIndex(ballot, index1))
	log_.Append(makeInstanceByIndex(ballot, index2))
	log_.Append(makeInstanceByIndex(ballot, index3))

	log_.CommitUntil(index3, ballot+1)

	assert.False(t, log_.log[index1].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log_.log[index2].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log_.log[index3].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log_.IsExecutable())
}

func TestCommitUntilCase2(t *testing.T) {
	setup()
	const (
		index1 int64 = iota
		index2
		index3
		ballot
	)

	log_.Append(makeInstanceByIndex(ballot, index1))
	log_.Append(makeInstanceByIndex(ballot, index2))
	log_.Append(makeInstanceByIndex(ballot, index3))

	defer expectDeath(t, "Commit until test2 - no panic")
	log_.CommitUntil(index3, ballot-1)
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

	log_.Append(makeInstanceByIndex(ballot, index1))
	log_.Append(makeInstanceByIndex(ballot, index3))
	log_.Append(makeInstanceByIndex(ballot, index4))

	log_.CommitUntil(index4, ballot)

	assert.True(t, log_.log[index1].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log_.log[index3].GetState() == pb.InstanceState_COMMITTED)
	assert.False(t, log_.log[index4].GetState() == pb.InstanceState_COMMITTED)
	assert.True(t, log_.IsExecutable())
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
		log_.Execute(store_)
		log_.Execute(store_)
		log_.Execute(store_)
		wg.Done()
	}()

	log_.Append(makeInstanceByIndex(ballot, index1))
	log_.Append(makeInstanceByIndex(ballot, index2))
	log_.Append(makeInstanceByIndex(ballot, index3))
	log_.CommitUntil(index3, ballot)
	wg.Wait()

	assert.True(t, log_.log[index1].GetState() == pb.InstanceState_EXECUTED)
	assert.True(t, log_.log[index2].GetState() == pb.InstanceState_EXECUTED)
	assert.True(t, log_.log[index3].GetState() == pb.InstanceState_EXECUTED)
	assert.Equal(t, index3, log_.LastExecuted())
	assert.False(t, log_.IsExecutable())
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
		log_.Execute(store_)
		log_.Execute(store_)
		log_.Execute(store_)
		wg.Done()
	}()

	log_.Append(makeInstanceByIndex(ballot, index1))
	log_.Append(makeInstanceByIndex(ballot, index2))
	log_.Append(makeInstanceByIndex(ballot, index3))
	log_.CommitUntil(index3, ballot)
	wg.Wait()

	log_.TrimUntil(index3)

	assert.Nil(t, log_.log[index1])
	assert.Nil(t, log_.log[index2])
	assert.Nil(t, log_.log[index3])
	assert.Equal(t, index3, log_.LastExecuted())
	assert.Equal(t, index3, log_.GlobalLastExecuted())
	assert.False(t, log_.IsExecutable())
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
		log_.Execute(store_)
		log_.Execute(store_)
		wg.Done()
	}()

	log_.Append(makeInstanceByIndex(ballot, index1))
	log_.Append(makeInstanceByIndex(ballot, index2))
	log_.CommitUntil(index2, ballot)
	wg.Wait()

	log_.TrimUntil(index2)

	assert.Nil(t, log_.log[index1])
	assert.Nil(t, log_.log[index2])
	assert.Equal(t, index2, log_.LastExecuted())
	assert.Equal(t, index2, log_.GlobalLastExecuted())
	assert.False(t, log_.IsExecutable())

	log_.Append(makeInstanceByIndex(ballot, index1))
	log_.Append(makeInstanceByIndex(ballot, index2))
	assert.Nil(t, log_.log[index1])
	assert.Nil(t, log_.log[index2])
}

func TestLog_InstancesForPrepare(t *testing.T) {
	setup()
	var ballot int64 = 0
	const(
		index1 int64 = iota + 1
		index2
		index3
	)

	var wg sync.WaitGroup

	expect := make([]*pb.Instance, 0)
	assert.Equal(t, expect, log_.InstancesForPrepare())

	expect = append(expect, makeInstanceByIndex(ballot, index1))
	log_.Append(makeInstanceByIndex(ballot, index1))
	expect = append(expect, makeInstanceByIndex(ballot, index2))
	log_.Append(makeInstanceByIndex(ballot, index2))
	expect = append(expect, makeInstanceByIndex(ballot, index3))
	log_.Append(makeInstanceByIndex(ballot, index3))

	instances := log_.InstancesForPrepare()
	assert.Equal(t, expect, instances)

	var index int64 = 2
	log_.CommitUntil(index, ballot)
	assert.Equal(t, expect, instances)

	wg.Add(1)
	go func() {
		log_.Execute(store_)
		log_.Execute(store_)
		wg.Done()
	}()
	wg.Wait()

	log_.TrimUntil(index)
	expect = expect[index:]
	assert.Equal(t, expect, log_.InstancesForPrepare())
}

func expectDeath(t *testing.T, msg string) {
	if r := recover(); r == nil {
		t.Errorf(msg)
	}
}