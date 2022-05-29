package log

import (
	"github.com/psu-csl/replicated-store/go/command"
	inst "github.com/psu-csl/replicated-store/go/instance"
	"github.com/psu-csl/replicated-store/go/store"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	log_   *Log
	store_ *store.MemKVStore
)

func setup() {
	log_ = NewLog()
	store_ = store.NewMemKVStore()
}

func makeInstance(ballot int64) inst.Instance {
	return inst.MakeInstance(ballot, command.Command{},
	log_.AdvanceLastIndex(), inst.InProgress, 0)
}

func makeInstanceByIndex(ballot int64, index int64) inst.Instance {
	return inst.MakeInstance(ballot, command.Command{}, index,
	inst.InProgress,  0)
}

func makeInstanceByState(ballot int64, state inst.State) inst.Instance {
	return inst.MakeInstance(ballot, command.Command{}, log_.AdvanceLastIndex(),
	state, 0)
}

func makeInstanceByIndexAndType(ballot int64, index int64,
	cmdType command.Type) inst.Instance {
	return inst.MakeInstance(ballot, command.Command{Type: cmdType}, index,
	inst.InProgress, 0)
}

func makeInstanceByAll(ballot int64, index int64, state inst.State,
	cmdType command.Type) inst.Instance {
	return inst.MakeInstance(ballot, command.Command{Type: cmdType}, index,
	state, 0)
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
	assert.Equal(t, int64(1), log_.log[1].Index())
	assert.Equal(t, int64(2), log_.log[2].Index())
}

func TestAppendExecuted(t *testing.T) {
	setup()

	log_.Append(makeInstanceByState(0, inst.Executed))
	assert.True(t, log_.log[1].IsCommitted())
}

func TestAppendWithGap(t *testing.T) {
	setup()

	var index int64 = 42
	log_.Append(makeInstanceByIndex(0, index))
	assert.Equal(t, index, log_.log[index].Index())
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
	log_.Append(makeInstanceByIndexAndType(loBallot, index, command.Put))
	log_.Append(makeInstanceByIndexAndType(hiBallot, index, command.Del))
	assert.Equal(t, command.Del, log_.log[index].Command().Type)
}

func TestAppendLowBallotNoEffect(t *testing.T)  {
	setup()

	var (
		index int64 = 1
		loBallot int64 = 0
		hiBallot int64 = 1
	)
	log_.Append(makeInstanceByIndexAndType(hiBallot, index, command.Put))
	log_.Append(makeInstanceByIndexAndType(loBallot, index, command.Del))
	assert.Equal(t, command.Put, log_.log[index].Command().Type)
}

func TestAppendCase3Committed(t *testing.T)  {
	setup()

	var index int64 = 1
	inst1 := makeInstanceByAll(0, index, inst.Committed, command.Put)
	inst2 := makeInstanceByAll(0, index, inst.InProgress, command.Del)
	log_.Append(inst1)

	defer expectDeath(t, "Append case 3")
	log_.Append(inst2)
}

func TestAppendCase3Executed(t *testing.T)  {
	setup()

	var index int64 = 1
	inst1 := makeInstanceByAll(0, index, inst.Executed, command.Put)
	inst2 := makeInstanceByAll(0, index, inst.InProgress, command.Del)
	log_.Append(inst1)

	defer expectDeath(t, "Append case 3")
	log_.Append(inst2)
}

func TestAppendCase4(t *testing.T)  {
	setup()

	var index int64 = 1
	inst1 := makeInstanceByAll(0, index, inst.InProgress, command.Put)
	inst2 := makeInstanceByAll(0, index, inst.InProgress, command.Del)
	log_.Append(inst1)

	defer expectDeath(t, "Append case 4")
	log_.Append(inst2)
}

func expectDeath(t *testing.T, msg string) {
	if r := recover(); r == nil {
		t.Errorf(msg)
	}
}