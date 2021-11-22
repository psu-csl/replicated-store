package paxos

import (
	"github.com/psu-csl/replicated-store/go/operation"
)
import "github.com/psu-csl/replicated-store/go/store"
import "sync"
import "sync/atomic"

type Fate int

const (
	Decided   Fate = iota + 1
)

type DecidedRequestArgs struct {
	Slot   uint64
	Value  operation.Command
	Sender int
}

type Log struct {
	status  Fate
	command operation.Command
}

type Paxos struct {
	mu           sync.RWMutex
	peers        []string
	me           int            // index into peers[]

	log          sync.Map       // in-memory log
	slot         uint64         // next available slot
	nextExecSlot uint64

	store        *store.Store

	execLock     sync.Mutex
	execCond     *sync.Cond
}

func NewPaxos(peers []string, me int, store *store.Store) *Paxos {
	px := Paxos{
		peers:        peers,
		me:           me,
		log:          sync.Map{},
		slot:         0,
		nextExecSlot: 0,
		store: store,
	}
	px.execCond = sync.NewCond(&px.execLock)
	return &px
}

func (px *Paxos) AgreeAndExecute(cmd operation.Command) *operation.CommandResult {
	// check leader and may run p1
	cmdResult := px.dummyPaxos(cmd)

	return cmdResult
}

func (px *Paxos) dummyPaxos(cmd operation.Command) *operation.CommandResult {
	slot := atomic.AddUint64(&px.slot, 1)
	// Assume everything runs well, and proceed to commit phase
	px.commit(slot, cmd)
	// Execute the command only after the command is committed in its local log
	cmdResult := px.execEntry(slot)
	return &cmdResult
}

// Hanlder to commit the command in the in-memorty log
func (px *Paxos) commit(slot uint64, cmd operation.Command) error {
	px.mu.Lock()
	entry := Log{
		status:  Decided,
		command: cmd,
	}
	px.log.Store(slot, entry)
	px.mu.Unlock()

	//if reqArgs.Sender != px.me {
	//	// Follower nodes receive the command
	//	// they should proceed to execute it, but don't need to return the result
	//	px.execEntry(reqArgs.Slot)
	//}
	return nil
}

func (px *Paxos) execEntry(slot uint64) operation.CommandResult {
	px.execLock.Lock()
	defer px.execLock.Unlock()
	for px.nextExecSlot + 1 != slot {
		px.execCond.Wait()
	}

	entry, _ := px.log.Load(slot)
	cmd := entry.(Log).command
	result := operation.CommandResult{
		CommandID:    cmd.CommandID,
		IsSuccess:    true,
		Value:        "",
		Error:        "",
	}
	switch cmd.Type {
	case "Put":
		err := px.store.Put(cmd.Key, cmd.Value)
		if err != nil {
			result.IsSuccess = false
			result.Error = err.Error()
		}
	case "Get":
		val, err := px.store.Get(cmd.Key)
		result.Value = val
		if err != nil {
			result.IsSuccess = false
			result.Error = err.Error()
		}
	case "Delete":
		err := px.store.Del(cmd.Key)
		if err != nil {
			result.IsSuccess = false
			result.Error = err.Error()
		}
	default:
		result.IsSuccess = false
		result.Error = "command type not found"
	}

	px.nextExecSlot = slot
	px.execCond.Broadcast()
	return result
}
