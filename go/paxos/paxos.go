package paxos

import (
	"github.com/psu-csl/replicated-store/go/command"
)
import "github.com/psu-csl/replicated-store/go/store"

type Paxos struct {
	peers []string
	me    int // index into peers[]
	store *store.Store
}

func NewPaxos(peers []string, me int, store *store.Store) *Paxos {
	px := Paxos{
		peers: peers,
		me:    me,
		store: store,
	}
	return &px
}

func (px *Paxos) AgreeAndExecute(cmd command.Command) *command.CommandResult {
	// check leader and may run p1
	cmdResult := px.dummyPaxos(cmd)

	return cmdResult
}

func (px *Paxos) dummyPaxos(cmd command.Command) *command.CommandResult {
	cmdResult := px.execEntry(cmd)
	return &cmdResult
}

func (px *Paxos) execEntry(cmd command.Command) command.CommandResult {
	//px.execLock.Lock()
	//defer px.execLock.Unlock()
	//for px.nextExecSlot + 1 != slot {
	//	px.execCond.Wait()
	//}

	result := command.CommandResult{
		IsSuccess: true,
		Value:     "",
	}
	switch cmd.CommandType {
	case "Put":
		err := px.store.Put(cmd.Key, cmd.Value)
		if err != nil {
			result.IsSuccess = false
		}
	case "Get":
		val, err := px.store.Get(cmd.Key)
		result.Value = val
		if err != nil {
			result.IsSuccess = false
		}
	case "Delete":
		err := px.store.Del(cmd.Key)
		if err != nil {
			result.IsSuccess = false
		}
	default:
		result.IsSuccess = false
	}

	//px.nextExecSlot = slot
	return result
}
