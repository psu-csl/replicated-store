package paxos

import (
	"encoding/json"
	"github.com/psu-csl/replicated-store/go/operation"
	"log"
)
import "github.com/psu-csl/replicated-store/go/store"
import "net"
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

func NewPaxos(peers []string, me int) *Paxos {
	px := Paxos{
		peers:        peers,
		me:           me,
		log:          sync.Map{},
		slot:         0,
		nextExecSlot: 0,
	}
	px.store = store.NewStore()
	px.execCond = sync.NewCond(&px.execLock)
	return &px
}

func (px *Paxos) Start(cmd operation.Command, socket net.Conn) {
	go px.startPaxos(cmd, socket)
}

func (px *Paxos) startPaxos(cmd operation.Command, socket net.Conn) {
	// check leader and may run p1
	cmdResult := px.phase2Accept(cmd)

	// Socket writes back command result
	respByte, err := json.Marshal(cmdResult)
	if err != nil {
		log.Printf("json marshal error on server: %v", err)
	}
	_, err = socket.Write(respByte)
	if err != nil {
		log.Printf("server write error: %v", err)
	}
}

func (px *Paxos) phase2Accept(cmd operation.Command) *operation.CommandResult {
	slot := atomic.AddUint64(&px.slot, 1)
	// Assume everything runs well, and proceed to commit phase
	px.sendDecidedReq(slot, cmd)
	// Execute the command only after the command is committed in its local log
	cmdResult := px.execEntry(slot)
	return &cmdResult
}

// Send Decided request
func (px *Paxos) sendDecidedReq(slot uint64, v operation.Command) bool {
	decidedArgs := DecidedRequestArgs {
		Slot:   slot,
		Value:  v,
		Sender: px.me,
	}
	// Send commit messages to other nodes

	// Append entry to its own log
	px.Decided(&decidedArgs)
	return true
}

// Hanlder to commit the command in the in-memorty log
func (px *Paxos) Decided(reqArgs *DecidedRequestArgs) error {
	px.mu.Lock()
	entry := Log{
		status:  Decided,
		command: reqArgs.Value,
	}
	px.log.Store(reqArgs.Slot, entry)
	px.mu.Unlock()

	if reqArgs.Sender != px.me {
		// Follower nodes receive the command
		// they should proceed to execute it, but don't need to return the result
		px.execEntry(reqArgs.Slot)
	}
	return nil
}

func (px *Paxos) execEntry(slot uint64) operation.CommandResult {
	px.execLock.Lock()
	defer px.execLock.Unlock()
	for px.nextExecSlot + 1 != slot {
		px.execCond.Wait()
	}

	entry, _ := px.log.Load(slot)
	cmdResult := px.store.ApplyCommand(entry.(Log).command)

	px.nextExecSlot = slot
	px.execCond.Broadcast()
	return cmdResult
}
