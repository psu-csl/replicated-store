package multipaxos

import (
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	"sync"
)

type ResultType int

const (
	Ok ResultType = iota
	Retry
	SomeElseLeader
)

const (
	IdBits               = 0xff
	RoundIncrement       = IdBits + 1
	MaxNumPeers    int64 = 0xf
	NoLeader       int64 = -1
)

type RpcPeer struct {
	Id   int64
	Stub pb.MultiPaxosRPCClient
}

func NewRpcPeer(id int64, stub pb.MultiPaxosRPCClient) *RpcPeer {
	peer := &RpcPeer{
		Id:   id,
		Stub: stub,
	}
	return peer
}

type Result struct {
	Type   ResultType
	Leader int64
}

type PrepareState struct {
	NumRpcs int
	NumOks  int
	Leader  int64
	Log     map[int64]*pb.Instance
	Mu      sync.Mutex
	Cv      *sync.Cond
}

func NewPrepareState(leader int64) *PrepareState {
	prepareState := &PrepareState{
		NumRpcs: 0,
		NumOks:  0,
		Leader:  leader,
		Log:     make(map[int64]*pb.Instance),
	}
	prepareState.Cv = sync.NewCond(&prepareState.Mu)
	return prepareState
}

type AcceptState struct {
	NumRpcs int
	NumOks  int
	Leader  int64
	Mu      sync.Mutex
	Cv      *sync.Cond
}

func NewAcceptState(leader int64) *AcceptState {
	acceptState := &AcceptState{
		NumRpcs: 0,
		NumOks:  0,
		Leader:  leader,
	}
	acceptState.Cv = sync.NewCond(&acceptState.Mu)
	return acceptState
}

type CommitState struct {
	NumRpcs         int
	NumOks          int
	Leader          int64
	MinLastExecuted int64
	Mu              sync.Mutex
	Cv              *sync.Cond
}

func NewCommitState(leader int64, minLastExecuted int64) *CommitState {
	commitState := &CommitState{
		NumRpcs:         0,
		NumOks:          0,
		Leader:          leader,
		MinLastExecuted: minLastExecuted,
	}
	commitState.Cv = sync.NewCond(&commitState.Mu)
	return commitState
}
