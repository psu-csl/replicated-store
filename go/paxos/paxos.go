package paxos

import "github.com/psu-csl/replicated-store/go/store"

type Paxos struct {
	peers []string
	me    int // index into peers[]
	store *store.MemKVStore
}

func NewPaxos(peers []string, me int, store *store.MemKVStore) *Paxos {
	px := Paxos{
		peers: peers,
		me:    me,
		store: store,
	}
	return &px
}

