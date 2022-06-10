package multipaxos

import (
	"github.com/psu-csl/replicated-store/go/config"
	consensusLog "github.com/psu-csl/replicated-store/go/log"
	"sync"
	"time"
)

const (
	IdBits = 0xff
	RoundIncrement = IdBits + 1
	MaxNumPeers int64 = 0xf
)

type Multipaxos struct {
	id                int64
	ballot            int64
	log               *consensusLog.Log
	peers             []*Multipaxos
	ready             bool
	lastHeartbeat     time.Time
	heartbeatInterval int64
	mu                sync.Mutex
}

func NewMultipaxos(config config.Config, log *consensusLog.Log) *Multipaxos {
	paxos := Multipaxos{
		id:                config.Id,
		ballot:            MaxNumPeers,
		log:               log,
		peers:             nil,
		ready:             false,
		lastHeartbeat:     time.Time{},
		heartbeatInterval: 0,
	}
	return &paxos
}

func (p *Multipaxos) NextBallot() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ballot += RoundIncrement
	p.ballot = (p.ballot & ^IdBits) | p.id
	return p.ballot
}

func (p *Multipaxos) Leader() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.ballot & IdBits
}

func (p *Multipaxos) IsLeader() bool {
	return p.Leader() == p.id
}

func (p *Multipaxos) IsSomeoneElseLeader() bool {
	id := p.Leader()
	return id != p.id && id < MaxNumPeers
}

// Testing helper functions
func (p *Multipaxos) Id() int64 {
	return p.id
}

func (p *Multipaxos) Ballot() int64 {
	return p.ballot
}

func (p *Multipaxos) LastHeartbeat() time.Time {
	return p.lastHeartbeat
}
