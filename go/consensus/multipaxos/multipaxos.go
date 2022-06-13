package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	consensusLog "github.com/psu-csl/replicated-store/go/log"
	"google.golang.org/grpc"
	"net"
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
	pb.UnimplementedMultiPaxosRPCServer
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

func (p *Multipaxos) HeartbeatHandler(ctx context.Context,
	msg *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if msg.Ballot >= p.ballot {
		p.lastHeartbeat = time.Now()
		p.ballot = msg.Ballot
		p.log.CommitUntil(msg.LastExecuted, msg.Ballot)
		p.log.TrimUntil(msg.GlobalLastExecuted)
	}
	return &pb.HeartbeatResponse{LastExecuted: p.log.LastExecuted()}, nil
}

func (p *Multipaxos) Run() {
	// Create grpc server
	listener, err := net.Listen("tcp", ":8888")
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterMultiPaxosRPCServer(grpcServer, p)
	go grpcServer.Serve(listener)
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
