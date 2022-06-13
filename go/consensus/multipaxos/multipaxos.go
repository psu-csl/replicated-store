package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/command"
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	inst "github.com/psu-csl/replicated-store/go/instance"
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

func (p *Multipaxos) AcceptHandler(ctx context.Context, 
	msg *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if msg.GetBallot() >= p.ballot {
		p.ballot = msg.GetBallot()
		cmd := command.Command{
			Key:   msg.GetCommand().GetKey(),
			Value: msg.GetCommand().GetValue(),
			Type:  msg.GetCommand().GetType(),
		}
		instance := inst.MakeInstance(msg.GetBallot(), cmd, 
			msg.GetIndex(), inst.InProgress, msg.GetClientId())
		p.log.Append(instance)
		return &pb.AcceptResponse{Type: pb.AcceptResponse_ok,
			Ballot: p.ballot}, nil
	}
	return &pb.AcceptResponse{Type: pb.AcceptResponse_reject,
		Ballot: p.ballot}, nil
}

func (p *Multipaxos) HeartbeatHandler(ctx context.Context,
	msg *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if msg.GetBallot() >= p.ballot {
		p.lastHeartbeat = time.Now()
		p.ballot = msg.Ballot
		p.log.CommitUntil(msg.GetLastExecuted(), msg.GetBallot())
		p.log.TrimUntil(msg.GetGlobalLastExecuted())
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

	// Setup connections to other severs
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
