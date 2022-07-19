package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/command"
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	inst "github.com/psu-csl/replicated-store/go/instance"
	consensusLog "github.com/psu-csl/replicated-store/go/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	IdBits = 0xff
	RoundIncrement = IdBits + 1
	MaxNumPeers int64 = 0xf
	RPCTimeout  = 100
)

type Multipaxos struct {
	running            int32
	ready              bool
	ballot             int64
	log                *consensusLog.Log
	id                 int64
	heartbeatInterval  int64
	ports              string
	lastHeartbeat      int64
	rpcPeers           []pb.MultiPaxosRPCClient
	server             *grpc.Server
	rpcServerRunning   bool
	rpcServerRunningCv *sync.Cond
	mu                 sync.Mutex

	cvLeader           *sync.Cond
	heartbeatRequest   pb.HeartbeatRequest
	heartbeatNumRpcs   int
	heartbeatResponses []*pb.HeartbeatResponse
	heartbeatMutex     sync.Mutex
	heartbeatCv        *sync.Cond

	cvFollower         *sync.Cond
	prepareRequest     pb.PrepareRequest
	prepareNumRpcs     int
	prepareOkResponses [][]*pb.Instance
	prepareMutex       sync.Mutex
	prepareCV          *sync.Cond

	addrs              []string
	pb.UnimplementedMultiPaxosRPCServer
}

func NewMultipaxos(config config.Config, log *consensusLog.Log) *Multipaxos {
	paxos := Multipaxos{
		running:            0,
		ready:              false,
		ballot:             MaxNumPeers,
		log:                log,
		id:                 config.Id,
		heartbeatInterval:  config.HeartbeatInterval,
		ports:              config.Peers[config.Id],
		lastHeartbeat:      0,
		rpcPeers:           make([]pb.MultiPaxosRPCClient, len(config.Peers)),
		rpcServerRunning:   false,
		server:             nil,
		heartbeatNumRpcs:   0,
		heartbeatResponses: make([]*pb.HeartbeatResponse, len(config.Peers)),
		prepareNumRpcs:     0,
		prepareOkResponses: make([][]*pb.Instance, len(config.Peers)),
		addrs: config.Peers,
	}
	paxos.rpcServerRunningCv = sync.NewCond(&paxos.mu)
	paxos.cvFollower = sync.NewCond(&paxos.mu)
	paxos.cvLeader = sync.NewCond(&paxos.mu)
	paxos.heartbeatCv = sync.NewCond(&paxos.heartbeatMutex)
	paxos.prepareCV = sync.NewCond(&paxos.prepareMutex)

	return &paxos
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
			Type:  command.Type(msg.GetCommand().GetType()),
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

// Helper functions
func (p *Multipaxos) NextBallot() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ballot += RoundIncrement
	p.ballot = (p.ballot & ^IdBits) | p.id
	p.ready = false
	p.cvLeader.Signal()
	return p.ballot
}

func(p *Multipaxos) setBallot(ballot int64) {
	oldId := p.ballot & IdBits
	newId := p.ballot & IdBits
	if (oldId == p.id  || oldId == MaxNumPeers) && oldId != newId {
		p.cvFollower.Signal()
	}
	p.ballot = ballot
}

func (p *Multipaxos) Leader() int64 {
	return p.ballot & IdBits
}

func (p *Multipaxos) IsLeaderLockless() bool {
	// Different from current c++ version
	return p.Leader() == p.id
}

func (p *Multipaxos) IsLeader() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.IsLeaderLockless()
}

func (p *Multipaxos) IsSomeoneElseLeader() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	leaderId := p.Leader()
	return leaderId != p.id && leaderId < MaxNumPeers
}

func (p *Multipaxos) waitUntilLeader() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.running == 1 && p.IsLeaderLockless() {
		p.cvLeader.Wait()
	}
}

func (p *Multipaxos) waitUntilFollower() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.running == 1 && p.IsLeaderLockless() {
		p.cvFollower.Wait()
	}
}

func (p *Multipaxos) Start() {
	if p.running == 1 {
		panic("running is true before Start()")
	}
	p.StartThreads()
	p.StartServer()
	p.Connect()
}

func (p *Multipaxos) StartServer() {
	atomic.StoreInt32(&p.running, 1)
	listener, err := net.Listen("tcp", p.ports)
	if err != nil {
		panic(err)
	}
	p.server = grpc.NewServer()
	pb.RegisterMultiPaxosRPCServer(p.server, p)

	p.mu.Lock()
	p.rpcServerRunning = true
	p.rpcServerRunningCv.Signal()
	p.mu.Unlock()

	go p.server.Serve(listener)
}

func (p *Multipaxos) StartThreads() {
	go p.PrepareThread()
	go p.HeartbeatThread()
}

func (p *Multipaxos) Stop() {
	//if p.running == 0 {
	//	panic("not running before Stop")
	//}
	atomic.StoreInt32(&p.running, 0)

	p.cvLeader.Signal()
	p.cvFollower.Signal()

	p.mu.Lock()
	for !p.rpcServerRunning {
		p.rpcServerRunningCv.Wait()
	}
	p.mu.Unlock()
	p.server.GracefulStop()
}

func (p *Multipaxos) Connect() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	//opts = append(opts, grpc.WithBlock())
	//retryOpts := []grpc_retry.CallOption{grpc_retry.WithBackoff(grpc_retry.
	//	BackoffLinear(100 * time.Millisecond))}
	//opts = append(opts, grpc.WithUnaryInterceptor(grpc_retry.
	//	UnaryClientInterceptor(retryOpts...)))

	for i, addr := range p.addrs {
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			panic("dial error")
		}
		client := pb.NewMultiPaxosRPCClient(conn)
		p.rpcPeers[i] = client
	}
}

// Functions for testing
func (p *Multipaxos) Id() int64 {
	return p.id
}

func (p *Multipaxos) Ballot() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ballot
}
