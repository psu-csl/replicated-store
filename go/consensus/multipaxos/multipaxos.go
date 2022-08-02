package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	consensusLog "github.com/psu-csl/replicated-store/go/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"math/rand"
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
	running           int32
	isReady           int32
	ballot            int64
	log               *consensusLog.Log
	id                int64
	heartbeatInterval int64
	heartbeatDelta    int64
	ports             string
	lastHeartbeat     int64
	rpcPeers          []pb.MultiPaxosRPCClient
	server            *grpc.Server
	rpcServerRunning  bool
	rpcServerRunningCv *sync.Cond
	mu                 sync.Mutex
	cvLeader           *sync.Cond
	cvFollower         *sync.Cond

	addrs              []string
	pb.UnimplementedMultiPaxosRPCServer
}

func NewMultipaxos(config config.Config, log *consensusLog.Log) *Multipaxos {
	paxos := Multipaxos{
		running:           0,
		isReady:           0,
		ballot:            MaxNumPeers,
		log:               log,
		id:                config.Id,
		heartbeatInterval: config.HeartbeatInterval,
		heartbeatDelta:    config.HeartbeatDelta,
		ports:             config.Peers[config.Id],
		lastHeartbeat:     0,
		rpcPeers:          make([]pb.MultiPaxosRPCClient, len(config.Peers)),
		rpcServerRunning:  false,
		server:            nil,
		addrs: config.Peers,
	}
	paxos.rpcServerRunningCv = sync.NewCond(&paxos.mu)
	paxos.cvFollower = sync.NewCond(&paxos.mu)
	paxos.cvLeader = sync.NewCond(&paxos.mu)

	return &paxos
}

func (p *Multipaxos) Start() {
	if p.running == 1 {
		panic("running is true before Start()")
	}
	p.StartServer()
	p.Connect()
	p.StartThreads()
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

func (p *Multipaxos) Replicate(command *pb.Command, clientId int64) Result {
	ballot, isReady := p.Ballot()
	if IsLeader(ballot, p.id) {
		if isReady == 1{
			return p.SendAccepts(ballot, p.log.AdvanceLastIndex(), command,
				clientId)
		}
		return Result{Type: Retry, Leader: -1}
	}
	if IsSomeoneElseLeader(ballot, p.id) {
		return Result{Type: SomeElseLeader, Leader: Leader(ballot)}
	}
	return Result{Type: Retry, Leader: -1}
}

func (p *Multipaxos) HeartbeatThread() {
	for p.running == 1 {
		p.waitUntilLeader()

		gle := p.log.GlobalLastExecuted()
		for p.running == 1 {
			ballot, _ := p.Ballot()
			if !IsLeader(ballot, p.id) {
				break
			}

			gle = p.SendHeartbeats(ballot, gle)
			time.Sleep(time.Duration(p.heartbeatInterval))
		}
	}
}

func (p *Multipaxos) PrepareThread() {
	for p.running == 1 {
		p.waitUntilFollower()
		for p.running == 1 {
			sleepTime := p.heartbeatInterval + p.heartbeatDelta +
				rand.Int63n(p.heartbeatInterval-p.heartbeatDelta)
			time.Sleep(time.Duration(sleepTime))
			if time.Now().UnixNano()/1e6-p.lastHeartbeat < p.heartbeatInterval {
				continue
			}

			ballot := p.NextBallot()
			replayLog := p.SendPrepares(ballot)
			p.Replay(ballot, replayLog)
			break
		}
	}
}

func (p *Multipaxos) SendHeartbeats(ballot int64, globalLastExecuted int64) int64 {
	var (
		numRpcs         = 0
		numOks          = 0
		leader          = p.id
		minLastExecuted = p.log.LastExecuted()
		mu              sync.Mutex
	)
	cv := sync.NewCond(&mu)
	request := pb.HeartbeatRequest{
		Ballot:             ballot,
		LastExecuted:       minLastExecuted,
		GlobalLastExecuted: globalLastExecuted,
		Sender:             p.id,
	}

	for i, peer := range p.rpcPeers {
		go func(i int, peer pb.MultiPaxosRPCClient) {
			ctx, cancel := context.WithTimeout(context.Background(),
				RPCTimeout*time.Millisecond)
			defer cancel()

			response, err := peer.Heartbeat(ctx, &request)
			mu.Lock()
			defer mu.Unlock()
			defer cv.Signal()

			numRpcs += 1
			if err != nil {
				return
			}
			if response.GetType() == pb.ResponseType_OK {
				numOks += 1
				if response.GetLastExecuted() < minLastExecuted {
					minLastExecuted = response.GetLastExecuted()
				}
			} else {
				p.mu.Lock()
				if response.GetBallot() > p.ballot {
					p.setBallot(response.GetBallot())
					leader = Leader(p.ballot)
				}
				p.mu.Unlock()
			}
		}(i, peer)
	}

	mu.Lock()
	defer mu.Unlock()
	for leader == p.id && numRpcs != len(p.rpcPeers) {
		cv.Wait()
	}
	if numOks == len(p.rpcPeers) {
		return minLastExecuted
	}
	return globalLastExecuted
}

func (p *Multipaxos) SendPrepares(ballot int64) map[int64]*pb.Instance {
	var (
		numRpcs = 0
		numOks  = 0
		leader = p.id
		mu      sync.Mutex
	)
	cv := sync.NewCond(&mu)
	logMap := make(map[int64]*pb.Instance)
	request := pb.PrepareRequest{
		Ballot: ballot,
		Sender: p.id,
	}

	for i, peer := range p.rpcPeers {
		go func(i int, peer pb.MultiPaxosRPCClient) {
			ctx, cancel := context.WithTimeout(context.Background(),
				RPCTimeout*time.Millisecond)
			defer cancel()

			response, err := peer.Prepare(ctx, &request)
			mu.Lock()
			defer mu.Unlock()
			defer cv.Signal()

			numRpcs += 1
			if err != nil {
				return
			}
			if response.GetType() == pb.ResponseType_OK {
				numOks += 1
				receivedInstance := response.GetLogs()
				for _, instance := range receivedInstance {
					consensusLog.Insert(logMap, instance)
				}
			} else {
				p.mu.Lock()
				if response.GetBallot() > p.ballot {
					p.setBallot(response.GetBallot())
					leader = Leader(p.ballot)
				}
				p.mu.Unlock()
			}
		}(i, peer)
	}

	mu.Lock()
	defer mu.Unlock()
	for leader == p.id && numOks <= len(p.rpcPeers) / 2 &&
		numRpcs != len(p.rpcPeers) {
		cv.Wait()
	}

	if numOks > len(p.rpcPeers) / 2 {
		return logMap
	}
	return nil
}

func (p *Multipaxos) SendAccepts(ballot int64, index int64,
	command *pb.Command, clientId int64) Result {
	var (
		numRpcs = 0
		numOks  = 0
		leader = p.id
		mu      sync.Mutex
	)
	cv := sync.NewCond(&mu)

	instance := pb.Instance{
		Ballot:   ballot,
		Index:    index,
		ClientId: clientId,
		State:    pb.InstanceState_INPROGRESS,
		Command:  command,
	}

	request := pb.AcceptRequest{
		Instance: &instance,
		Sender:   p.id,
	}

	for i, peer := range p.rpcPeers {
		go func(i int, peer pb.MultiPaxosRPCClient) {
			ctx := context.Background()
			response, err := peer.Accept(ctx, &request)
			mu.Lock()
			defer mu.Unlock()
			defer cv.Signal()

			numRpcs += 1
			if err != nil {
				return
			}
			if response.GetType() == pb.ResponseType_OK {
				numOks += 1
			} else {
				p.mu.Lock()
				if response.GetBallot() > p.ballot {
					p.setBallot(response.GetBallot())
					leader = Leader(p.ballot)
				}
				p.mu.Unlock()
			}
		}(i, peer)
	}

	mu.Lock()
	defer mu.Unlock()
	for leader == p.id && numOks <= len(p.rpcPeers) / 2 &&
		numRpcs != len(p.rpcPeers) {
		cv.Wait()
	}

	if numOks > len(p.rpcPeers) / 2 {
		p.log.Commit(index)
		return Result{Type: Ok, Leader: -1}
	}
	if leader != p.id {
		return Result{Type: SomeElseLeader, Leader: leader}
	}
	return Result{Type: Retry, Leader: -1}
}

func (p *Multipaxos) Replay(ballot int64, replayLog map[int64]*pb.Instance) {
	if replayLog == nil {
		return
	}

	for index, instance := range replayLog {
		result := p.SendAccepts(ballot, index, instance.GetCommand(),
			instance.GetClientId())
		if result.Type == SomeElseLeader {
			return
		} else if result.Type == Retry {
			continue
		}
	}
	atomic.StoreInt32(&p.isReady, 1)
}

func (p *Multipaxos) Heartbeat(ctx context.Context,
	request *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if request.GetBallot() >= p.ballot {
		atomic.StoreInt64(&p.lastHeartbeat, time.Now().UnixNano() / 1e6)
		p.setBallot(request.GetBallot())
		p.log.CommitUntil(request.GetLastExecuted(), request.GetBallot())
		p.log.TrimUntil(request.GetGlobalLastExecuted())
	}
	return &pb.HeartbeatResponse{LastExecuted: p.log.LastExecuted()}, nil
}

func (p *Multipaxos) Prepare(ctx context.Context,
	request *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if request.GetBallot() >= p.ballot {
		p.setBallot(request.GetBallot())
		logSlice := p.log.InstancesSinceGlobalLastExecuted()
		grpcLogs := make([]*pb.Instance, len(logSlice))
		for index, i := range logSlice {
			grpcLogs[index] = i
		}
		return &pb.PrepareResponse{
			Type:   pb.ResponseType_OK,
			Ballot: -1,
			Logs:  grpcLogs,
		}, nil
	}
	return &pb.PrepareResponse{
		Type:   pb.ResponseType_REJECT,
		Ballot: p.ballot,
		Logs:   nil,
	}, nil
}

func (p *Multipaxos) Accept(ctx context.Context,
	request *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if request.GetInstance().GetBallot() >= p.ballot {
		p.setBallot(request.GetInstance().GetBallot())
		instance := &pb.Instance{
			Ballot:   request.GetInstance().GetBallot(),
			Index:    request.GetInstance().GetIndex(),
			ClientId: request.GetInstance().GetClientId(),
			State:    pb.InstanceState_INPROGRESS,
			Command:  request.GetInstance().GetCommand(),
		}
		p.log.Append(instance)
		return &pb.AcceptResponse{Type: pb.ResponseType_OK,
			Ballot: p.ballot}, nil
	}
	return &pb.AcceptResponse{Type: pb.ResponseType_REJECT,
		Ballot: p.ballot}, nil
}

// Helper functions
func (p *Multipaxos) NextBallot() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	oldBallot := p.ballot
	p.ballot += RoundIncrement
	p.ballot = (p.ballot & ^IdBits) | p.id
	p.isReady = 1
	log.Printf("%v became a leader: ballot: %v -> %v\n", p.id, oldBallot,
		p.ballot)
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

func (p *Multipaxos) Id() int64 {
	return p.id
}

func (p *Multipaxos) Ballot() (int64, int32){
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.ballot, p.isReady
}

func (p *Multipaxos) waitUntilLeader() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.running == 1 && !IsLeader(p.ballot, p.id) {
		p.cvLeader.Wait()
	}
}

func (p *Multipaxos) waitUntilFollower() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.running == 1 && IsLeader(p.ballot, p.id) {
		p.cvFollower.Wait()
	}
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

func Leader(ballot int64) int64 {
	return ballot & IdBits
}

func IsLeader(ballot int64, id int64) bool {
	return Leader(ballot) == id
}

func IsSomeoneElseLeader(ballot int64, id int64) bool {
	return !IsLeader(ballot, id) && Leader(ballot) < MaxNumPeers
}
