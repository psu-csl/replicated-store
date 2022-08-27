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

func Leader(ballot int64) int64 {
	return ballot & IdBits
}

func IsLeader(ballot int64, id int64) bool {
	return Leader(ballot) == id
}

func IsSomeoneElseLeader(ballot int64, id int64) bool {
	return !IsLeader(ballot, id) && Leader(ballot) < MaxNumPeers
}

type Multipaxos struct {
	ready          int32
	ballot         int64
	log            *consensusLog.Log
	id             int64
	CommitInterval int64
	port           string
	lastCommit     int64
	rpcPeers       []pb.MultiPaxosRPCClient
	mu             sync.Mutex

	cvLeader   *sync.Cond
	cvFollower *sync.Cond

	server             *grpc.Server
	rpcServerRunning   bool
	rpcServerRunningCv *sync.Cond

	prepareThreadRunning uint32
	commitThreadRunning  uint32

	addrs []string
	pb.UnimplementedMultiPaxosRPCServer
}

func NewMultipaxos(config config.Config, log *consensusLog.Log) *Multipaxos {
	paxos := Multipaxos{
		ready:                0,
		ballot:               MaxNumPeers,
		log:                  log,
		id:                   config.Id,
		CommitInterval:       config.CommitInterval,
		port:                 config.Peers[config.Id],
		lastCommit:           0,
		rpcPeers:             make([]pb.MultiPaxosRPCClient, len(config.Peers)),
		rpcServerRunning:     false,
		prepareThreadRunning: 0,
		commitThreadRunning:  0,
		server:               nil,
		addrs:                config.Peers,
	}
	paxos.rpcServerRunningCv = sync.NewCond(&paxos.mu)
	paxos.cvFollower = sync.NewCond(&paxos.mu)
	paxos.cvLeader = sync.NewCond(&paxos.mu)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	for i, addr := range config.Peers {
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			panic("dial error")
		}
		client := pb.NewMultiPaxosRPCClient(conn)
		paxos.rpcPeers[i] = client
	}

	return &paxos
}

func (p *Multipaxos) Start() {
	p.StartPrepareThread()
	p.StartCommitThread()
	p.StartRPCServer()
}

func (p *Multipaxos) Stop() {
	p.StopRPCServer()
	p.StopPrepareThread()
	p.StopCommitThread()
}

func (p *Multipaxos) StartRPCServer() {
	listener, err := net.Listen("tcp", p.port)
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

func (p *Multipaxos) StopRPCServer() {
	p.mu.Lock()
	for !p.rpcServerRunning {
		p.rpcServerRunningCv.Wait()
	}
	p.mu.Unlock()
	p.server.GracefulStop()
}

func (p *Multipaxos) StartPrepareThread() {
	if p.prepareThreadRunning == 1 {
		panic("prepareThreadRunning is true")
	}
	atomic.StoreUint32(&p.prepareThreadRunning, 1)
	go p.PrepareThread()
}

func (p *Multipaxos) StopPrepareThread() {
	if p.prepareThreadRunning == 0 {
		panic("prepareThreadRunning is false")
	}
	atomic.StoreUint32(&p.prepareThreadRunning, 0)
	p.cvFollower.Signal()
}

func (p *Multipaxos) StartCommitThread() {
	if p.commitThreadRunning == 1 {
		panic("commitThreadRunning is true")
	}
	atomic.StoreUint32(&p.commitThreadRunning, 1)
	go p.CommitThread()
}

func (p *Multipaxos) StopCommitThread() {
	if p.commitThreadRunning == 0 {
		panic("commitThreadRunning is false")
	}
	atomic.StoreUint32(&p.commitThreadRunning, 0)
	p.cvLeader.Signal()
}

func (p *Multipaxos) Replicate(command *pb.Command, clientId int64) Result {
	ballot, ready := p.Ballot()
	if IsLeader(ballot, p.id) {
		if ready == 1 {
			return p.RunAcceptPhase(ballot, p.log.AdvanceLastIndex(), command,
				clientId)
		}
		return Result{Type: Retry, Leader: NoLeader}
	}
	if IsSomeoneElseLeader(ballot, p.id) {
		return Result{Type: SomeElseLeader, Leader: Leader(ballot)}
	}
	return Result{Type: Retry, Leader: NoLeader}
}

func (p *Multipaxos) PrepareThread() {
	for p.prepareThreadRunning == 1 {
		p.waitUntilFollower()
		for p.prepareThreadRunning == 1 {
			p.sleepForRandomInterval()
			if time.Now().UnixNano()/1e6-p.lastCommit < p.CommitInterval {
				continue
			}
			ballot := p.NextBallot()
			replayLog := p.RunPreparePhase(ballot)
			p.Replay(ballot, replayLog)
			break
		}
	}
}

func (p *Multipaxos) CommitThread() {
	for p.commitThreadRunning == 1 {
		p.waitUntilLeader()

		gle := p.log.GlobalLastExecuted()
		for p.commitThreadRunning == 1 {
			ballot, _ := p.Ballot()
			if !IsLeader(ballot, p.id) {
				break
			}
			gle = p.RunCommitPhase(ballot, gle)
			p.sleepForCommitInterval()
		}
	}
}

func (p *Multipaxos) RunPreparePhase(ballot int64) map[int64]*pb.Instance {
	var (
		numRpcs = 0
		numOks  = 0
		leader  = p.id
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
			ctx := context.Background()
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
				receivedInstances := response.GetLogs()
				for _, instance := range receivedInstances {
					consensusLog.Insert(logMap, instance)
				}
			} else {
				p.mu.Lock()
				if response.GetBallot() >= p.ballot {
					p.SetBallot(response.GetBallot())
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

	if numOks > len(p.rpcPeers)/2 {
		return logMap
	}
	return nil
}

func (p *Multipaxos) RunAcceptPhase(ballot int64, index int64,
	command *pb.Command, clientId int64) Result {
	var (
		numRpcs = 0
		numOks  = 0
		leader  = p.id
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
				if response.GetBallot() >= p.ballot {
					p.SetBallot(response.GetBallot())
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
		return Result{Type: Ok, Leader: NoLeader}
	}
	if leader != p.id {
		return Result{Type: SomeElseLeader, Leader: leader}
	}
	return Result{Type: Retry, Leader: NoLeader}
}

func (p *Multipaxos) RunCommitPhase(ballot int64, globalLastExecuted int64) int64 {
	var (
		numRpcs         = 0
		numOks          = 0
		leader          = p.id
		minLastExecuted = p.log.LastExecuted()
		mu              sync.Mutex
	)
	cv := sync.NewCond(&mu)

	request := pb.CommitRequest{
		Ballot:             ballot,
		LastExecuted:       minLastExecuted,
		GlobalLastExecuted: globalLastExecuted,
		Sender:             p.id,
	}

	for i, peer := range p.rpcPeers {
		go func(i int, peer pb.MultiPaxosRPCClient) {
			ctx := context.Background()

			response, err := peer.Commit(ctx, &request)
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
				if response.GetBallot() >= p.ballot {
					p.SetBallot(response.GetBallot())
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

func (p *Multipaxos) Replay(ballot int64, replayLog map[int64]*pb.Instance) {
	if replayLog == nil {
		return
	}

	for index, instance := range replayLog {
		result := p.RunAcceptPhase(ballot, index, instance.GetCommand(),
			instance.GetClientId())
		for result.Type == Retry {
			result = p.RunAcceptPhase(ballot, index, instance.GetCommand(),
				instance.GetClientId())
		}
		if result.Type == SomeElseLeader {
			return
		}
	}
	atomic.StoreInt32(&p.ready, 1)
}

func (p *Multipaxos) Prepare(ctx context.Context,
	request *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if request.GetBallot() >= p.ballot {
		p.SetBallot(request.GetBallot())
		logSlice := p.log.InstancesSinceGlobalLastExecuted()
		responseLogs := make([]*pb.Instance, 0, len(logSlice))
		for _, instance := range logSlice {
			responseLogs = append(responseLogs, instance)
		}
		return &pb.PrepareResponse{
			Type: pb.ResponseType_OK,
			Logs: responseLogs,
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
		p.SetBallot(request.GetInstance().GetBallot())
		//instance := &pb.Instance{
		//	Ballot:   request.GetInstance().GetBallot(),
		//	Index:    request.GetInstance().GetIndex(),
		//	ClientId: request.GetInstance().GetClientId(),
		//	State:    pb.InstanceState_INPROGRESS,
		//	Command:  request.GetInstance().GetCommand(),
		//}
		p.log.Append(request.GetInstance())
		return &pb.AcceptResponse{Type: pb.ResponseType_OK, Ballot: p.ballot}, nil
	}
	return &pb.AcceptResponse{Type: pb.ResponseType_REJECT,
		Ballot: p.ballot}, nil
}

func (p *Multipaxos) Commit(ctx context.Context,
	request *pb.CommitRequest) (*pb.CommitResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	response := &pb.CommitResponse{}
	if request.GetBallot() >= p.ballot {
		atomic.StoreInt64(&p.lastCommit, time.Now().UnixNano()/1e6)
		p.SetBallot(request.GetBallot())
		p.log.CommitUntil(request.GetLastExecuted(), request.GetBallot())
		p.log.TrimUntil(request.GetGlobalLastExecuted())
		response.LastExecuted = p.log.LastExecuted()
		response.Type = pb.ResponseType_OK
	} else {
		response.Ballot = p.ballot
		response.Type = pb.ResponseType_REJECT
	}
	return response, nil
}

// Helper functions
func (p *Multipaxos) NextBallot() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	oldBallot := p.ballot
	p.ballot += RoundIncrement
	p.ballot = (p.ballot & ^IdBits) | p.id
	p.ready = 1
	log.Printf("%v became a leader: ballot: %v -> %v\n", p.id, oldBallot,
		p.ballot)
	p.cvLeader.Signal()
	return p.ballot
}

func (p *Multipaxos) SetBallot(ballot int64) {
	oldId := p.ballot & IdBits
	newId := p.ballot & IdBits
	if (oldId == p.id || oldId == MaxNumPeers) && oldId != newId {
		p.cvFollower.Signal()
	}
	p.ballot = ballot
}

func (p *Multipaxos) Id() int64 {
	return p.id
}

func (p *Multipaxos) Ballot() (int64, int32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.ballot, p.ready
}

func (p *Multipaxos) waitUntilLeader() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.commitThreadRunning == 1 && !IsLeader(p.ballot, p.id) {
		p.cvLeader.Wait()
	}
}

func (p *Multipaxos) waitUntilFollower() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.prepareThreadRunning == 1 && IsLeader(p.ballot, p.id) {
		p.cvFollower.Wait()
	}
}

func (p *Multipaxos) sleepForCommitInterval() {
	time.Sleep(time.Duration(p.CommitInterval) * time.Millisecond)
}

func (p *Multipaxos) sleepForRandomInterval() {
	sleepTime := p.CommitInterval + p.CommitInterval / 2 +
		rand.Int63n(p.CommitInterval / 2)
	time.Sleep(time.Duration(sleepTime) * time.Millisecond)
}

func (p *Multipaxos) Connect() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	for i, addr := range p.addrs {
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			panic("dial error")
		}
		client := pb.NewMultiPaxosRPCClient(conn)
		p.rpcPeers[i] = client
	}
}
