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
	rpcPeers       []*RpcPeer
	mu             sync.Mutex

	cvLeader   *sync.Cond
	cvFollower *sync.Cond

	server             *grpc.Server
	rpcServerRunning   bool
	rpcServerRunningCv *sync.Cond

	prepareThreadRunning uint32
	commitThreadRunning  uint32

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
		rpcPeers:             make([]*RpcPeer, len(config.Peers)),
		rpcServerRunning:     false,
		prepareThreadRunning: 0,
		commitThreadRunning:  0,
		server:               nil,
	}
	paxos.rpcServerRunningCv = sync.NewCond(&paxos.mu)
	paxos.cvFollower = sync.NewCond(&paxos.mu)
	paxos.cvLeader = sync.NewCond(&paxos.mu)
	rand.Seed(paxos.id)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	for id, addr := range config.Peers {
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			panic("dial error")
		}
		client := pb.NewMultiPaxosRPCClient(conn)
		paxos.rpcPeers[id] = NewRpcPeer(int64(id), client)
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
			if p.receivedCommit() {
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
	state := NewPrepareState(p.id)

	request := pb.PrepareRequest{
		Ballot: ballot,
		Sender: p.id,
	}

	for i, peer := range p.rpcPeers {
		go func(i int, peer *RpcPeer) {
			ctx := context.Background()
			response, err := peer.Stub.Prepare(ctx, &request)
			state.Mu.Lock()
			defer state.Mu.Unlock()
			defer state.Cv.Signal()

			state.NumRpcs += 1
			if err != nil {
				return
			}
			if response.GetType() == pb.ResponseType_OK {
				state.NumOks += 1
				receivedInstances := response.GetLogs()
				for _, instance := range receivedInstances {
					consensusLog.Insert(state.Log, instance)
				}
			} else {
				p.mu.Lock()
				if response.GetBallot() >= p.ballot {
					p.SetBallot(response.GetBallot())
					state.Leader = Leader(p.ballot)
				}
				p.mu.Unlock()
			}
		}(i, peer)
	}

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for state.Leader == p.id && state.NumOks <= len(p.rpcPeers) / 2 &&
		state.NumRpcs != len(p.rpcPeers) {
		state.Cv.Wait()
	}

	if state.NumOks > len(p.rpcPeers)/2 {
		return state.Log
	}
	return nil
}

func (p *Multipaxos) RunAcceptPhase(ballot int64, index int64,
	command *pb.Command, clientId int64) Result {
	state := NewAcceptState(p.id)

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
		go func(i int, peer *RpcPeer) {
			ctx := context.Background()
			response, err := peer.Stub.Accept(ctx, &request)
			state.Mu.Lock()
			defer state.Mu.Unlock()
			defer state.Cv.Signal()

			state.NumRpcs += 1
			if err != nil {
				return
			}
			if response.GetType() == pb.ResponseType_OK {
				state.NumOks += 1
			} else {
				p.mu.Lock()
				if response.GetBallot() >= p.ballot {
					p.SetBallot(response.GetBallot())
					state.Leader = Leader(p.ballot)
				}
				p.mu.Unlock()
			}
		}(i, peer)
	}

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for state.Leader == p.id && state.NumOks <= len(p.rpcPeers) / 2 &&
		state.NumRpcs != len(p.rpcPeers) {
		state.Cv.Wait()
	}

	if state.NumOks > len(p.rpcPeers) / 2 {
		p.log.Commit(index)
		return Result{Type: Ok, Leader: NoLeader}
	}
	if state.Leader != p.id {
		return Result{Type: SomeElseLeader, Leader: state.Leader}
	}
	return Result{Type: Retry, Leader: NoLeader}
}

func (p *Multipaxos) RunCommitPhase(ballot int64, globalLastExecuted int64) int64 {
	state := NewCommitState(p.id, p.log.LastExecuted())

	request := pb.CommitRequest{
		Ballot:             ballot,
		LastExecuted:       state.MinLastExecuted,
		GlobalLastExecuted: globalLastExecuted,
		Sender:             p.id,
	}

	for i, peer := range p.rpcPeers {
		go func(i int, peer *RpcPeer) {
			ctx := context.Background()

			response, err := peer.Stub.Commit(ctx, &request)
			state.Mu.Lock()
			defer state.Mu.Unlock()
			defer state.Cv.Signal()

			state.NumRpcs += 1
			if err != nil {
				return
			}
			if response.GetType() == pb.ResponseType_OK {
				state.NumOks += 1
				if response.GetLastExecuted() < state.MinLastExecuted {
					state.MinLastExecuted = response.GetLastExecuted()
				}
			} else {
				p.mu.Lock()
				if response.GetBallot() >= p.ballot {
					p.SetBallot(response.GetBallot())
					state.Leader = Leader(p.ballot)
				}
				p.mu.Unlock()
			}
		}(i, peer)
	}

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for state.Leader == p.id && state.NumRpcs != len(p.rpcPeers) {
		state.Cv.Wait()
	}
	if state.NumOks == len(p.rpcPeers) {
		return state.MinLastExecuted
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

	response := &pb.AcceptResponse{}
	if request.GetInstance().GetBallot() >= p.ballot {
		p.SetBallot(request.GetInstance().GetBallot())
		p.log.Append(request.GetInstance())
		response.Type = pb.ResponseType_OK
		return response, nil
	} else {
		response.Ballot = p.ballot
		response.Type = pb.ResponseType_REJECT
		return response, nil
	}
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

func (p *Multipaxos) receivedCommit() bool {
	return time.Now().UnixNano() /1e6 - p.lastCommit < p.CommitInterval
}

func (p *Multipaxos) Connect(addrs []string) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	for i, addr := range addrs {
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			panic("dial error")
		}
		client := pb.NewMultiPaxosRPCClient(conn)
		p.rpcPeers[i] = NewRpcPeer(int64(i), client)
	}
}
