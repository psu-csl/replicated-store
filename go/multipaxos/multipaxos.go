package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/config"
	Log "github.com/psu-csl/replicated-store/go/log"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
	logger "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Multipaxos struct {
	ballot         int64
	log            *Log.Log
	id             int64
	commitReceived int32
	commitInterval int64
	port           string
	rpcPeers       []*RpcPeer
	mu             sync.Mutex

	cvLeader   *sync.Cond
	cvFollower *sync.Cond

	rpcServer          *grpc.Server
	rpcServerRunning   bool
	rpcServerRunningCv *sync.Cond

	prepareThreadRunning int32
	commitThreadRunning  int32

	pb.UnimplementedMultiPaxosRPCServer
}

func NewMultipaxos(log *Log.Log, config config.Config) *Multipaxos {
	multipaxos := Multipaxos{
		ballot:               MaxNumPeers,
		log:                  log,
		id:                   config.Id,
		commitReceived:       0,
		commitInterval:       config.CommitInterval,
		port:                 config.Peers[config.Id],
		rpcPeers:             make([]*RpcPeer, len(config.Peers)),
		rpcServerRunning:     false,
		prepareThreadRunning: 0,
		commitThreadRunning:  0,
		rpcServer:            nil,
	}
	multipaxos.rpcServerRunningCv = sync.NewCond(&multipaxos.mu)
	multipaxos.cvFollower = sync.NewCond(&multipaxos.mu)
	multipaxos.cvLeader = sync.NewCond(&multipaxos.mu)
	rand.Seed(multipaxos.id)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	for id, addr := range config.Peers {
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			panic("dial error")
		}
		client := pb.NewMultiPaxosRPCClient(conn)
		multipaxos.rpcPeers[id] = NewRpcPeer(int64(id), client)
	}

	return &multipaxos
}

func (p *Multipaxos) Id() int64 {
	return p.id
}

func (p *Multipaxos) Ballot() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.ballot
}

func (p *Multipaxos) NextBallot() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	nextBallot := p.ballot
	nextBallot += RoundIncrement
	nextBallot = (nextBallot & ^IdBits) | p.id
	return nextBallot
}

func (p *Multipaxos) BecomeLeader(newBallot int64, newLastIndex int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	logger.Infof("%v became a leader: ballot: %v -> %v\n", p.id, p.ballot,
		newBallot)
	p.ballot = newBallot
	p.log.SetLastIndex(newLastIndex)
	p.cvLeader.Signal()
}

func (p *Multipaxos) BecomeFollower(newBallot int64) {
	oldLeaderId := ExtractLeaderId(p.ballot)
	newLeaderId := ExtractLeaderId(newBallot)
	if newLeaderId != p.id && (oldLeaderId == p.id || oldLeaderId == MaxNumPeers) {
		logger.Infof("%v became a follower: ballot: %v -> %v\n", p.id,
			p.ballot, newBallot)
		p.cvFollower.Signal()
	}
	p.ballot = newBallot
}

func (p *Multipaxos) sleepForCommitInterval() {
	time.Sleep(time.Duration(p.commitInterval) * time.Millisecond)
}

func (p *Multipaxos) sleepForRandomInterval() {
	sleepTime := p.commitInterval + p.commitInterval/ 2 +
		rand.Int63n(p.commitInterval/ 2)
	logger.Debug(sleepTime)
	time.Sleep(time.Duration(sleepTime) * time.Millisecond)
}

func (p *Multipaxos) receivedCommit() bool {
	var t int32 = 1
	return atomic.CompareAndSwapInt32(&p.commitReceived, t, 0)
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
	logger.Infof("%v starting rpc server at %v", p.id, p.port)
	listener, err := net.Listen("tcp", p.port)
	if err != nil {
		panic(err)
	}
	p.rpcServer = grpc.NewServer()
	pb.RegisterMultiPaxosRPCServer(p.rpcServer, p)

	p.mu.Lock()
	p.rpcServerRunning = true
	p.rpcServerRunningCv.Signal()
	p.mu.Unlock()

	go p.rpcServer.Serve(listener)
}

func (p *Multipaxos) StopRPCServer() {
	p.mu.Lock()
	for !p.rpcServerRunning {
		p.rpcServerRunningCv.Wait()
	}
	logger.Infof("%v stopping rpc server at %v", p.id, p.port)
	p.mu.Unlock()
	p.rpcServer.GracefulStop()
}

func (p *Multipaxos) StartPrepareThread() {
	logger.Infof("%v starting prepare thread", p.id)
	if p.prepareThreadRunning == 1 {
		panic("prepareThreadRunning is true")
	}
	atomic.StoreInt32(&p.prepareThreadRunning, 1)
	go p.PrepareThread()
}

func (p *Multipaxos) StopPrepareThread() {
	logger.Infof("%v stopping prepare thread", p.id)
	if p.prepareThreadRunning == 0 {
		panic("prepareThreadRunning is false")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	atomic.StoreInt32(&p.prepareThreadRunning, 0)
	p.cvFollower.Signal()
}

func (p *Multipaxos) StartCommitThread() {
	logger.Infof("%v starting commit thread", p.id)
	if p.commitThreadRunning == 1 {
		panic("commitThreadRunning is true")
	}
	atomic.StoreInt32(&p.commitThreadRunning, 1)
	go p.CommitThread()
}

func (p *Multipaxos) StopCommitThread() {
	logger.Infof("%v stopping commit thread", p.id)
	if p.commitThreadRunning == 0 {
		panic("commitThreadRunning is false")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	atomic.StoreInt32(&p.commitThreadRunning, 0)
	p.cvLeader.Signal()
}

func (p *Multipaxos) Replicate(command *pb.Command, clientId int64) Result {
	ballot := p.Ballot()
	if IsLeader(ballot, p.id) {
			return p.RunAcceptPhase(ballot, p.log.AdvanceLastIndex(), command,
				clientId)
	}
	if IsSomeoneElseLeader(ballot, p.id) {
		return Result{Type: SomeElseLeader, Leader: ExtractLeaderId(ballot)}
	}
	return Result{Type: Retry, Leader: -1}
}

func (p *Multipaxos) PrepareThread() {
	for atomic.LoadInt32(&p.prepareThreadRunning) == 1 {
		p.mu.Lock()
		for atomic.LoadInt32(&p.prepareThreadRunning) == 1 && IsLeader(p.ballot, p.id) {
			p.cvFollower.Wait()
		}
		p.mu.Unlock()

		for atomic.LoadInt32(&p.prepareThreadRunning) == 1 {
			p.sleepForRandomInterval()
			if p.receivedCommit() {
				continue
			}
			nextBallot := p.NextBallot()
			lastIndex, replayLog := p.RunPreparePhase(nextBallot)
			if replayLog != nil {
				p.BecomeLeader(nextBallot, lastIndex)
				p.Replay(nextBallot, replayLog)
				break
			}
		}
	}
}

func (p *Multipaxos) CommitThread() {
	for atomic.LoadInt32(&p.commitThreadRunning) == 1 {
		p.mu.Lock()
		for atomic.LoadInt32(&p.commitThreadRunning) == 1 && !IsLeader(p.ballot, p.id) {
			p.cvLeader.Wait()
		}
		p.mu.Unlock()

		gle := p.log.GlobalLastExecuted()
		for atomic.LoadInt32(&p.commitThreadRunning) == 1 {
			ballot := p.Ballot()
			if !IsLeader(ballot, p.id) {
				break
			}
			gle = p.RunCommitPhase(ballot, gle)
			p.sleepForCommitInterval()
		}
	}
}

func (p *Multipaxos) RunPreparePhase(ballot int64) (int64,
	map[int64]*pb.Instance) {
	state := NewPrepareState(p.id)

	request := pb.PrepareRequest{
		Ballot: ballot,
		Sender: p.id,
	}

	for _, peer := range p.rpcPeers {
		go func(peer *RpcPeer) {
			ctx := context.Background()
			response, err := peer.Stub.Prepare(ctx, &request)
			logger.Infof("%v sent prepare request to %v", p.id, peer.Id)

			state.Mu.Lock()
			defer state.Mu.Unlock()
			defer state.Cv.Signal()

			state.NumRpcs += 1
			if err == nil {
				if response.GetType() == pb.ResponseType_OK {
					state.NumOks += 1
					receivedInstances := response.GetLogs()
					for _, instance := range receivedInstances {
						if instance.Index > state.LastIndex {
							state.LastIndex = instance.Index
						}
						Log.Insert(state.Log, instance)
					}
				} else {
					p.mu.Lock()
					if response.GetBallot() > p.ballot {
						p.BecomeFollower(response.GetBallot())
						state.Leader = ExtractLeaderId(p.ballot)
					}
					p.mu.Unlock()
				}
			}
		}(peer)
	}

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for state.Leader == p.id && state.NumOks <= len(p.rpcPeers) / 2 &&
		state.NumRpcs != len(p.rpcPeers) {
		state.Cv.Wait()
	}

	if state.NumOks > len(p.rpcPeers)/2 {
		return state.LastIndex, state.Log
	}
	return -1, nil
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

	for _, peer := range p.rpcPeers {
		go func(peer *RpcPeer) {
			ctx := context.Background()
			response, err := peer.Stub.Accept(ctx, &request)
			logger.Infof("%v sent accept request to %v", p.id, peer.Id)

			state.Mu.Lock()
			defer state.Mu.Unlock()
			defer state.Cv.Signal()

			state.NumRpcs += 1
			if err == nil {
				if response.GetType() == pb.ResponseType_OK {
					state.NumOks += 1
				} else {
					p.mu.Lock()
					if response.GetBallot() > p.ballot {
						p.BecomeFollower(response.GetBallot())
						state.Leader = ExtractLeaderId(p.ballot)
					}
					p.mu.Unlock()
				}
			}
		}(peer)
	}

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for state.Leader == p.id && state.NumOks <= len(p.rpcPeers) / 2 &&
		state.NumRpcs != len(p.rpcPeers) {
		state.Cv.Wait()
	}

	if state.NumOks > len(p.rpcPeers) / 2 {
		p.log.Commit(index)
		return Result{Type: Ok, Leader: -1}
	}
	if state.Leader != p.id {
		return Result{Type: SomeElseLeader, Leader: state.Leader}
	}
	return Result{Type: Retry, Leader: -1}
}

func (p *Multipaxos) RunCommitPhase(ballot int64, globalLastExecuted int64) int64 {
	state := NewCommitState(p.id, p.log.LastExecuted())

	request := pb.CommitRequest{
		Ballot:             ballot,
		LastExecuted:       state.MinLastExecuted,
		GlobalLastExecuted: globalLastExecuted,
		Sender:             p.id,
	}

	for _, peer := range p.rpcPeers {
		go func(peer *RpcPeer) {
			ctx := context.Background()

			response, err := peer.Stub.Commit(ctx, &request)
			logger.Infof("%v sent commit request to %v", p.id, peer.Id)

			state.Mu.Lock()
			defer state.Mu.Unlock()
			defer state.Cv.Signal()

			state.NumRpcs += 1
			if err == nil {
				if response.GetType() == pb.ResponseType_OK {
					state.NumOks += 1
					if response.GetLastExecuted() < state.MinLastExecuted {
						state.MinLastExecuted = response.GetLastExecuted()
					}
				} else {
					p.mu.Lock()
					if response.GetBallot() > p.ballot {
						p.BecomeFollower(response.GetBallot())
						state.Leader = ExtractLeaderId(p.ballot)
					}
					p.mu.Unlock()
				}
			}
		}(peer)
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

func (p *Multipaxos) Replay(ballot int64, log map[int64]*pb.Instance) {
	for index, instance := range log {
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
}

func (p *Multipaxos) Prepare(ctx context.Context,
	request *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	logger.Infof("%v <--prepare-- %v", p.id, request.GetSender())
	response := &pb.PrepareResponse{}
	if request.GetBallot() > p.ballot {
		p.BecomeFollower(request.GetBallot())
		logSlice := p.log.Instances()
		response.Logs = make([]*pb.Instance, 0, len(logSlice))
		for _, i := range logSlice {
			response.Logs = append(response.Logs, i)
		}
		response.Type = pb.ResponseType_OK
	} else {
		response.Type = pb.ResponseType_REJECT
		response.Ballot = p.ballot
	}
	return response, nil
}

func (p *Multipaxos) Accept(ctx context.Context,
	request *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	logger.Infof("%v <--accept-- %v", p.id, request.GetSender())
	response := &pb.AcceptResponse{}
	if request.GetInstance().GetBallot() >= p.ballot {
		p.BecomeFollower(request.GetInstance().GetBallot())
		p.log.Append(request.GetInstance())
		response.Type = pb.ResponseType_OK
	} else {
		response.Ballot = p.ballot
		response.Type = pb.ResponseType_REJECT
	}
	return response, nil
}

func (p *Multipaxos) Commit(ctx context.Context,
	request *pb.CommitRequest) (*pb.CommitResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	logger.Infof("%v <--commit-- %v", p.id, request.GetSender())
	response := &pb.CommitResponse{}
	if request.GetBallot() >= p.ballot {
		atomic.StoreInt32(&p.commitReceived, 1)
		p.BecomeFollower(request.GetBallot())
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
