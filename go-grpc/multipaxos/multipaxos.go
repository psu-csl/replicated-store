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
	return atomic.LoadInt64(&p.ballot)
}

func (p *Multipaxos) NextBallot() int64 {
	nextBallot := p.Ballot()
	nextBallot += RoundIncrement
	nextBallot = (nextBallot & ^IdBits) | p.id
	return nextBallot
}

func (p *Multipaxos) BecomeLeader(newBallot int64, newLastIndex int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	logger.Infof("%v became a leader: ballot: %v -> %v\n", p.id, p.Ballot(),
		newBallot)
	p.log.SetLastIndex(newLastIndex)
	atomic.StoreInt64(&p.ballot, newBallot)
	p.cvLeader.Signal()
}

func (p *Multipaxos) BecomeFollower(newBallot int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if newBallot <= p.Ballot() {
		return
	}

	oldLeaderId := ExtractLeaderId(p.Ballot())
	newLeaderId := ExtractLeaderId(newBallot)
	if newLeaderId != p.id && (oldLeaderId == p.id || oldLeaderId == MaxNumPeers) {
		logger.Infof("%v became a follower: ballot: %v -> %v\n", p.id,
			p.Ballot(), newBallot)
		p.cvFollower.Signal()
	}
	atomic.StoreInt64(&p.ballot, newBallot)
}

func (p *Multipaxos) sleepForCommitInterval() {
	time.Sleep(time.Duration(p.commitInterval) * time.Millisecond)
}

func (p *Multipaxos) sleepForRandomInterval() {
	sleepTime := p.commitInterval + p.commitInterval/2 +
		rand.Int63n(p.commitInterval/2)
	time.Sleep(time.Duration(sleepTime) * time.Millisecond)
}

func (p *Multipaxos) receivedCommit() bool {
	return atomic.CompareAndSwapInt32(&p.commitReceived, 1, 0)
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
		for atomic.LoadInt32(&p.prepareThreadRunning) == 1 && IsLeader(p.Ballot(), p.id) {
			p.cvFollower.Wait()
		}
		p.mu.Unlock()

		for atomic.LoadInt32(&p.prepareThreadRunning) == 1 {
			p.sleepForRandomInterval()
			if p.receivedCommit() {
				continue
			}
			nextBallot := p.NextBallot()
			maxLastIndex, log := p.RunPreparePhase(nextBallot)
			if log != nil {
				p.BecomeLeader(nextBallot, maxLastIndex)
				p.Replay(nextBallot, log)
				break
			}
		}
	}
}

func (p *Multipaxos) CommitThread() {
	for atomic.LoadInt32(&p.commitThreadRunning) == 1 {
		p.mu.Lock()
		for atomic.LoadInt32(&p.commitThreadRunning) == 1 && !IsLeader(p.Ballot(), p.id) {
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
	state := NewPrepareState()

	request := pb.PrepareRequest{
		Sender: p.id,
		Ballot: ballot,
	}

	if ballot > p.Ballot() {
		state.NumRpcs++
		state.NumOks++
		state.Log = p.log.GetLog()
		state.MaxLastIndex = p.log.LastIndex()
	} else {
		return -1, nil
	}

	for _, peer := range p.rpcPeers {
		if peer.Id == p.id {
			continue
		}
		go func(peer *RpcPeer) {
			ctx := context.Background()
			response, err := peer.Stub.Prepare(ctx, &request)
			logger.Infof("%v sent prepare request to %v", p.id, peer.Id)

			state.Mu.Lock()
			defer state.Mu.Unlock()

			state.NumRpcs += 1
			if err == nil {
				if response.GetType() == pb.ResponseType_OK {
					state.NumOks += 1
					for i := 0; i < len(response.GetLogs()); i++ {
						instance := response.GetLogs()[i]
						if instance.Index > state.MaxLastIndex {
							state.MaxLastIndex = instance.Index
						}
						Log.Insert(state.Log, instance)
					}
				} else {
					p.BecomeFollower(response.GetBallot())
				}
			}
			state.Cv.Signal()
		}(peer)
	}

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for state.NumOks <= len(p.rpcPeers)/2 && state.NumRpcs != len(p.rpcPeers) {
		state.Cv.Wait()
	}

	if state.NumOks > len(p.rpcPeers)/2 {
		return state.MaxLastIndex, state.Log
	}
	return -1, nil
}

func (p *Multipaxos) RunAcceptPhase(ballot int64, index int64,
	command *pb.Command, clientId int64) Result {
	state := NewAcceptState()

	instance := pb.Instance{
		Ballot:   ballot,
		Index:    index,
		ClientId: clientId,
		State:    pb.InstanceState_INPROGRESS,
		Command:  command,
	}

	if ballot == p.Ballot() {
		instance := pb.Instance{
			Ballot:   ballot,
			Index:    index,
			ClientId: clientId,
			State:    pb.InstanceState_INPROGRESS,
			Command:  command,
		}
		state.NumRpcs++
		state.NumOks++
		p.log.Append(&instance)
	} else {
		return Result{SomeElseLeader, ExtractLeaderId(p.Ballot())}
	}

	request := pb.AcceptRequest{
		Sender:   p.id,
		Instance: &instance,
	}

	for _, peer := range p.rpcPeers {
		if peer.Id == p.id {
			continue
		}
		go func(peer *RpcPeer) {
			ctx := context.Background()
			response, err := peer.Stub.Accept(ctx, &request)
			logger.Infof("%v sent accept request to %v", p.id, peer.Id)

			state.Mu.Lock()
			defer state.Mu.Unlock()

			state.NumRpcs += 1
			if err == nil {
				if response.GetType() == pb.ResponseType_OK {
					state.NumOks += 1
				} else {
					p.BecomeFollower(response.GetBallot())
				}
			}
			state.Cv.Signal()
		}(peer)
	}

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for IsLeader(p.Ballot(), p.id) && state.NumOks <= len(p.rpcPeers)/2 &&
		state.NumRpcs != len(p.rpcPeers) {
		state.Cv.Wait()
	}

	if state.NumOks > len(p.rpcPeers)/2 {
		p.log.Commit(index)
		return Result{Type: Ok, Leader: -1}
	}
	if !IsLeader(p.Ballot(), p.id) {
		return Result{Type: SomeElseLeader, Leader: ExtractLeaderId(p.Ballot())}
	}
	return Result{Type: Retry, Leader: -1}
}

func (p *Multipaxos) RunCommitPhase(ballot int64, globalLastExecuted int64) int64 {
	state := NewCommitState(p.log.LastExecuted())

	request := pb.CommitRequest{
		Ballot:             ballot,
		Sender:             p.id,
		LastExecuted:       state.MinLastExecuted,
		GlobalLastExecuted: globalLastExecuted,
	}

	state.NumRpcs++
	state.NumOks++
	state.MinLastExecuted = p.log.LastExecuted()
	p.log.TrimUntil(globalLastExecuted)

	for _, peer := range p.rpcPeers {
		if peer.Id == p.id {
			continue
		}
		go func(peer *RpcPeer) {
			ctx := context.Background()

			response, err := peer.Stub.Commit(ctx, &request)
			logger.Infof("%v sent commit request to %v", p.id, peer.Id)

			state.Mu.Lock()
			defer state.Mu.Unlock()

			state.NumRpcs += 1
			if err == nil {
				if response.GetType() == pb.ResponseType_OK {
					state.NumOks += 1
					if response.GetLastExecuted() < state.MinLastExecuted {
						state.MinLastExecuted = response.GetLastExecuted()
					}
				} else {
					p.BecomeFollower(response.GetBallot())
				}
			}
			state.Cv.Signal()
		}(peer)
	}

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for IsLeader(p.Ballot(), p.id) && state.NumRpcs != len(p.rpcPeers) {
		state.Cv.Wait()
	}
	if state.NumOks == len(p.rpcPeers) {
		return state.MinLastExecuted
	}
	return globalLastExecuted
}

func (p *Multipaxos) Replay(ballot int64, log map[int64]*pb.Instance) {
	for index, instance := range log {
		r := p.RunAcceptPhase(ballot, index, instance.GetCommand(),
			instance.GetClientId())
		for r.Type == Retry {
			r = p.RunAcceptPhase(ballot, index, instance.GetCommand(),
				instance.GetClientId())
		}
		if r.Type == SomeElseLeader {
			return
		}
	}
}

func (p *Multipaxos) Prepare(ctx context.Context,
	request *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	logger.Infof("%v <--prepare-- %v", p.id, request.GetSender())
	response := &pb.PrepareResponse{}
	if request.GetBallot() > p.Ballot() {
		p.BecomeFollower(request.GetBallot())
		response.Logs = make([]*pb.Instance, 0, len(p.log.Instances()))
		for _, i := range p.log.Instances() {
			response.Logs = append(response.Logs, i)
		}
		response.Type = pb.ResponseType_OK
	} else {
		response.Ballot = p.Ballot()
		response.Type = pb.ResponseType_REJECT
	}
	return response, nil
}

func (p *Multipaxos) Accept(ctx context.Context,
	request *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	logger.Infof("%v <--accept-- %v", p.id, request.GetSender())
	response := &pb.AcceptResponse{}
	if request.GetInstance().GetBallot() >= p.Ballot() {
		p.log.Append(request.GetInstance())
		response.Type = pb.ResponseType_OK
		if request.GetInstance().GetBallot() > p.Ballot() {
			p.BecomeFollower(request.GetInstance().GetBallot())
		}
	}
	if request.GetInstance().GetBallot() < p.Ballot() {
		response.Ballot = p.Ballot()
		response.Type = pb.ResponseType_REJECT
	}
	return response, nil
}

func (p *Multipaxos) Commit(ctx context.Context,
	request *pb.CommitRequest) (*pb.CommitResponse, error) {
	logger.Infof("%v <--commit-- %v", p.id, request.GetSender())
	response := &pb.CommitResponse{}
	if request.GetBallot() >= p.Ballot() {
		atomic.StoreInt32(&p.commitReceived, 1)
		p.log.CommitUntil(request.GetLastExecuted(), request.GetBallot())
		p.log.TrimUntil(request.GetGlobalLastExecuted())
		response.LastExecuted = p.log.LastExecuted()
		response.Type = pb.ResponseType_OK
		if request.GetBallot() > p.Ballot() {
			p.BecomeFollower(request.GetBallot())
		}
	} else {
		response.Ballot = p.Ballot()
		response.Type = pb.ResponseType_REJECT
	}
	return response, nil
}
