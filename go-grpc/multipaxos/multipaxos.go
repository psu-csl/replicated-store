package multipaxos

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/psu-csl/replicated-store/go/config"
	Log "github.com/psu-csl/replicated-store/go/log"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
	logger "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Multipaxos struct {
	ballot         int64
	log            *Log.Log
	id             int64
	commitReceived int64
	commitInterval int64
	port           string
	rpcPeers       RpcPeerList
	mu             sync.Mutex

	initCommitInterval int64
	lastElectedTime    time.Time
	numElections       int64
	electionThreshold  int64
	frequencyThreshold int64

	cvLeader   *sync.Cond
	cvFollower *sync.Cond

	rpcServer          *grpc.Server
	rpcServerRunning   bool
	rpcServerRunningCv *sync.Cond

	prepareThreadRunning int32
	commitThreadRunning  int32
	joinReady            bool

	OverloadedTime     int64
	singleWindow       *Queue
	detector           *OverloadedDetector
	overloadedWorkload float64
	overloadedFlag     int64

	pb.UnimplementedMultiPaxosRPCServer
}

func NewMultipaxos(log *Log.Log, config config.Config, join bool) *Multipaxos {
	multipaxos := Multipaxos{
		ballot:         MaxNumPeers,
		log:            log,
		id:             config.Id,
		commitReceived: 0,
		commitInterval: config.CommitInterval,
		port:           config.Peers[config.Id],
		rpcPeers: RpcPeerList{
			List: make([]*RpcPeer, len(config.Peers)),
		},
		initCommitInterval:   config.CommitInterval,
		lastElectedTime:      time.Now(),
		numElections:         0,
		electionThreshold:    config.ElectionLimit,
		frequencyThreshold:   config.Threshold,
		rpcServerRunning:     false,
		prepareThreadRunning: 0,
		commitThreadRunning:  0,
		joinReady:            !join,
		OverloadedTime:       0,
		rpcServer:            nil,
		singleWindow:         NewQueue(config.WindowSize),
		detector: NewOverloadedDetector(config.QueueSize, config.UpThreshold,
			config.DownThreshold, DRIFT),
		overloadedWorkload: 0,
		overloadedFlag:     0,
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
		multipaxos.rpcPeers.List[id] = NewRpcPeer(int64(id), client)
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

	tp := time.Now().UnixNano()
	logger.Errorf("time: %v, %v became a leader: ballot: %v -> %v, "+
		"initialized number of elections: %v\n", tp, p.id, p.Ballot(),
		newBallot, p.numElections)
	p.log.SetLastIndex(newLastIndex)
	atomic.StoreInt64(&p.ballot, newBallot)
	if p.commitReceived > 1 {
		atomic.StoreInt64(&p.OverloadedTime, p.commitReceived)
		p.commitReceived = 0
	}
	p.cvLeader.Broadcast()
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
		if p.numElections > p.frequencyThreshold {
			p.commitInterval *= 2
		}
		p.overloadedFlag = 0
		p.cvFollower.Signal()
	}
	atomic.StoreInt64(&p.ballot, newBallot)
	atomic.StoreInt64(&p.commitReceived, 1)
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
	return atomic.CompareAndSwapInt64(&p.commitReceived, 1, 0)
}

func (p *Multipaxos) Start() {
	p.mu.Lock()
	for !p.joinReady {
		p.cvLeader.Wait()
	}
	p.mu.Unlock()
	p.StartPrepareThread()
	p.StartCommitThread()
	p.StartRPCServer()
	go p.MonitorThread()
}

func (p *Multipaxos) Stop() {
	p.StopRPCServer()
	p.StopPrepareThread()
	p.StopCommitThread()
}

func (p *Multipaxos) StartRPCServer() {
	p.mu.Lock()
	if p.rpcServerRunning {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
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
	p.cvLeader.Broadcast()
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
			maxLastIndex, _ := p.RunPreparePhase(nextBallot)
			p.countElection()
			if maxLastIndex != -1 {
				p.BecomeLeader(nextBallot, maxLastIndex)
				p.Replay(nextBallot, maxLastIndex)
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
		state.MaxLastIndex = p.log.LastIndex()
	} else {
		return -1, nil
	}

	p.rpcPeers.RLock()
	numPeers := len(p.rpcPeers.List)
	for _, peer := range p.rpcPeers.List {
		if peer.Id == p.id {
			continue
		}
		go func(peer *RpcPeer) {
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Duration(p.commitInterval)*time.Millisecond)
			response, err := peer.Stub.Prepare(ctx, &request)
			logger.Infof("%v sent prepare request to %v", p.id, peer.Id)

			state.Mu.Lock()
			defer state.Mu.Unlock()

			state.NumRpcs += 1
			if err == nil {
				if response.GetType() == pb.ResponseType_OK {
					state.NumOks += 1
					if response.GetLastIndex() > state.MaxLastIndex {
						state.MaxLastIndex = response.GetLastIndex()
					}
				} else {
					p.BecomeFollower(response.GetBallot())
				}
			}
			state.Cv.Signal()
			cancel()
		}(peer)
	}
	p.rpcPeers.RUnlock()

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for state.NumOks <= numPeers/2 && state.NumRpcs != numPeers {
		state.Cv.Wait()
	}

	if state.NumOks > numPeers/2 {
		return state.MaxLastIndex, nil
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

	p.rpcPeers.RLock()
	numPeers := len(p.rpcPeers.List)
	for _, peer := range p.rpcPeers.List {
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
	p.rpcPeers.RUnlock()

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for IsLeader(p.Ballot(), p.id) && state.NumOks <= numPeers/2 &&
		state.NumRpcs != numPeers {
		state.Cv.Wait()
	}

	if state.NumOks > numPeers/2 {
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

	p.rpcPeers.RLock()
	numPeers := len(p.rpcPeers.List)
	for _, peer := range p.rpcPeers.List {
		if peer.Id == p.id {
			continue
		}
		go func(peer *RpcPeer) {
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Duration(p.commitInterval)*time.Millisecond)

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
			cancel()
		}(peer)
	}
	p.rpcPeers.RUnlock()

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for IsLeader(p.Ballot(), p.id) && state.NumRpcs != numPeers {
		state.Cv.Wait()
	}
	if state.NumOks == numPeers {
		return state.MinLastExecuted
	}
	return globalLastExecuted
}

func (p *Multipaxos) Replay(ballot int64, lastIndex int64) {
	state := NewReplayState()
	state.Log = p.log.GetLog()
	state.MaxLastIndex = lastIndex

	request := pb.InstanceRequest{
		LastIndex:    -1,
		LastExecuted: p.log.LastExecuted(),
		Ballot:       ballot,
		Sender:       p.id,
	}

	p.rpcPeers.RLock()
	numPeers := len(p.rpcPeers.List)
	for _, peer := range p.rpcPeers.List {
		if peer.Id == p.id {
			continue
		}
		go func(peer *RpcPeer) {
			ctx := context.Background()
			stream, err := peer.Stub.InstancesGap(ctx, &request)
			state.NumRpcs += 1
			if err != nil {
				logger.Infoln(err)
				state.Cv.Signal()
				return
			}
			for {
				instance, err := stream.Recv()
				if err == io.EOF {
					break
				} else if err != nil || !IsLeader(p.Ballot(), p.id) {
					logger.Error(err)
					break
				}
				if instance.GetIndex() > state.MaxLastIndex {
					state.MaxLastIndex = instance.GetIndex()
				}
				state.Mu.Lock()
				Log.Insert(state.Log, instance)
				state.Mu.Unlock()
			}
			state.NumOks += 1
			state.Cv.Signal()
		}(peer)
	}
	p.rpcPeers.RUnlock()

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for state.NumOks < numPeers {
		state.Cv.Wait()
	}

	for index := request.LastExecuted + 1; index <= state.MaxLastIndex; index++ {
		instance, ok := state.Log[index]
		var cmd *pb.Command
		var clientId int64
		if ok {
			cmd = instance.GetCommand()
			clientId = instance.GetClientId()
		} else {
			cmd = &pb.Command{
				Type: pb.CommandType_GET,
				Key:  "1",
			}
			clientId = -2
		}
		go func(index int64, cmd *pb.Command, clientId int64) {
			r := p.RunAcceptPhase(ballot, index, cmd, clientId)

			for r.Type == Retry {
				r = p.RunAcceptPhase(ballot, index, cmd, clientId)
			}
		}(index, cmd, clientId)
	}
}

func (p *Multipaxos) RequestInstanceGap() {
	leaderId := ExtractLeaderId(p.ballot)
	request := pb.InstanceRequest{
		LastIndex:    p.log.LastIndex(),
		LastExecuted: p.log.LastExecuted(),
		Ballot:       p.Ballot(),
		Sender:       p.id,
	}
	if request.LastIndex == request.LastExecuted {
		request.LastIndex = -1
	}
	stream, err := p.rpcPeers.List[leaderId].Stub.InstancesGap(context.
		Background(), &request)
	if err != nil {
		logger.Error(err)
		return
	}
	//logger.Infof("before - last_index: %v\n", request.LastIndex)
	//count := 0
	for {
		instance, err := stream.Recv()
		//count += 1
		if err == io.EOF {
			break
		} else if err != nil {
			logger.Error(err)
			break
		}
		p.log.Append(instance)
	}
	//logger.Infof("Recovered missing instances, recv: %v\n", count)
}

func (p *Multipaxos) Reconfigure(cmd *pb.Command) {
	peerId, err := strconv.Atoi(cmd.Key)
	if err != nil {
		return
	}

	if cmd.Type == pb.CommandType_ADDNODE {
		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		conn, err := grpc.Dial(cmd.Value, opts...)
		if err != nil {
			logger.Errorln("dial error")
		} else {
			client := pb.NewMultiPaxosRPCClient(conn)
			p.rpcPeers.Lock()
			p.rpcPeers.List = append(p.rpcPeers.List,
				NewRpcPeer(int64(peerId), client))
			p.rpcPeers.Unlock()
			if !IsLeader(p.Ballot(), p.id) {
				return
			}
			stream, err := client.ResumeSnapshot(context.Background())
			if err != nil {
				return
			}
			buffer, err := p.log.MakeSnapshot(p.ballot)
			if err != nil {
				return
			}
			for offset := 0; offset < buffer.Len(); offset += ChunkSize {
				var chunk []byte
				if offset+ChunkSize > buffer.Len() {
					chunk = buffer.Bytes()[offset:]
				} else {
					chunk = buffer.Bytes()[offset : offset+ChunkSize]
				}
				err = stream.Send(&pb.SnapshotRequest{Chunk: chunk})
				if err != nil {
					logger.Error(err)
					return
				}
			}
			_, err = stream.CloseAndRecv()
			if err != nil {
				logger.Error(err)
			}
		}
	} else if cmd.Type == pb.CommandType_DELNODE {
		for i, peer := range p.rpcPeers.List {
			if peer.Id == int64(peerId) {
				p.rpcPeers.Lock()
				p.rpcPeers.List = append(p.rpcPeers.List[:i],
					p.rpcPeers.List[i+1:]...)
				p.rpcPeers.Unlock()
			}
		}
	}
}

func (p *Multipaxos) countElection() {
	elapse := time.Since(p.lastElectedTime)
	p.lastElectedTime = time.Now()
	if elapse.Milliseconds() < p.electionThreshold {
		p.numElections += 1
	} else {
		p.numElections = 1
		if p.commitInterval > p.initCommitInterval {
			p.commitInterval = p.initCommitInterval
		}
	}
}

func (p *Multipaxos) Prepare(ctx context.Context,
	request *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	logger.Infof("%v <--prepare-- %v", p.id, request.GetSender())
	response := &pb.PrepareResponse{}
	if request.GetBallot() > p.Ballot() {
		p.BecomeFollower(request.GetBallot())
		response.LastIndex = p.log.LastIndex()
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
		if atomic.LoadInt64(&p.commitReceived) <= 1 {
			atomic.StoreInt64(&p.commitReceived, 1)
		}
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

func (p *Multipaxos) ResumeSnapshot(stream pb.MultiPaxosRPC_ResumeSnapshotServer) error {
	logger.Infof("%v <--snapshot--\n", p.id)
	buffer := &bytes.Buffer{}
	for {
		snapshotChunk, err := stream.Recv()
		if err == io.EOF {
			snapshot := &Log.Snapshot{}
			err = gob.NewDecoder(buffer).Decode(snapshot)
			if err != nil {
				logger.Error(err)
				return err
			}
			p.BecomeFollower(snapshot.Ballot)
			p.log.ResumeSnapshot(snapshot)
			p.mu.Lock()
			p.joinReady = true
			p.cvLeader.Broadcast()
			p.mu.Unlock()
			go p.RequestInstanceGap()
			return stream.SendAndClose(&pb.SnapshotResponse{Done: true})
		}
		if err != nil {
			logger.Error(err)
			return err
		}
		buffer.Write(snapshotChunk.GetChunk())
	}
}

func (p *Multipaxos) InstancesGap(request *pb.InstanceRequest,
	stream pb.MultiPaxosRPC_InstancesGapServer) error {
	logger.Infof("%v <--instances request-- %v\n", p.id, request.GetSender())
	instances := p.log.InstancesRange(request.GetLastExecuted(),
		request.GetLastIndex())
	ballot := request.GetBallot()
	for _, instance := range instances {
		err := stream.Send(instance)
		if err != nil {
			logger.Error(err)
			return err
		} else if p.Ballot() > ballot {
			return nil
		}
	}
	return nil
}

func (p *Multipaxos) MonitorThread() {
	var (
		numLog                 = 0
		curLastExecuted  int64 = 0
		curGLE           int64 = 0
		prevLastExecuted int64 = 0
		numInflight      int64 = 0
		numExecuted      int64 = 0
	)
	for atomic.LoadInt32(&p.commitThreadRunning) == 1 {
		p.mu.Lock()
		for atomic.LoadInt32(&p.commitThreadRunning) == 1 && !IsLeader(p.Ballot(), p.id) {
			p.cvLeader.Wait()
		}
		p.mu.Unlock()
		for IsLeader(p.Ballot(), p.id) {
			curLastExecuted, curGLE, numLog = p.log.GetIndexes()
			numInflight = int64(numLog) - (curLastExecuted - curGLE)
			numExecuted = curLastExecuted - prevLastExecuted
			prevLastExecuted = curLastExecuted

			if numInflight > 0 || prevLastExecuted > 0 {
				variance := p.singleWindow.AppendAndCalculate(numInflight)
				if variance > 0.0 {
					isChangePoint, posVar, negVar := p.detector.Push(variance,
						float64(numInflight), float64(numExecuted))
					if isChangePoint == 1 && p.overloadedFlag == 0 {
						logger.Errorln("change point signals")
						p.overloadedFlag = 1
					} else if isChangePoint == -1 {
						if p.ResetOverloadedStatus() {
							p.overloadedFlag = 0
						}
					} else if p.overloadedFlag == 1 {
						p.HandoverLeadership(posVar, negVar)
						p.overloadedFlag = 2
					}
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
		p.singleWindow.Clear()
		p.detector.Clear()
	}
}

func (p *Multipaxos) HandoverLeadership(posVarSum, negVarSum float64) {
	if atomic.LoadInt64(&p.OverloadedTime) == 0 {
		value := strconv.FormatFloat(posVarSum, 'f', 3, 64) +
			" " + strconv.FormatFloat(negVarSum, 'f', 3, 64)
		cmd := pb.Command{
			Type:  pb.CommandType_PUT,
			Key:   "overloaded",
			Value: value,
		}
		result := Result{Type: Retry}
		for result.Type == Retry {
			result = p.Replicate(&cmd, -1)
		}
		logger.Errorln("handover leadership due to overloaded")
	}
}

func (p *Multipaxos) ResetOverloadedStatus() bool {
	overloadedTime := atomic.LoadInt64(&p.OverloadedTime)
	if overloadedTime != 0 && time.Now().Unix()-overloadedTime > 10 {
		atomic.StoreInt64(&p.OverloadedTime, 0)
		logger.Errorln("reset overloaded status")
		return true
	}
	return false
}

func (p *Multipaxos) Monitor() {
	length, lastIndex, lastExecuted, gle, indice := p.log.GetLogStatus()
	logger.Errorf("ballot: %v, log len: %v, last_index: %v, "+
		"last_executed: %v, gle: %v, indice: %v, node_list: %v", p.Ballot(),
		length, lastIndex, lastExecuted, gle, indice, p.rpcPeers.List)
}

func (p *Multipaxos) TriggerElection(
	ballot int64,
	posVarSum float64,
	negVarSum float64,
) {
	if !IsLeader(p.Ballot(), p.id) && ballot >= p.Ballot() {
		timestamp := time.Now().Unix()
		atomic.StoreInt64(&p.commitReceived, timestamp)
		p.detector.SetVarSum(posVarSum, negVarSum)
	}
}
