package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	consensusLog "github.com/psu-csl/replicated-store/go/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	running            int32
	ready              bool
	ballot             int64
	log                *consensusLog.Log
	id                 int64
	heartbeatInterval  int64
	heartbeatDelta     int64
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
		heartbeatDelta:     config.HeartbeatDelta,
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

func (p *Multipaxos) Start() {
	if p.running == 1 {
		panic("running is true before Start()")
	}
	p.StartThreads()
	p.StartServer()
	p.Connect()
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

func (p *Multipaxos) PrepareThread() {
	for p.running == 1 {
		p.waitUntilFollower()
		for p.running == 1 && !p.IsLeader() {
			sleepTime := p.heartbeatInterval + p.heartbeatDelta +
				rand.Int63n(p.heartbeatInterval-p.heartbeatDelta)
			time.Sleep(time.Duration(sleepTime))
			if time.Now().UnixNano()/1e6-p.lastHeartbeat <
				p.heartbeatInterval {
				continue
			}

			p.prepareRequest.Ballot = p.NextBallot()
			for i, peer := range p.rpcPeers {
				go func(i int, peer pb.MultiPaxosRPCClient) {
					ctx, cancel := context.WithTimeout(context.Background(),
						RPCTimeout * time.Millisecond)
					defer cancel()

					response, err := peer.Prepare(ctx, &p.prepareRequest)
					p.prepareMutex.Lock()
					defer p.prepareMutex.Unlock()
					defer p.prepareCV.Signal()

					p.prepareNumRpcs += 1
					if err != nil {
						return
					}
					if response.Type == pb.ResponseType_OK {
						p.prepareOkResponses = append(p.prepareOkResponses,
							response.GetLogs())
					} else {
						p.mu.Lock()
						if response.GetBallot() > p.ballot {
							p.setBallot(response.GetBallot())
						}
						p.mu.Unlock()
					}
				}(i, peer)
			}

			p.prepareMutex.Lock()
			for p.IsLeader() && len(p.prepareOkResponses) <= len(p.
				rpcPeers) / 2 && p.prepareNumRpcs != len(p.rpcPeers) {
				p.prepareCV.Wait()
			}

			p.prepareNumRpcs = 0
			if len(p.prepareOkResponses) <= len(p.rpcPeers) / 2 ||
				p.IsSomeoneElseLeader() {
				p.prepareOkResponses = make([][]*pb.Instance, len(p.rpcPeers))
				p.prepareMutex.Unlock()
				continue
			}

			p.mu.Lock()
			replayLog := p.merge(p.prepareOkResponses)
			if p.replay(replayLog) {
				p.ready = true
			}
			p.mu.Unlock()
			p.prepareOkResponses = make([][]*pb.Instance, len(p.rpcPeers))
			p.prepareMutex.Unlock()
			break
		}
	}
}

func (p *Multipaxos) HeartbeatThread() {
	for p.running == 1 {
		p.waitUntilLeader()

		globalLastExecuted := p.log.GlobalLastExecuted()
		for p.running == 1 && p.IsLeader() {
			p.mu.Lock()
			p.heartbeatRequest.Ballot = p.ballot
			p.mu.Unlock()
			p.heartbeatRequest.LastExecuted = p.log.LastExecuted()
			p.heartbeatRequest.GlobalLastExecuted = globalLastExecuted

			for i, peer := range p.rpcPeers {
				go func(i int, peer pb.MultiPaxosRPCClient) {
					ctx, cancel := context.WithTimeout(context.Background(),
						RPCTimeout * time.Millisecond)
					defer cancel()

					response, err := peer.Heartbeat(ctx, &p.heartbeatRequest)
					p.heartbeatMutex.Lock()
					defer p.heartbeatMutex.Unlock()
					defer p.heartbeatCv.Signal()

					p.heartbeatNumRpcs += 1
					if err != nil {
						return
					}
					p.heartbeatResponses = append(p.heartbeatResponses, response)
				}(i, peer)
			}

			p.heartbeatMutex.Lock()
			for p.IsLeader() && p.heartbeatNumRpcs != len(p.rpcPeers) {
				p.heartbeatCv.Wait()
			}
			if len(p.heartbeatResponses) == len(p.rpcPeers) {
				min := p.heartbeatResponses[0].GetLastExecuted()
				for i := 1; i < len(p.heartbeatResponses); i++ {
					if p.heartbeatResponses[i].GetLastExecuted() < min {
						min = p.heartbeatResponses[i].GetLastExecuted()
					}
				}
				globalLastExecuted = min
			}
			// Clear info from previous round
			p.heartbeatNumRpcs = 0
			p.heartbeatResponses = make([]*pb.HeartbeatResponse, len(p.rpcPeers))
			p.heartbeatMutex.Unlock()
			time.Sleep(time.Duration(p.heartbeatInterval))
		}
	}
}

func (p *Multipaxos) Replicate(cmd *pb.Command, clientId int64) AcceptResult {
	p.mu.Lock()
	if p.IsLeaderLockless() {
		if p.ready {
			request := pb.AcceptRequest{
				Ballot:   p.ballot,
				Command:  cmd,
				Index:    p.log.AdvanceLastIndex(),
				ClientId: clientId,
			}
			p.mu.Unlock()
			return p.Accept(&request)
		}
		p.mu.Unlock()
		return AcceptResult{Type: Retry, Leader: -1}
	}
	p.mu.Unlock()
	if p.IsSomeoneElseLeader() {
		return AcceptResult{Type: Reject, Leader: p.Leader()}
	}
	return AcceptResult{Type: Retry, Leader: -1}
}

func (p *Multipaxos) Accept(request *pb.AcceptRequest) AcceptResult {
	var (
		numResponses   = 0
		numOkResponses = 0
		mu             sync.Mutex
	)
	cv := sync.NewCond(&mu)
	ctx := context.Background()

	for i, peer := range p.rpcPeers {
		go func(i int, peer pb.MultiPaxosRPCClient) {
			response, err := peer.AcceptHandler(ctx, request)
			mu.Lock()
			defer mu.Unlock()
			defer cv.Signal()

			numResponses += 1
			if err != nil {
				return
			}
			if response.Type == pb.ResponseType_OK {
				numOkResponses += 1
			} else if response.Type == pb.ResponseType_REJECT {
				p.mu.Lock()
				if response.GetBallot() > p.ballot {
					p.setBallot(response.GetBallot())
				}
				p.mu.Unlock()
			}
		}(i, peer)
	}

	mu.Lock()
	defer mu.Unlock()
	for p.IsLeader() && numOkResponses <= len(p.rpcPeers) / 2 &&
		numResponses != len(p.rpcPeers) {
		cv.Wait()
	}

	if numOkResponses > len(p.rpcPeers) / 2 {
		p.log.Commit(request.Index)
		return AcceptResult{Type: Ok, Leader: -1}
	}
	if p.IsSomeoneElseLeader() {
		return AcceptResult{Type: Reject, Leader: p.Leader()}
	}
	return AcceptResult{Type: Retry, Leader: -1}
}

func (p *Multipaxos) Prepare(ctx context.Context,
	msg *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if msg.GetBallot() >= p.ballot {
		p.setBallot(msg.GetBallot())
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

func (p *Multipaxos) Heartbeat(ctx context.Context,
	msg *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if msg.GetBallot() >= p.ballot {
		atomic.StoreInt64(&p.lastHeartbeat, time.Now().UnixNano() / 1e6)
		p.setBallot(msg.GetBallot())
		p.log.CommitUntil(msg.GetLastExecuted(), msg.GetBallot())
		p.log.TrimUntil(msg.GetGlobalLastExecuted())
	}
	return &pb.HeartbeatResponse{LastExecuted: p.log.LastExecuted()}, nil
}

func (p *Multipaxos) AcceptHandler(ctx context.Context,
	msg *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if msg.GetBallot() >= p.ballot {
		p.setBallot(msg.GetBallot())
		instance := &pb.Instance{
			Ballot:   msg.GetBallot(),
			Index:    msg.GetIndex(),
			ClientId: msg.GetClientId(),
			State:    pb.InstanceState_INPROGRESS,
			Command:  msg.GetCommand(),
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

func (p *Multipaxos) merge(logs [][]*pb.Instance) map[int64]*pb.Instance {
	replayLog := make(map[int64]*pb.Instance)
	for _, slice := range logs {
		for _, instance := range slice {
			consensusLog.Insert(replayLog, instance)
		}
	}
	return replayLog
}

func (p *Multipaxos) replay(replayLog map[int64]*pb.Instance) bool {
	for index, instance := range replayLog {
		p.mu.Lock()
		request := &pb.AcceptRequest{
			Ballot:   p.ballot,
			Command: instance.GetCommand(),
			Index:    index,
			ClientId: instance.GetClientId(),
		}
		p.mu.Unlock()

		result := p.Accept(request)
		if result.Type == Reject {
			break
		} else if result.Type == Retry {
			continue
		}
	}
	return true
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

// Functions for testing
func (p *Multipaxos) Id() int64 {
	return p.id
}

func (p *Multipaxos) Ballot() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ballot
}
