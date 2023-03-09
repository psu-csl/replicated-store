package multipaxos

import (
	"encoding/json"
	"github.com/psu-csl/replicated-store/go/config"
	Log "github.com/psu-csl/replicated-store/go/log"
	tcp "github.com/psu-csl/replicated-store/go/multipaxos/network"
	logger "github.com/sirupsen/logrus"
	"math/rand"
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
	peers          []*Peer
	nextChannelId  uint64
	channels       *tcp.ChannelMap
	mu             sync.Mutex

	cvLeader   *sync.Cond
	cvFollower *sync.Cond

	prepareThreadRunning int32
	commitThreadRunning  int32
}

func NewMultipaxos(log *Log.Log, config config.Config) *Multipaxos {
	multipaxos := Multipaxos{
		ballot:               MaxNumPeers,
		log:                  log,
		id:                   config.Id,
		commitReceived:       0,
		commitInterval:       config.CommitInterval,
		port:                 config.Peers[config.Id],
		peers:                make([]*Peer, len(config.Peers)),
		nextChannelId:        0,
		prepareThreadRunning: 0,
		commitThreadRunning:  0,
	}
	multipaxos.channels = &tcp.ChannelMap{
		Channels: make(map[uint64]chan string),
	}
	multipaxos.cvFollower = sync.NewCond(&multipaxos.mu)
	multipaxos.cvLeader = sync.NewCond(&multipaxos.mu)
	rand.Seed(multipaxos.id)

	for id, addr := range config.Peers {
		multipaxos.peers[id] = &Peer{
			Id:   int64(id),
			Stub: MakePeer(addr, multipaxos.channels),
		}
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
}

func (p *Multipaxos) Stop() {
	p.StopPrepareThread()
	p.StopCommitThread()
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

func (p *Multipaxos) Replicate(command *tcp.Command, clientId int64) Result {
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
	map[int64]*tcp.Instance) {
	state := NewPrepareState()

	if ballot > p.Ballot() {
		state.NumRpcs++
		state.NumOks++
		state.Log = p.log.GetLog()
		state.MaxLastIndex = p.log.LastIndex()
	} else {
		return -1, nil
	}

	prepareRequest := tcp.PrepareRequest{
		Sender: p.id,
		Ballot: ballot,
	}
	request, _ := json.Marshal(prepareRequest)
	cid, responseChan := p.addChannel(len(p.peers))
	for _, peer := range p.peers {
		if peer.Id == p.id {
			continue
		}
		go func(peer *Peer) {
			peer.Stub.SendAwaitResponse(tcp.PREPAREREQUEST, cid, string(request))
			logger.Infof("%v sent prepare request to %v", p.id, peer.Id)

			response := <-responseChan
			var prepareResponse tcp.PrepareResponse
			json.Unmarshal([]byte(response), &prepareResponse)

			state.Mu.Lock()
			defer state.Mu.Unlock()

			state.NumRpcs += 1
			if prepareResponse.Type == tcp.Ok {
				state.NumOks += 1
				for i := 0; i < len(prepareResponse.Logs); i++ {
					instance := prepareResponse.Logs[i]
					if instance.Index > state.MaxLastIndex {
						state.MaxLastIndex = instance.Index
					}
					Log.Insert(state.Log, instance)
				}
			} else {
				p.BecomeFollower(prepareResponse.Ballot)
			}
			state.Cv.Signal()
		}(peer)
	}

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for state.NumOks <= len(p.peers)/2 && state.NumRpcs != len(p.peers) {
		state.Cv.Wait()
	}

	if state.NumOks > len(p.peers)/2 {
		p.removeChannel(cid)
		return state.MaxLastIndex, state.Log
	}
	p.removeChannel(cid)
	return -1, nil
}

func (p *Multipaxos) RunAcceptPhase(ballot int64, index int64,
	command *tcp.Command, clientId int64) Result {
	state := NewAcceptState()

	instance := tcp.Instance{
		Ballot:   ballot,
		Index:    index,
		ClientId: clientId,
		State:    tcp.Inprogress,
		Command:  command,
	}

	if ballot == p.Ballot() {
		instance := tcp.Instance{
			Ballot:   ballot,
			Index:    index,
			ClientId: clientId,
			State:    tcp.Inprogress,
			Command:  command,
		}
		state.NumRpcs++
		state.NumOks++
		p.log.Append(&instance)
	} else {
		return Result{SomeElseLeader, ExtractLeaderId(p.Ballot())}
	}

	acceptRequest := tcp.AcceptRequest{
		Sender:   p.id,
		Instance: &instance,
	}
	request, _ := json.Marshal(acceptRequest)
	cid, responseChan := p.addChannel(len(p.peers))

	for _, peer := range p.peers {
		if peer.Id == p.id {
			continue
		}
		go func(peer *Peer) {
			peer.Stub.SendAwaitResponse(tcp.ACCEPTREQUEST, cid, string(request))
			logger.Infof("%v sent accept request to %v", p.id, peer.Id)

			response := <-responseChan
			var acceptResponse tcp.AcceptResponse
			json.Unmarshal([]byte(response), &acceptResponse)

			state.Mu.Lock()
			defer state.Mu.Unlock()

			state.NumRpcs += 1
			if acceptResponse.Type == tcp.Ok {
				state.NumOks += 1
			} else {
				p.BecomeFollower(acceptResponse.Ballot)
			}
			state.Cv.Signal()
		}(peer)
	}

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for IsLeader(p.Ballot(), p.id) && state.NumOks <= len(p.peers)/2 &&
		state.NumRpcs != len(p.peers) {
		state.Cv.Wait()
	}

	if state.NumOks > len(p.peers)/2 {
		p.log.Commit(index)
		p.removeChannel(cid)
		return Result{Type: Ok, Leader: -1}
	}

	if !IsLeader(p.Ballot(), p.id) {
		p.removeChannel(cid)
		return Result{Type: SomeElseLeader, Leader: ExtractLeaderId(p.Ballot())}
	}
	p.removeChannel(cid)
	return Result{Type: Retry, Leader: -1}
}

func (p *Multipaxos) RunCommitPhase(ballot int64, globalLastExecuted int64) int64 {
	state := NewCommitState(p.log.LastExecuted())

	state.NumRpcs++
	state.NumOks++
	state.MinLastExecuted = p.log.LastExecuted()
	p.log.TrimUntil(globalLastExecuted)

	commitRequest := tcp.CommitRequest{
		Ballot:             ballot,
		Sender:             p.id,
		LastExecuted:       state.MinLastExecuted,
		GlobalLastExecuted: globalLastExecuted,
	}
	request, _ := json.Marshal(commitRequest)
	cid, responseChan := p.addChannel(len(p.peers))

	for _, peer := range p.peers {
		if peer.Id == p.id {
			continue
		}
		go func(peer *Peer) {
			peer.Stub.SendAwaitResponse(tcp.COMMITREQUEST, cid, string(request))
			logger.Infof("%v sent commit request to %v", p.id, peer.Id)

			response := <-responseChan
			var commitResponse tcp.CommitResponse
			json.Unmarshal([]byte(response), &commitResponse)

			state.Mu.Lock()
			defer state.Mu.Unlock()

			state.NumRpcs += 1
			if commitResponse.Type == tcp.Ok {
				state.NumOks += 1
				if commitResponse.LastExecuted < state.MinLastExecuted {
					state.MinLastExecuted = commitResponse.LastExecuted
				}
			} else {
				p.BecomeFollower(commitResponse.Ballot)
			}
			state.Cv.Signal()
		}(peer)
	}

	state.Mu.Lock()
	defer state.Mu.Unlock()
	for IsLeader(p.Ballot(), p.id) && state.NumRpcs != len(p.peers) {
		state.Cv.Wait()
	}
	if state.NumOks == len(p.peers) {
		p.removeChannel(cid)
		return state.MinLastExecuted
	}
	p.removeChannel(cid)
	return globalLastExecuted
}

func (p *Multipaxos) Replay(ballot int64, log map[int64]*tcp.Instance) {
	for index, instance := range log {
		r := p.RunAcceptPhase(ballot, index, instance.Command,
			instance.ClientId)
		for r.Type == Retry {
			r = p.RunAcceptPhase(ballot, index, instance.Command,
				instance.ClientId)
		}
		if r.Type == SomeElseLeader {
			return
		}
	}
}

func (p *Multipaxos) addChannel(numPeers int) (uint64, chan string) {
	responseChan := make(chan string, numPeers-1)
	channelId := atomic.AddUint64(&p.nextChannelId, 1)
	p.channels.Lock()
	p.channels.Channels[channelId] = responseChan
	p.channels.Unlock()
	return channelId, responseChan
}

func (p *Multipaxos) removeChannel(channelId uint64) {
	p.channels.Lock()
	channel := p.channels.Channels[channelId]
	close(channel)
	delete(p.channels.Channels, channelId)
	p.channels.Unlock()
}

func (p *Multipaxos) Prepare(request tcp.PrepareRequest) tcp.PrepareResponse {
	logger.Infof("%v <--prepare-- %v", p.id, request.Sender)
	response := tcp.PrepareResponse{}
	if request.Ballot > p.Ballot() {
		p.BecomeFollower(request.Ballot)
		response.Logs = make([]*tcp.Instance, 0, len(p.log.Instances()))
		for _, i := range p.log.Instances() {
			response.Logs = append(response.Logs, i)
		}
		response.Type = tcp.Ok
	} else {
		response.Ballot = p.Ballot()
		response.Type = tcp.Reject
	}
	return response
}

func (p *Multipaxos) Accept(request tcp.AcceptRequest) tcp.AcceptResponse {
	logger.Infof("%v <--accept-- %v", p.id, request.Sender)
	response := tcp.AcceptResponse{}
	if request.Instance.Ballot >= p.Ballot() {
		p.log.Append(request.Instance)
		response.Type = tcp.Ok
		if request.Instance.Ballot > p.Ballot() {
			p.BecomeFollower(request.Instance.Ballot)
		}
	}
	if request.Instance.Ballot < p.Ballot() {
		response.Ballot = p.Ballot()
		response.Type = tcp.Reject
	}
	return response
}

func (p *Multipaxos) Commit(request tcp.CommitRequest) tcp.CommitResponse {
	logger.Infof("%v <--commit-- %v", p.id, request.Sender)
	response := tcp.CommitResponse{}
	if request.Ballot >= p.Ballot() {
		atomic.StoreInt32(&p.commitReceived, 1)
		p.log.CommitUntil(request.LastExecuted, request.Ballot)
		p.log.TrimUntil(request.GlobalLastExecuted)
		response.LastExecuted = p.log.LastExecuted()
		response.Type = tcp.Ok
		if request.Ballot > p.Ballot() {
			p.BecomeFollower(request.Ballot)
		}
	} else {
		response.Ballot = p.Ballot()
		response.Type = tcp.Reject
	}
	return response
}
