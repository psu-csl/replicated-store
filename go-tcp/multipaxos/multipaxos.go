package multipaxos

import (
	"encoding/json"
	"github.com/psu-csl/replicated-store/go/config"
	Log "github.com/psu-csl/replicated-store/go/log"
	tcp "github.com/psu-csl/replicated-store/go/multipaxos/network"
	logger "github.com/sirupsen/logrus"
	"math/rand"
	"strconv"
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
	peers          PeerList
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
		peers:                PeerList{list: make([]*Peer, len(config.Peers))},
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
		multipaxos.peers.list[id] = &Peer{
			Id:   int64(id),
			Stub: MakePeer(addr, multipaxos.channels),
		}
	}

	return &multipaxos
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
	p.peers.RLock()
	numPeers := len(p.peers.list)
	p.peers.RUnlock()
	numOks := 0
	log := make(map[int64]*tcp.Instance)
	maxLastIndex := int64(0)

	if ballot > p.Ballot() {
		numOks += 1
		log = p.log.GetLog()
		maxLastIndex = p.log.LastIndex()
		if numOks > numPeers/2 {
			return maxLastIndex, log
		}
	} else {
		return -1, nil
	}

	request, _ := json.Marshal(tcp.PrepareRequest{
		Sender: p.id,
		Ballot: ballot,
	})
	channelId, responseChan := p.addChannel(numPeers)
	p.peers.RLock()
	for _, peer := range p.peers.list {
		if peer.Id != p.id {
			go func(peer *Peer) {
				peer.Stub.SendAwaitResponse(tcp.PREPAREREQUEST,
					channelId, string(request))
				logger.Infof("%v sent prepare request to %v", p.id, peer.Id)
			}(peer)
		}
	}
	p.peers.RUnlock()

	for {
		response := <-responseChan
		var prepareResponse tcp.PrepareResponse
		json.Unmarshal([]byte(response), &prepareResponse)

		if prepareResponse.Type == tcp.Ok {
			numOks += 1
			for _, instance := range prepareResponse.Logs {
				if instance.Index > maxLastIndex {
					maxLastIndex = instance.Index
				}
				Log.Insert(log, instance)
			}
		} else {
			p.BecomeFollower(prepareResponse.Ballot)
			break
		}
		if numOks > numPeers/2 {
			p.removeChannel(channelId)
			return maxLastIndex, log
		}
	}
	p.removeChannel(channelId)
	return -1, nil
}

func (p *Multipaxos) RunAcceptPhase(ballot int64, index int64,
	command *tcp.Command, clientId int64) Result {

	p.peers.RLock()
	numPeers := len(p.peers.list)
	p.peers.RUnlock()
	numOks := 0

	if ballot == p.Ballot() {
		numOks += 1
		instance := tcp.Instance{
			Ballot:   ballot,
			Index:    index,
			ClientId: clientId,
			State:    tcp.Inprogress,
			Command:  command,
		}
		p.log.Append(&instance)
		if numOks > numPeers/2 {
			p.log.Commit(index)
			return Result{Type: Ok, Leader: -1}
		}
	} else {
		currentLeaderId := ExtractLeaderId(p.Ballot())
		return Result{SomeElseLeader, currentLeaderId}
	}

	instance := tcp.Instance{
		Ballot:   ballot,
		Index:    index,
		ClientId: clientId,
		State:    tcp.Inprogress,
		Command:  command,
	}
	request, _ := json.Marshal(tcp.AcceptRequest{
		Sender:   p.id,
		Instance: &instance,
	})
	channelId, responseChan := p.addChannel(numPeers)
	p.peers.RLock()
	for _, peer := range p.peers.list {
		if peer.Id != p.id {
			go func(peer *Peer) {
				peer.Stub.SendAwaitResponse(tcp.ACCEPTREQUEST,
					channelId, string(request))
				logger.Infof("%v sent accept request to %v", p.id, peer.Id)
			}(peer)
		}
	}
	p.peers.RUnlock()

	for {
		response := <-responseChan
		var acceptResponse tcp.AcceptResponse
		json.Unmarshal([]byte(response), &acceptResponse)

		if acceptResponse.Type == tcp.Ok {
			numOks += 1
		} else {
			p.BecomeFollower(acceptResponse.Ballot)
			break
		}
		if numOks > numPeers/2 {
			p.log.Commit(index)
			p.removeChannel(channelId)
			return Result{Type: Ok, Leader: -1}
		}
	}
	if !IsLeader(p.Ballot(), p.id) {
		p.removeChannel(channelId)
		return Result{Type: SomeElseLeader, Leader: ExtractLeaderId(p.Ballot())}
	}
	p.removeChannel(channelId)
	return Result{Type: Retry, Leader: -1}
}

func (p *Multipaxos) RunCommitPhase(ballot int64, globalLastExecuted int64) int64 {
	p.peers.RLock()
	numPeers := len(p.peers.list)
	p.peers.RUnlock()
	numOks := 0
	minLastExecuted := p.log.LastExecuted()

	numOks++
	p.log.TrimUntil(globalLastExecuted)
	if numOks == numPeers {
		return minLastExecuted
	}

	request, _ := json.Marshal(tcp.CommitRequest{
		Ballot:             ballot,
		LastExecuted:       minLastExecuted,
		GlobalLastExecuted: globalLastExecuted,
		Sender:             p.id,
	})
	channelId, responseChan := p.addChannel(numPeers)
	p.peers.RLock()
	for _, peer := range p.peers.list {
		if peer.Id != p.id {
			go func(peer *Peer) {
				peer.Stub.SendAwaitResponse(tcp.COMMITREQUEST,
					channelId, string(request))
				logger.Infof("%v sent commit request to %v", p.id, peer.Id)
			}(peer)
		}
	}
	p.peers.RUnlock()

	for {
		response := <-responseChan
		var commitResponse tcp.CommitResponse
		json.Unmarshal([]byte(response), &commitResponse)
		if commitResponse.Type == tcp.Ok {
			numOks += 1
			if commitResponse.LastExecuted < minLastExecuted {
				minLastExecuted = commitResponse.LastExecuted
			}
		} else {
			p.BecomeFollower(commitResponse.Ballot)
			break
		}
		if numOks == numPeers {
			p.removeChannel(channelId)
			return minLastExecuted
		}
	}
	p.removeChannel(channelId)
	return globalLastExecuted
}

func (p *Multipaxos) Replay(ballot int64, log map[int64]*tcp.Instance) {
	for _, instance := range log {
		var r Result
		for {
			r = p.RunAcceptPhase(ballot, instance.Index, instance.Command,
				instance.ClientId)
			if r.Type != Retry {
				break
			}
		}
		if r.Type == SomeElseLeader {
			return
		}
	}
}

func (p *Multipaxos) Reconfigure(cmd *tcp.Command) {
	peerId, err := strconv.Atoi(cmd.Key)
	if err != nil {
		return
	}

	p.peers.Lock()
	defer p.peers.Unlock()
	if cmd.Type == tcp.AddNode {
		peer := &Peer{
			Id:   int64(peerId),
			Stub: MakePeer(cmd.Value, p.channels),
		}
		p.peers.list = append(p.peers.list, peer)
		// send snapshot
	} else if cmd.Type == tcp.DelNode {
		for i, peer := range p.peers.list {
			if peer.Id == int64(peerId) {
				p.peers.list = append(p.peers.list[:i], p.peers.list[i+1:]...)
			}
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
	delete(p.channels.Channels, channelId)
	close(channel)
	p.channels.Unlock()
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

func (p *Multipaxos) Prepare(request tcp.PrepareRequest) tcp.PrepareResponse {
	logger.Infof("%v <--prepare-- %v", p.id, request.Sender)

	if request.Ballot > p.Ballot() {
		p.BecomeFollower(request.Ballot)
		return tcp.PrepareResponse{
			Type:   tcp.Ok,
			Ballot: p.Ballot(),
			Logs:   p.log.Instances(),
		}
	}
	return tcp.PrepareResponse{
		Type:   tcp.Reject,
		Ballot: p.Ballot(),
		Logs:   nil,
	}
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

	if request.Ballot >= p.Ballot() {
		atomic.StoreInt32(&p.commitReceived, 1)
		p.log.CommitUntil(request.LastExecuted, request.Ballot)
		p.log.TrimUntil(request.GlobalLastExecuted)
		if request.Ballot > p.Ballot() {
			p.BecomeFollower(request.Ballot)
		}
		return tcp.CommitResponse{
			Type:         tcp.Ok,
			Ballot:       p.Ballot(),
			LastExecuted: p.log.LastExecuted(),
		}
	}
	return tcp.CommitResponse{
		Type:         tcp.Reject,
		Ballot:       p.Ballot(),
		LastExecuted: 0,
	}
}

func (p *Multipaxos) Id() int64 {
	return p.id
}
