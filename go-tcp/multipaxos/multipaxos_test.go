package multipaxos

import (
	"bufio"
	"encoding/json"
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/kvstore"
	"github.com/psu-csl/replicated-store/go/log"
	tcp "github.com/psu-csl/replicated-store/go/multipaxos/network"
	"github.com/psu-csl/replicated-store/go/util"
	logger "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

const NumPeers = 3

var (
	configs       = make([]config.Config, NumPeers)
	logs          = make([]*log.Log, NumPeers)
	peers         = make([]*Multipaxos, NumPeers)
	stores        = make([]*kvstore.MemKVStore, NumPeers)
	peerListeners = make([]net.Listener, NumPeers)
	serverOn      = make([]bool, NumPeers)
	isSetup       = false
)

func setup() {
	for i := int64(0); i < NumPeers; i++ {
		configs[i] = config.DefaultConfig(i, NumPeers)
	}
	for i := int64(0); i < NumPeers; i++ {
		peerListeners[i], _ = net.Listen("tcp", configs[i].Peers[configs[i].Id])
		serverOn[i] = false
	}
	isSetup = true
}

func initPeers() {
	setup()
	for i := int64(0); i < NumPeers; i++ {
		stores[i] = kvstore.NewMemKVStore()
		logs[i] = log.NewLog(stores[i])
		peers[i] = NewMultipaxos(logs[i], configs[i])
		serverOn[i] = false
		go startServer(i, peers[i])
	}
}

func setupOnePeer(id int64) {
	if !isSetup {
		setup()
	}
	stores[id] = kvstore.NewMemKVStore()
	logs[id] = log.NewLog(stores[id])
	peers[id] = NewMultipaxos(logs[id], configs[id])
	go startServer(id, peers[id])
}

func tearDown() {
	for i, peer := range peers {
		peerListeners[i].Close()
		peer.Stop()
	}
	isSetup = false
}

func tearDownServers() {
	for i, _ := range peers {
		peerListeners[i].Close()
	}
	isSetup = false
}

func LeaderByPeer(peer *Multipaxos) int64 {
	return ExtractLeaderId(peer.Ballot())
}

func IsLeaderByPeer(peer *Multipaxos) bool {
	return IsLeader(peer.Ballot(), peer.Id())
}

func IsSomeoneElseLeaderByPeer(peer *Multipaxos) bool {
	return !IsLeaderByPeer(peer) && LeaderByPeer(peer) < MaxNumPeers
}

func sendCommit(p *Multipaxos, targetId int64, ballot int64,
	lastExecuted int64, globalLastExecuted int64) *tcp.CommitResponse {
	if !serverOn[targetId] {
		return nil
	}
	commitRequest := tcp.CommitRequest{
		Ballot:             ballot,
		LastExecuted:       lastExecuted,
		GlobalLastExecuted: globalLastExecuted,
	}
	request, _ := json.Marshal(commitRequest)
	cid, responseChan := p.addChannel(len(p.peers))
	p.peers[targetId].Stub.SendAwaitResponse(tcp.COMMITREQUEST, cid, string(request))
	response := <-responseChan
	var commitResponse tcp.CommitResponse
	json.Unmarshal([]byte(response), &commitResponse)
	return &commitResponse
}

func sendPrepare(p *Multipaxos, targetId int64, ballot int64) *tcp.PrepareResponse {
	if !serverOn[targetId] {
		return nil
	}
	prepareRequest := tcp.PrepareRequest{Ballot: ballot}
	request, _ := json.Marshal(prepareRequest)
	cid, responseChan := p.addChannel(len(p.peers))
	p.peers[targetId].Stub.SendAwaitResponse(tcp.PREPAREREQUEST, cid, string(request))
	response := <-responseChan
	var prepareResponse tcp.PrepareResponse
	json.Unmarshal([]byte(response), &prepareResponse)
	return &prepareResponse
}

func sendAccept(p *Multipaxos, targetId int64,
	inst *tcp.Instance) *tcp.AcceptResponse {
	if !serverOn[targetId] {
		return nil
	}
	acceptRequest := tcp.AcceptRequest{
		Instance: inst,
	}
	request, _ := json.Marshal(acceptRequest)
	cid, responseChan := p.addChannel(len(p.peers))
	p.peers[targetId].Stub.SendAwaitResponse(tcp.ACCEPTREQUEST, cid, string(request))
	response := <-responseChan
	var acceptResponse tcp.AcceptResponse
	json.Unmarshal([]byte(response), &acceptResponse)
	return &acceptResponse
}

func Connect(multipaxos *Multipaxos, addrs []string) {

}

func TestNewMultipaxos(t *testing.T) {
	setupOnePeer(0)
	defer tearDownServers()

	assert.Equal(t, MaxNumPeers, LeaderByPeer(peers[0]))
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.False(t, IsSomeoneElseLeaderByPeer(peers[0]))
}

func TestNextBallot(t *testing.T) {
	initPeers()
	defer tearDownServers()
	for id := 0; id < NumPeers; id++ {
		ballot := id
		ballot += RoundIncrement
		assert.EqualValues(t, ballot, peers[id].NextBallot())
	}
}

func TestRequestsWithLowerBallotIgnored(t *testing.T) {
	setupOnePeer(0)
	setupOnePeer(1)
	StartPeerConnection(0)
	defer tearDownServers()

	peers[0].BecomeLeader(peers[0].NextBallot(), logs[0].LastIndex())
	peers[0].BecomeLeader(peers[0].NextBallot(), logs[0].LastIndex())
	staleBallot := peers[1].NextBallot()

	r1 := sendPrepare(peers[1], 0, staleBallot)
	assert.EqualValues(t, tcp.Reject, r1.Type)
	assert.True(t, IsLeaderByPeer(peers[0]))

	index := logs[0].AdvanceLastIndex()
	instance := util.MakeInstance(staleBallot, index)
	r2 := sendAccept(peers[1], 0, instance)
	assert.EqualValues(t, tcp.Reject, r2.Type)
	assert.True(t, IsLeaderByPeer(peers[0]))
	assert.Nil(t, logs[0].At(index))

	r3 := sendCommit(peers[1], 0, staleBallot, 0, 0)
	assert.EqualValues(t, tcp.Reject, r3.Type)
	assert.True(t, IsLeaderByPeer(peers[0]))

}

func TestRequestsWithHigherBallotChangeLeaderToFollower(t *testing.T) {
	setupOnePeer(0)
	setupOnePeer(1)
	StartPeerConnection(0)
	defer tearDownServers()

	peers[0].BecomeLeader(peers[0].NextBallot(), logs[0].LastIndex())
	assert.True(t, IsLeaderByPeer(peers[0]))
	r1 := sendPrepare(peers[1], 0, peers[1].NextBallot())
	assert.EqualValues(t, tcp.Ok, r1.Type)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	peers[1].BecomeLeader(peers[1].NextBallot(), logs[1].LastIndex())
	peers[0].BecomeLeader(peers[0].NextBallot(), logs[0].LastIndex())
	assert.True(t, IsLeaderByPeer(peers[0]))
	index := logs[0].AdvanceLastIndex()
	instance := util.MakeInstance(peers[1].NextBallot(), index)
	r2 := sendAccept(peers[1], 0, instance)
	assert.EqualValues(t, tcp.Ok, r2.Type)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	peers[1].BecomeLeader(peers[1].NextBallot(), logs[1].LastIndex())
	peers[0].BecomeLeader(peers[0].NextBallot(), logs[0].LastIndex())
	assert.True(t, IsLeaderByPeer(peers[0]))
	r3 := sendCommit(peers[1], 0, peers[1].NextBallot(), 0, 0)
	assert.EqualValues(t, tcp.Ok, r3.Type)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))
}

func TestNextBallotAfterCommit(t *testing.T) {
	initPeers()
	StartPeerConnection(0)
	defer tearDownServers()

	ballot := peers[0].Id()
	sendCommit(peers[1], 0, peers[1].NextBallot(), 0, 0)
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	ballot += RoundIncrement
	ballot += RoundIncrement
	assert.EqualValues(t, ballot, peers[0].NextBallot())
}

func TestCommitCommitsAndTrims(t *testing.T) {
	setupOnePeer(0)
	setupOnePeer(1)
	StartPeerConnection(0)
	defer tearDownServers()

	ballot := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	logs[0].Append(util.MakeInstance(ballot, index1))
	index2 := logs[0].AdvanceLastIndex()
	logs[0].Append(util.MakeInstance(ballot, index2))
	index3 := logs[0].AdvanceLastIndex()
	logs[0].Append(util.MakeInstance(ballot, index3))

	r1 := sendCommit(peers[1], 0, ballot, index2, 0)
	assert.EqualValues(t, tcp.Ok, r1.Type)
	assert.EqualValues(t, 0, r1.LastExecuted)
	assert.True(t, log.IsCommitted(logs[0].At(index1)))
	assert.True(t, log.IsCommitted(logs[0].At(index2)))
	assert.True(t, log.IsInProgress(logs[0].At(index3)))

	logs[0].ReadInstance()
	logs[0].ReadInstance()

	r2 := sendCommit(peers[1], 0, ballot, index2, index2)
	assert.EqualValues(t, tcp.Ok, r2.Type)
	assert.EqualValues(t, index2, r2.LastExecuted)
	assert.Nil(t, logs[0].At(index1))
	assert.Nil(t, logs[0].At(index2))
	assert.True(t, log.IsInProgress(logs[0].At(index3)))
}

func TestPrepareRespondsWithCorrectInstances(t *testing.T) {
	setupOnePeer(0)
	setupOnePeer(1)
	StartPeerConnection(0)
	defer tearDownServers()

	ballot := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	instance1 := util.MakeInstance(ballot, index1)
	logs[0].Append(instance1)

	index2 := logs[0].AdvanceLastIndex()
	instance2 := util.MakeInstance(ballot, index2)
	logs[0].Append(instance2)

	index3 := logs[0].AdvanceLastIndex()
	instance3 := util.MakeInstance(ballot, index3)
	logs[0].Append(instance3)

	r1 := sendPrepare(peers[1], 0, ballot)
	assert.EqualValues(t, tcp.Ok, r1.Type)
	assert.EqualValues(t, 3, len(r1.Logs))
	assert.True(t, log.IsEqualInstance(instance1, r1.Logs[0]))
	assert.True(t, log.IsEqualInstance(instance2, r1.Logs[1]))
	assert.True(t, log.IsEqualInstance(instance3, r1.Logs[2]))

	r2 := sendCommit(peers[1], 0, ballot, index2, 0)
	assert.EqualValues(t, tcp.Ok, r2.Type)

	logs[0].ReadInstance()
	logs[0].ReadInstance()

	ballot = peers[0].NextBallot()
	r3 := sendPrepare(peers[1], 0, ballot)
	assert.EqualValues(t, tcp.Ok, r3.Type)
	assert.EqualValues(t, 3, len(r3.Logs))
	assert.True(t, log.IsExecuted(r3.Logs[0]))
	assert.True(t, log.IsExecuted(r3.Logs[1]))
	assert.True(t, log.IsInProgress(r1.Logs[2]))

	r4 := sendCommit(peers[1], 0, ballot, index2, 2)
	assert.EqualValues(t, tcp.Ok, r4.Type)

	ballot = peers[0].NextBallot()
	r5 := sendPrepare(peers[1], 0, ballot)
	assert.EqualValues(t, tcp.Ok, r5.Type)
	assert.EqualValues(t, 1, len(r5.Logs))
	assert.True(t, log.IsEqualInstance(instance3, r5.Logs[0]))
}

func TestAcceptAppendsToLog(t *testing.T) {
	setupOnePeer(0)
	setupOnePeer(1)
	StartPeerConnection(0)
	defer tearDownServers()

	ballot := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	instance1 := util.MakeInstance(ballot, index1)
	index2 := logs[0].AdvanceLastIndex()
	instance2 := util.MakeInstance(ballot, index2)

	r1 := sendAccept(peers[1], 0, instance1)
	assert.EqualValues(t, tcp.Ok, r1.Type)
	assert.True(t, log.IsEqualInstance(instance1, logs[0].At(index1)))
	assert.Nil(t, logs[0].At(index2))

	r2 := sendAccept(peers[1], 0, instance2)
	assert.EqualValues(t, tcp.Ok, r2.Type)
	assert.True(t, log.IsEqualInstance(instance1, logs[0].At(index1)))
	assert.True(t, log.IsEqualInstance(instance2, logs[0].At(index2)))
}

func TestPrepareResponseWithHigherBallotChangesLeaderToFollower(t *testing.T) {
	initPeers()
	defer tearDownServers()
	for i, _ := range peers {
		StartPeerConnection(int64(i))
	}

	peer0Ballot := peers[0].NextBallot()
	peers[0].BecomeLeader(peer0Ballot, logs[0].LastIndex())
	peers[1].BecomeLeader(peers[1].NextBallot(), logs[1].LastIndex())
	peer2Ballot := peers[2].NextBallot()
	peers[2].BecomeLeader(peer2Ballot, logs[2].LastIndex())
	peer2Ballot = peers[2].NextBallot()
	peers[2].BecomeLeader(peer2Ballot, logs[2].LastIndex())

	r := sendCommit(peers[2], 1, peer2Ballot, 0, 0)
	assert.EqualValues(t, tcp.Ok, r.Type)
	assert.False(t, IsLeaderByPeer(peers[1]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[1]))

	assert.True(t, IsLeaderByPeer(peers[0]))
	peer0Ballot = peers[0].NextBallot()
	peers[0].RunPreparePhase(peer0Ballot)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[0]))
}

func TestAcceptResponseWithHigherBallotChangesLeaderToFollower(t *testing.T) {
	initPeers()
	defer tearDownServers()
	for i, _ := range peers {
		StartPeerConnection(int64(i))
	}

	peer0Ballot := peers[0].NextBallot()
	peers[0].BecomeLeader(peer0Ballot, logs[0].LastIndex())
	peers[1].BecomeLeader(peers[1].NextBallot(), logs[1].LastIndex())
	peer2Ballot := peers[2].NextBallot()
	peers[2].BecomeLeader(peer2Ballot, logs[2].LastIndex())

	r := sendCommit(peers[2], 1, peer2Ballot, 0, 0)
	assert.EqualValues(t, tcp.Ok, r.Type)
	assert.False(t, IsLeaderByPeer(peers[1]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[1]))

	assert.True(t, IsLeaderByPeer(peers[0]))
	r2 := peers[0].RunAcceptPhase(peer0Ballot, 1, &tcp.Command{}, 0)
	assert.EqualValues(t, SomeElseLeader, r2.Type)
	assert.EqualValues(t, 2, r2.Leader)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[0]))
}

func TestCommitResponseWithHigherBallotChangesLeaderToFollower(t *testing.T) {
	initPeers()
	defer tearDownServers()
	for i, _ := range peers {
		StartPeerConnection(int64(i))
	}
	Connect(peers[0], configs[0].Peers)

	peer0Ballot := peers[0].NextBallot()
	peers[0].BecomeLeader(peer0Ballot, logs[0].LastIndex())
	peers[1].BecomeLeader(peers[1].NextBallot(), logs[1].LastIndex())
	peer2Ballot := peers[2].NextBallot()
	peers[2].BecomeLeader(peer2Ballot, logs[2].LastIndex())

	r := sendCommit(peers[2], 1, peer2Ballot, 0, 0)
	assert.EqualValues(t, tcp.Ok, r.Type)
	assert.False(t, IsLeaderByPeer(peers[1]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[1]))

	assert.True(t, IsLeaderByPeer(peers[0]))
	peers[0].RunCommitPhase(peer0Ballot, 0)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[0]))
}

func TestRunPreparePhase(t *testing.T) {
	initPeers()
	defer tearDownServers()
	StartPeerConnection(0)

	const (
		index1 int64 = iota + 1
		index2
		index3
		index4
		index5
	)

	ballot0 := peers[0].NextBallot()
	peers[0].BecomeLeader(ballot0, logs[0].LastIndex())
	ballot1 := peers[1].NextBallot()
	peers[1].BecomeLeader(ballot1, logs[1].LastIndex())

	expectedLog := make(map[int64]*tcp.Instance)

	logs[0].Append(util.MakeInstanceWithType(ballot0, index1,
		tcp.Put))
	logs[1].Append(util.MakeInstanceWithType(ballot0, index1,
		tcp.Put))
	expectedLog[index1] = util.MakeInstanceWithType(ballot0, index1,
		tcp.Put)

	logs[1].Append(util.MakeInstance(ballot0, index2))
	expectedLog[index2] = util.MakeInstance(ballot0, index2)

	logs[0].Append(util.MakeInstanceWithAll(ballot0, index3,
		tcp.Committed, tcp.Del))
	logs[1].Append(util.MakeInstanceWithAll(ballot1, index3,
		tcp.Inprogress, tcp.Del))
	expectedLog[index3] = util.MakeInstanceWithAll(ballot0, index3,
		tcp.Committed, tcp.Del)

	logs[0].Append(util.MakeInstanceWithAll(ballot0, index4,
		tcp.Executed, tcp.Del))
	logs[1].Append(util.MakeInstanceWithAll(ballot1, index4,
		tcp.Inprogress, tcp.Del))
	expectedLog[index4] = util.MakeInstanceWithAll(ballot0, index4,
		tcp.Executed, tcp.Del)

	ballot0 = peers[0].NextBallot()
	peers[0].BecomeLeader(ballot0, logs[0].LastIndex())
	ballot1 = peers[1].NextBallot()
	peers[1].BecomeLeader(ballot1, logs[1].LastIndex())

	logs[0].Append(util.MakeInstanceWithAll(ballot0, index5,
		tcp.Inprogress, tcp.Get))
	logs[1].Append(util.MakeInstanceWithAll(ballot1, index5,
		tcp.Inprogress, tcp.Put))
	expectedLog[index5] = util.MakeInstanceWithAll(ballot1, index5,
		tcp.Inprogress, tcp.Put)

	ballot := peers[0].NextBallot()
	_, logMap := peers[0].RunPreparePhase(ballot)
	assert.Nil(t, logMap)

	StartPeerConnection(1)

	lastIndex, logMap := peers[0].RunPreparePhase(ballot)
	assert.EqualValues(t, 5, lastIndex)
	for index, instance := range logMap {
		if index == 3 || index == 4 {
			assert.True(t, log.IsEqualCommand(expectedLog[index].Command, instance.Command))
		} else {
			assert.True(t, log.IsEqualInstance(expectedLog[index], instance),
				"index: %v", index)
		}
	}
}

func TestRunAcceptPhase(t *testing.T) {
	initPeers()
	defer tearDownServers()
	StartPeerConnection(0)

	ballot := peers[0].NextBallot()
	peers[0].BecomeLeader(ballot, logs[0].LastIndex())
	index1 := logs[0].AdvanceLastIndex()
	instance1 := util.MakeInstanceWithType(ballot, index1, tcp.Put)

	r1 := peers[0].RunAcceptPhase(ballot, index1, &tcp.Command{Type: tcp.Put}, 0)
	assert.EqualValues(t, Retry, r1.Type)
	assert.EqualValues(t, -1, r1.Leader)

	assert.True(t, log.IsInProgress(logs[0].At(index1)))
	assert.Nil(t, logs[1].At(index1))
	assert.Nil(t, logs[2].At(index1))

	StartPeerConnection(1)
	Connect(peers[0], configs[0].Peers)

	r2 := peers[0].RunAcceptPhase(ballot, index1, &tcp.Command{Type: tcp.Put}, 0)
	assert.EqualValues(t, Ok, r2.Type)
	assert.EqualValues(t, -1, r2.Leader)

	assert.True(t, log.IsCommitted(logs[0].At(index1)))
	assert.True(t, log.IsEqualInstance(instance1, logs[1].At(index1)))
	assert.Nil(t, logs[2].At(index1))
}

func TestRunCommitPhase(t *testing.T) {
	initPeers()
	defer tearDownServers()
	StartPeerConnection(0)
	StartPeerConnection(1)

	numInstances := int64(3)
	ballot := peers[0].NextBallot()
	peers[0].BecomeLeader(ballot, logs[0].LastIndex())

	for index := int64(1); index <= numInstances; index++ {
		for peerId, log := range logs {
			if peerId == 2 && index == 3 {
				continue
			}
			instance := util.MakeInstanceWithState(ballot, index,
				tcp.Committed)
			log.Append(instance)
			log.ReadInstance()
		}
	}

	gle := int64(0)
	gle = peers[0].RunCommitPhase(ballot, gle)
	assert.EqualValues(t, 0, gle)

	StartPeerConnection(2)
	logs[2].Append(util.MakeInstance(ballot, 3))

	gle = peers[0].RunCommitPhase(ballot, gle)
	assert.EqualValues(t, 2, gle)

	logs[2].ReadInstance()

	gle = peers[0].RunCommitPhase(ballot, gle)
	assert.EqualValues(t, numInstances, gle)
}

func TestReplay(t *testing.T) {
	initPeers()
	defer tearDownServers()
	StartPeerConnection(0)
	StartPeerConnection(1)

	ballot := peers[0].NextBallot()
	peers[0].BecomeLeader(ballot, logs[0].LastIndex())

	const (
		index1 int64 = iota + 1
		index2
		index3
	)
	replayLog := map[int64]*tcp.Instance{
		index1: util.MakeInstanceWithAll(ballot, index1,
			tcp.Committed, tcp.Put),
		index2: util.MakeInstanceWithAll(ballot, index2,
			tcp.Executed, tcp.Get),
		index3: util.MakeInstanceWithAll(ballot, index3,
			tcp.Inprogress, tcp.Del),
	}

	assert.Nil(t, logs[0].At(index1))
	assert.Nil(t, logs[0].At(index2))
	assert.Nil(t, logs[0].At(index3))

	assert.Nil(t, logs[1].At(index1))
	assert.Nil(t, logs[1].At(index2))
	assert.Nil(t, logs[1].At(index3))

	newBallot := peers[0].NextBallot()
	peers[0].BecomeLeader(newBallot, logs[0].LastIndex())
	peers[0].Replay(newBallot, replayLog)

	assert.True(t, log.IsEqualInstance(util.MakeInstanceWithAll(newBallot,
		index1, tcp.Committed, tcp.Put),
		logs[0].At(index1)))
	assert.True(t, log.IsEqualInstance(util.MakeInstanceWithAll(newBallot,
		index2, tcp.Committed, tcp.Get),
		logs[0].At(index2)))
	assert.True(t, log.IsEqualInstance(util.MakeInstanceWithAll(newBallot,
		index3, tcp.Committed, tcp.Del),
		logs[0].At(index3)))

	assert.True(t, log.IsEqualInstance(util.MakeInstanceWithAll(newBallot,
		index1, tcp.Inprogress, tcp.Put),
		logs[1].At(index1)))
	assert.True(t, log.IsEqualInstance(util.MakeInstanceWithAll(newBallot,
		index2, tcp.Inprogress, tcp.Get),
		logs[1].At(index2)))
	assert.True(t, log.IsEqualInstance(util.MakeInstanceWithAll(newBallot,
		index3, tcp.Inprogress, tcp.Del),
		logs[1].At(index3)))
}

func TestReplicate(t *testing.T) {
	initPeers()
	defer tearDown()
	StartPeerConnection(0)
	peers[0].Start()

	r1 := peers[0].Replicate(&tcp.Command{}, 0)
	assert.Equal(t, Retry, r1.Type)
	assert.EqualValues(t, -1, r1.Leader)

	peers[1].Start()
	peers[2].Start()
	StartPeerConnection(1)
	StartPeerConnection(2)
	time.Sleep(5 * time.Second)

	leader := oneLeader()
	assert.NotEqualValues(t, -1, leader)

	r2 := peers[leader].Replicate(&tcp.Command{}, 0)
	assert.Equal(t, Ok, r2.Type)

	notLeader := (leader + 1) % NumPeers
	r3 := peers[notLeader].Replicate(&tcp.Command{}, 0)
	assert.EqualValues(t, SomeElseLeader, r3.Type)
	assert.EqualValues(t, leader, r3.Leader)
}

func oneLeader() int64 {
	leader := LeaderByPeer(peers[0])
	numLeader := 0
	for _, peer := range peers {
		if IsLeaderByPeer(peer) {
			numLeader++
			if numLeader > 1 || peer.Id() != leader {
				return -1
			}
		} else if LeaderByPeer(peer) != leader {
			return -1
		}
	}
	return leader
}

func StartPeerConnection(id int64) {
	serverOn[id] = true
}

func startServer(id int64, peer *Multipaxos) {
	for {
		client, err := peerListeners[id].Accept()
		if err != nil {
			logger.Error(err)
			break
		}
		go func(client net.Conn) {
			reader := bufio.NewReader(client)
			writer := bufio.NewWriter(client)
			for {
				request, err := reader.ReadString('\n')
				if err != nil {
					client.Close()
					return
				}
				handlePeerRequest(peer, writer, request)
			}
		}(client)
	}
}

func handlePeerRequest(multipaxos *Multipaxos, writer *bufio.Writer, line string) {
	var request tcp.Message
	err := json.Unmarshal([]byte(line), &request)
	if err != nil {
		return
	}

	msg := []byte(request.Msg)
	go func() {
		switch tcp.MessageType(request.Type) {
		case tcp.PREPAREREQUEST:
			prepareResponse := tcp.PrepareResponse{
				Type:   tcp.Reject,
				Ballot: 0,
			}
			if serverOn[multipaxos.id] {
				var prepareRequest tcp.PrepareRequest
				json.Unmarshal(msg, &prepareRequest)
				prepareResponse = multipaxos.Prepare(prepareRequest)
			} else {
				time.Sleep(500 * time.Millisecond)
			}
			responseJson, _ := json.Marshal(prepareResponse)
			tcpMessage, _ := json.Marshal(tcp.Message{
				Type:      uint8(tcp.PREPARERESPONSE),
				ChannelId: request.ChannelId,
				Msg:       string(responseJson),
			})
			logger.Info(string(tcpMessage))
			writer.WriteString(string(tcpMessage) + "\n")
			writer.Flush()
		case tcp.ACCEPTREQUEST:
			acceptResponse := tcp.AcceptResponse{
				Type:   tcp.Reject,
				Ballot: 0,
			}
			if serverOn[multipaxos.id] {
				var acceptRequest tcp.AcceptRequest
				json.Unmarshal(msg, &acceptRequest)
				acceptResponse = multipaxos.Accept(acceptRequest)
			} else {
				time.Sleep(500 * time.Millisecond)
			}
			responseJson, _ := json.Marshal(acceptResponse)
			tcpMessage, _ := json.Marshal(tcp.Message{
				Type:      uint8(tcp.ACCEPTRESPONSE),
				ChannelId: request.ChannelId,
				Msg:       string(responseJson),
			})
			writer.WriteString(string(tcpMessage) + "\n")
			writer.Flush()
		case tcp.COMMITREQUEST:
			commitResponse := tcp.CommitResponse{
				Type:   tcp.Reject,
				Ballot: 0,
			}
			if serverOn[multipaxos.id] {
				var commitRequest tcp.CommitRequest
				json.Unmarshal(msg, &commitRequest)
				commitResponse = multipaxos.Commit(commitRequest)
			} else {
				time.Sleep(500 * time.Millisecond)
			}
			responseJson, _ := json.Marshal(commitResponse)
			tcpMessage, _ := json.Marshal(tcp.Message{
				Type:      uint8(tcp.COMMITRESPONSE),
				ChannelId: request.ChannelId,
				Msg:       string(responseJson),
			})
			writer.WriteString(string(tcpMessage) + "\n")
			writer.Flush()
		}
	}()
}
