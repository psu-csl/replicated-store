package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	"github.com/psu-csl/replicated-store/go/consensus/multipaxos/util"
	"github.com/psu-csl/replicated-store/go/log"
	"github.com/psu-csl/replicated-store/go/store"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"testing"
	"time"
)

const NumPeers = 3

var (
	configs = make([]config.Config, NumPeers)
	logs    = make([]*log.Log, NumPeers)
	peers   = make([]*Multipaxos, NumPeers)
	stores  = make([]*store.MemKVStore, NumPeers)
)

func setupPeers() {
	for i:= int64(0); i < NumPeers; i++ {
		configs[i] = config.DefaultConfig(i, NumPeers)
		logs[i] = log.NewLog()
		stores[i] = store.NewMemKVStore()
		peers[i] = NewMultipaxos(configs[i], logs[i])
	}
}

func setupServer() {
	setupPeers()
	peers[0].Start()
	peers[1].Start()
	peers[2].Start()
}

func setupOnePeer(id int64) {
	configs[id] = config.DefaultConfig(id, NumPeers)
	logs[id] = log.NewLog()
	stores[id] = store.NewMemKVStore()
	peers[id] = NewMultipaxos(configs[id], logs[id])
}

func tearDown() {
	for _, peer := range peers {
		peer.Stop()
	}
}

func TestNewMultipaxos(t *testing.T) {
	setupOnePeer(0)

	assert.Equal(t, MaxNumPeers, LeaderByPeer(peers[0]))
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.False(t, IsSomeoneElseLeaderByPeer(peers[0]))
}

func TestNextBallot(t *testing.T) {
	setupOnePeer(2)
	id := peers[2].Id()
	ballot := id

	ballot += RoundIncrement
	assert.Equal(t, ballot, peers[2].NextBallot())
	ballot += RoundIncrement
	assert.Equal(t, ballot, peers[2].NextBallot())

	assert.True(t, IsLeaderByPeer(peers[2]))
	assert.False(t, IsSomeoneElseLeaderByPeer(peers[2]))
	assert.Equal(t, id, LeaderByPeer(peers[2]))
}

func TestRequestsWithLowerBallotIgnored(t *testing.T) {
	setupOnePeer(0)
	setupOnePeer(1)
	peers[0].StartServer()
	stub := makeStub(configs[0].Peers[0])

	peers[0].NextBallot()
	peers[0].NextBallot()
	staleBallot := peers[1].NextBallot()

	r1 := sendCommit(stub, staleBallot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_REJECT, r1.GetType())
	assert.True(t, IsLeaderByPeer(peers[0]))

	r2 := sendPrepare(stub, staleBallot)
	assert.EqualValues(t, pb.ResponseType_REJECT, r2.GetType())
	assert.True(t, IsLeaderByPeer(peers[0]))

	index := logs[0].AdvanceLastIndex()
	instance := util.MakeInstance(staleBallot, index)
	r3 := sendAccept(stub, instance)
	assert.EqualValues(t, pb.ResponseType_REJECT, r3.GetType())
	assert.True(t, IsLeaderByPeer(peers[0]))
	assert.Nil(t, logs[0].Find(index))

	peers[0].Stop()
}

func TestRequestsWithHigherBallotChangeLeaderToFollower(t *testing.T) {
	setupOnePeer(0)
	setupOnePeer(1)
	peers[0].StartServer()
	stub := makeStub(configs[0].Peers[0])

	peers[0].NextBallot()
	assert.True(t, IsLeaderByPeer(peers[0]))
	r1 := sendCommit(stub, peers[1].NextBallot(), 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r1.GetType())
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	peers[0].NextBallot()
	assert.True(t, IsLeaderByPeer(peers[0]))
	r2 := sendPrepare(stub, peers[1].NextBallot())
	assert.EqualValues(t, pb.ResponseType_OK, r2.GetType())
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	peers[0].NextBallot()
	assert.True(t, IsLeaderByPeer(peers[0]))
	index := logs[0].AdvanceLastIndex()
	instance := util.MakeInstance(peers[1].NextBallot(), index)
	r3 := sendAccept(stub, instance)
	assert.EqualValues(t, pb.ResponseType_OK, r3.GetType())
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	peers[0].Stop()
}

func TestNextBallotAfterCommit(t *testing.T) {
	setupPeers()
	peers[0].StartServer()
	stub := makeStub(configs[0].Peers[0])
	ballot := peers[0].Id()

	ctx := context.Background()
	request := pb.CommitRequest{
		Ballot: peers[1].NextBallot(),
	}
	stub.Commit(ctx, &request)
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	ballot += RoundIncrement
	ballot += RoundIncrement
	assert.EqualValues(t, ballot, peers[0].NextBallot())

	peers[0].Stop()
}

func TestCommitCommitsAndTrims(t *testing.T) {
	setupOnePeer(0)
	peers[0].StartServer()
	stub := makeStub(configs[0].Peers[0])

	ballot := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	logs[0].Append(util.MakeInstance(ballot, index1))
	index2 := logs[0].AdvanceLastIndex()
	logs[0].Append(util.MakeInstance(ballot, index2))
	index3 := logs[0].AdvanceLastIndex()
	logs[0].Append(util.MakeInstance(ballot, index3))

	r1 := sendCommit(stub, ballot, index2, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r1.GetType())
	assert.EqualValues(t, 0, r1.LastExecuted)
	assert.True(t, log.IsCommitted(logs[0].Find(index1)))
	assert.True(t, log.IsCommitted(logs[0].Find(index2)))
	assert.True(t, log.IsInProgress(logs[0].Find(index3)))

	logs[0].Execute(stores[0])
	logs[0].Execute(stores[0])

	r2 := sendCommit(stub, ballot, index2, index2)
	assert.EqualValues(t, pb.ResponseType_OK, r2.GetType())
	assert.EqualValues(t, index2, r2.LastExecuted)
	assert.Nil(t, logs[0].Find(index1))
	assert.Nil(t, logs[0].Find(index2))
	assert.True(t, log.IsInProgress(logs[0].Find(index3)))

	peers[0].Stop()
}

func TestPrepareRespondsWithCorrectInstances(t *testing.T) {
	setupOnePeer(0)
	peers[0].StartServer()
	stub := makeStub(configs[0].Peers[0])

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

	r1 := sendPrepare(stub, ballot)
	assert.EqualValues(t, pb.ResponseType_OK, r1.GetType())
	assert.EqualValues(t, 3, len(r1.GetLogs()))
	assert.True(t, log.IsEqualInstance(instance1, r1.GetLogs()[0]))
	assert.True(t, log.IsEqualInstance(instance2, r1.GetLogs()[1]))
	assert.True(t, log.IsEqualInstance(instance3, r1.GetLogs()[2]))

	r2 := sendCommit(stub, ballot, index2, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r2.GetType())
	logs[0].Execute(stores[0])
	logs[0].Execute(stores[0])

	r3 := sendPrepare(stub, ballot)
	assert.EqualValues(t, pb.ResponseType_OK, r3.GetType())
	assert.EqualValues(t, 3, len(r3.GetLogs()))
	assert.True(t, log.IsExecuted(r3.GetLogs()[0]))
	assert.True(t, log.IsExecuted(r3.GetLogs()[1]))
	assert.True(t, log.IsEqualInstance(instance3, r1.GetLogs()[2]))

	r4 := sendCommit(stub, ballot, index2, 2)
	assert.EqualValues(t, pb.ResponseType_OK, r4.GetType())

	r5 := sendPrepare(stub, ballot)
	assert.EqualValues(t, pb.ResponseType_OK, r5.GetType())
	assert.EqualValues(t, 1, len(r5.GetLogs()))
	assert.True(t, log.IsEqualInstance(instance3, r5.GetLogs()[0]))

	peers[0].Stop()
}

func TestAcceptAppendsToLog(t *testing.T) {
	setupOnePeer(0)
	peers[0].StartServer()
	stub := makeStub(configs[0].Peers[0])

	ballot := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	instance1 := util.MakeInstance(ballot, index1)
	index2 := logs[0].AdvanceLastIndex()
	instance2 := util.MakeInstance(ballot, index2)

	r1 := sendAccept(stub, instance1)
	assert.EqualValues(t, pb.ResponseType_OK, r1.GetType())
	assert.True(t, log.IsEqualInstance(instance1, logs[0].Find(index1)))
	assert.Nil(t, logs[0].Find(index2))

	r2 := sendAccept(stub, instance2)
	assert.EqualValues(t, pb.ResponseType_OK, r2.GetType())
	assert.True(t, log.IsEqualInstance(instance1, logs[0].Find(index1)))
	assert.True(t, log.IsEqualInstance(instance2, logs[0].Find(index2)))

	peers[0].Stop()
}

func TestPrepareResponseWithHigherBallotChangesLeaderToFollower(t *testing.T) {
	setupPeers()
	defer tearDown()
	for _, peer := range peers {
		peer.StartServer()
		peer.Connect()
	}
	stub1 := makeStub(configs[0].Peers[1])

	peer0Ballot := peers[0].NextBallot()
	peer2Ballot := peers[2].NextBallot()

	r := sendCommit(stub1, peer2Ballot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r.GetType())
	assert.False(t, IsLeaderByPeer(peers[1]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[1]))

	assert.True(t, IsLeaderByPeer(peers[0]))
	peers[0].RunPreparePhase(peer0Ballot)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[0]))
}

func TestAcceptResponseWithHigherBallotChangesLeaderToFollower(t *testing.T) {
	setupPeers()
	defer tearDown()
	for _, peer := range peers {
		peer.StartServer()
		peer.Connect()
	}
	stub1 := makeStub(configs[0].Peers[1])

	peer0Ballot := peers[0].NextBallot()
	peer2Ballot := peers[2].NextBallot()

	r := sendCommit(stub1, peer2Ballot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r.GetType())
	assert.False(t, IsLeaderByPeer(peers[1]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[1]))

	assert.True(t, IsLeaderByPeer(peers[0]))
	r2 := peers[0].RunAcceptPhase(peer0Ballot, 1, &pb.Command{}, 0)
	assert.EqualValues(t, SomeElseLeader, r2.Type)
	assert.EqualValues(t, 2, r2.Leader)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[0]))
}

func TestCommitResponseWithHigherBallotChangesLeaderToFollower(t *testing.T) {
	setupPeers()
	defer tearDown()
	for _, peer := range peers {
		peer.StartServer()
	}
	peers[0].Connect()
	stub1 := makeStub(configs[0].Peers[1])

	peer0Ballot := peers[0].NextBallot()
	peer2Ballot := peers[2].NextBallot()

	r := sendCommit(stub1, peer2Ballot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r.GetType())
	assert.False(t, IsLeaderByPeer(peers[1]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[1]))

	assert.True(t, IsLeaderByPeer(peers[0]))
	peers[0].RunCommitPhase(peer0Ballot, 0)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[0]))
}

func TestSendPreparesMajority(t *testing.T) {
	setupPeers()
	peers[0].StartServer()
	defer peers[0].Stop()

	expect := make(map[int64]*pb.Instance)
	ballot := peers[0].NextBallot()
	index := logs[0].AdvanceLastIndex()
	for i, log := range logs {
		instance := util.MakeInstance(ballot, index)
		expect[index] = instance
		log.Append(instance)
		log.Commit(index)
		log.Execute(stores[i])
	}
	assert.Nil(t, peers[0].RunPreparePhase(ballot))

	peers[1].StartServer()
	defer peers[1].Stop()
	time.Sleep(2 * time.Second)

	logMap := peers[0].RunPreparePhase(ballot)
	assert.Equal(t, len(expect), len(logMap))
	for index, instance := range expect {
		assert.True(t, log.IsEqualInstance(instance, logMap[index]))
	}
}

func TestSendPreparesWithReplay(t *testing.T) {
	setupPeers()
	defer tearDown()
	peers[0].StartServer()
	peers[1].StartServer()
	peers[0].Connect()

	ballot0 := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	instance1 := util.MakeInstanceWithState(ballot0, index1, pb.InstanceState_COMMITTED)
	logs[0].Append(instance1)

	ballot2 := peers[2].NextBallot()
	logs[2].Append(util.MakeInstance(ballot2, logs[2].AdvanceLastIndex()))
	index2 := logs[2].AdvanceLastIndex()
	instance2 := util.MakeInstance(ballot2, index2)
	logs[1].Append(instance2)
	logs[2].Append(util.MakeInstance(ballot2, index2))

	leaderBallot := peers[0].NextBallot()
	logMap := peers[0].RunPreparePhase(leaderBallot)
	assert.EqualValues(t, 2, len(logMap))
	assert.True(t, log.IsEqualInstance(instance1, logMap[index1]))
	assert.True(t, log.IsEqualInstance(instance2, logMap[index2]))

	peers[2].StartServer()
	time.Sleep(2 * time.Second)

	peers[0].Replay(leaderBallot, logMap)
	time.Sleep(100 * time.Millisecond)

	expect1 := util.MakeInstance(leaderBallot, index1)
	assert.True(t, log.IsCommitted(logs[0].Find(index1)))
	assert.EqualValues(t, ballot0, logs[0].Find(index1).GetBallot())
	assert.True(t, log.IsEqualInstance(expect1, logs[2].Find(index1)))
	assert.True(t, log.IsEqualInstance(expect1, logs[1].Find(index1)))

	expect2 := util.MakeInstance(leaderBallot, index2)
	assert.True(t, log.IsCommitted(logs[0].Find(index2)))
	assert.True(t, log.IsEqualInstance(expect2, logs[1].Find(index2)))
	assert.True(t, log.IsEqualInstance(expect2, logs[2].Find(index2)))
}

func TestRunAcceptPhase(t *testing.T) {
	setupPeers()
	peers[0].StartServer()
	defer peers[0].Stop()

	ballot := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	instance1 := util.MakeInstanceWithType(ballot, index1, pb.CommandType_PUT)

	r1 := peers[0].RunAcceptPhase(ballot, index1, &pb.Command{Type: pb.CommandType_PUT}, 0)
	assert.EqualValues(t, Retry, r1.Type)
	assert.EqualValues(t, NoLeader, r1.Leader)

	assert.True(t, log.IsInProgress(logs[0].Find(index1)))
	assert.Nil(t, logs[1].Find(index1))
	assert.Nil(t, logs[2].Find(index1))

	peers[1].StartServer()
	defer peers[1].Stop()
	peers[0].Connect()

	r2 := peers[0].RunAcceptPhase(ballot, index1, &pb.Command{Type: pb.CommandType_PUT}, 0)
	assert.EqualValues(t, Ok, r2.Type)
	assert.EqualValues(t, NoLeader, r2.Leader)

	assert.True(t, log.IsCommitted(logs[0].Find(index1)))
	assert.True(t, log.IsEqualInstance(instance1, logs[1].Find(index1)))
	assert.Nil(t, logs[2].Find(index1))
}

func TestRunCommitPhase(t *testing.T) {
	setupPeers()
	defer tearDown()
	peers[0].StartServer()
	peers[1].StartServer()

	numInstances := int64(3)
	ballot := peers[0].NextBallot()

	for index := int64(1); index <= numInstances; index++ {
		for peerId, log := range logs {
			if peerId == 2 && index == 3 {
				continue
			}
			instance := util.MakeInstanceWithState(ballot, index,
				pb.InstanceState_COMMITTED)
			log.Append(instance)
			log.Execute(stores[peerId])
		}
	}

	gle := int64(0)
	gle = peers[0].RunCommitPhase(ballot, gle)
	assert.EqualValues(t, 0, gle)

	peers[2].StartServer()
	logs[2].Append(util.MakeInstance(ballot, 3))
	time.Sleep(2 * time.Second)

	gle = peers[0].RunCommitPhase(ballot, gle)
	assert.EqualValues(t, 2, gle)

	logs[2].Execute(stores[2])

	gle = peers[0].RunCommitPhase(ballot, gle)
	assert.EqualValues(t, numInstances, gle)
}

func TestReplicateRetry(t *testing.T) {
	setupPeers()

	r1 := peers[0].Replicate(&pb.Command{}, 0)
	assert.Equal(t, Retry, r1.Type)
	assert.Equal(t, NoLeader, r1.Leader)

	peers[0].NextBallot()
	r2 := peers[0].Replicate(&pb.Command{}, 0)
	assert.Equal(t, Retry, r2.Type)
	assert.Equal(t, NoLeader, r2.Leader)
}

func TestReplicateSomeOneElseLeader(t *testing.T) {
	setupPeers()
	peers[0].StartServer()
	peers[1].StartServer()
	defer peers[0].Stop()
	defer peers[1].Stop()
	stub := makeStub(configs[0].Peers[0])

	ballot := peers[1].NextBallot()
	r1 := sendCommit(stub, ballot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r1.GetType())
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	r2 := peers[0].Replicate(&pb.Command{}, 0)
	assert.EqualValues(t, SomeElseLeader, r2.Type)
	assert.EqualValues(t, 1, r2.Leader)
}

func TestReplicateReady(t *testing.T) {
	setupPeers()
	defer tearDown()

	peers[1].StartServer()
	peers[2].StartServer()
	peers[0].Start()
	time.Sleep(1000 * time.Millisecond)
	result := peers[0].Replicate(&pb.Command{Type: pb.CommandType_PUT}, 0)
	assert.Equal(t, Ok, result.Type)
	assert.Equal(t, int64(0), result.Leader)
}

func TestOneLeaderElected(t *testing.T) {
	setupServer()
	defer tearDown()

	time.Sleep(time.Duration(3 * configs[0].CommitInterval) * time.Millisecond)
	assert.True(t, oneLeader())
}

func TestProposeCommand(t *testing.T) {
	setupServer()
	defer tearDown()

	time.Sleep(time.Duration(3 * configs[0].CommitInterval) * time.Millisecond)
	for !oneLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	results := make([]Result, 0, len(peers))
	for _, peer := range peers {
		results = append(results, peer.Replicate(&pb.Command{}, 0))
	}

	numOk := 0
	numSomeoneElseLeader := 0
	for _, r := range results {
		if r.Type == Ok {
			numOk++
		} else if r.Type == SomeElseLeader {
			numSomeoneElseLeader++
		}
	}
	assert.EqualValues(t, 1, numOk)
	assert.EqualValues(t, SomeElseLeader, numSomeoneElseLeader)

	for _, log := range logs {
		assert.EqualValues(t, 2, log.AdvanceLastIndex())
	}
}

func TestTrimAfterExecution(t *testing.T) {
	setupServer()
	defer tearDown()

	time.Sleep(time.Duration(3 * configs[0].CommitInterval) * time.Millisecond)
	for !oneLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	leaderId := int64(0)
	numCmds := 10
	result := peers[0].Replicate(&pb.Command{}, 0)
	if result.Type == SomeElseLeader {
		leaderId = result.Leader
	}

	for i := 0; i < numCmds; i++ {
		result = peers[leaderId].Replicate(&pb.Command{}, 0)
		assert.EqualValues(t, Ok, result.Type)
	}

	gle := logs[leaderId].AdvanceLastIndex() - 1
	var wg sync.WaitGroup
	for i, l := range logs {
		wg.Add(1)
		go func(log *log.Log) {
			for index := int64(0); index < gle; index++ {
				log.Execute(stores[i])
			}
			wg.Done()
		}(l)
	}
	wg.Wait()

	time.Sleep(time.Duration(3 * configs[0].CommitInterval) * time.Millisecond)

	for _, log := range logs {
		assert.EqualValues(t, gle, log.GlobalLastExecuted())
	}
}

func TestProposeWithFailPeer(t *testing.T) {
	setupPeers()
	peers[0].StartServer()
	peers[1].StartServer()
	peers[0].Connect()

	ballot0 := peers[0].NextBallot()
	peers[0].RunPreparePhase(ballot0)

	cmd1 := &pb.Command{Type: pb.CommandType_PUT}
	peers[0].Replicate(cmd1, 0)
	assert.EqualValues(t, pb.CommandType_PUT, logs[0].Find(1).GetCommand().GetType())
	assert.EqualValues(t, pb.CommandType_PUT, logs[1].Find(1).GetCommand().
		GetType())

	peers[0].Stop()
	peers[2].StartServer()
	time.Sleep(2 * time.Second)

	ballot2 := peers[2].NextBallot()
	logMap := peers[2].RunPreparePhase(ballot2)
	peers[2].Replay(ballot2, logMap)
	r1 := peers[2].Replicate(&pb.Command{Type: pb.CommandType_DEL}, 2)
	for r1.Type == Retry {
		r1 = peers[2].Replicate(&pb.Command{Type: pb.CommandType_DEL}, 2)
	}
	assert.EqualValues(t, Ok, r1.Type)
	assert.EqualValues(t, 2, logs[0].AdvanceLastIndex())
	assert.EqualValues(t, 3, logs[1].AdvanceLastIndex())
	assert.EqualValues(t, 3, logs[2].AdvanceLastIndex())

	assert.EqualValues(t, pb.CommandType_DEL, logs[1].Find(2).GetCommand().
		GetType())
	assert.EqualValues(t, pb.CommandType_PUT, logs[2].Find(1).GetCommand().
		GetType())
	assert.EqualValues(t, pb.CommandType_DEL, logs[2].Find(2).GetCommand().
		GetType())
}

func makeStub(target string) pb.MultiPaxosRPCClient {
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.
		NewCredentials()))
	if err != nil {
		panic("dial error")
	}
	stub := pb.NewMultiPaxosRPCClient(conn)
	return stub
}

func oneLeader() bool {
	if IsLeaderByPeer(peers[0]) {
		return !IsLeaderByPeer(peers[1]) && !IsLeaderByPeer(peers[2]) &&
			LeaderByPeer(peers[1]) == 0 && LeaderByPeer(peers[2]) == 0
	}
	if IsLeaderByPeer(peers[1]) {
		return !IsLeaderByPeer(peers[0]) && !IsLeaderByPeer(peers[2]) &&
			LeaderByPeer(peers[0]) == 1 && LeaderByPeer(peers[2]) == 1
	}
	if IsLeaderByPeer(peers[2]) {
		return !IsLeaderByPeer(peers[0]) && !IsLeaderByPeer(peers[1]) &&
			LeaderByPeer(peers[0]) == 2 && LeaderByPeer(peers[1]) == 2
	}
	return false
}

func LeaderByPeer(peer *Multipaxos) int64 {
	ballot, _ := peer.Ballot()
	return Leader(ballot)
}

func IsLeaderByPeer(peer *Multipaxos) bool {
	ballot, _ := peer.Ballot()
	return IsLeader(ballot, peer.Id())
}

func IsSomeoneElseLeaderByPeer(peer *Multipaxos) bool {
	return !IsLeaderByPeer(peer) && LeaderByPeer(peer) < MaxNumPeers
}

func sendCommit(stub pb.MultiPaxosRPCClient, ballot int64,
	lastExecuted int64, globalLastExecuted int64) *pb.CommitResponse {
	ctx := context.Background()
	request := pb.CommitRequest{
		Ballot:             ballot,
		LastExecuted:       lastExecuted,
		GlobalLastExecuted: globalLastExecuted,
	}
	response, err := stub.Commit(ctx, &request)
	if err != nil {
		return nil
	}
	return response
}

func sendPrepare(stub pb.MultiPaxosRPCClient, ballot int64) *pb.PrepareResponse {
	ctx := context.Background()
	request := pb.PrepareRequest{Ballot: ballot}
	response, err := stub.Prepare(ctx, &request)
	if err != nil {
		return nil
	}
	return response
}

func sendAccept(stub pb.MultiPaxosRPCClient,
	inst *pb.Instance) *pb.AcceptResponse {
	ctx := context.Background()
	request := pb.AcceptRequest{
		Instance: inst,
	}
	response, err := stub.Accept(ctx, &request)
	if err != nil {
		return nil
	}
	return response
}
