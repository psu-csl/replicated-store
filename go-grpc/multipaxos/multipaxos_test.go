package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/kvstore"
	"github.com/psu-csl/replicated-store/go/log"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
	"github.com/psu-csl/replicated-store/go/util"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"testing"
	"time"
)

const NumPeers = 3

var (
	configs = make([]config.Config, NumPeers)
	logs    = make([]*log.Log, NumPeers)
	peers   = make([]*Multipaxos, NumPeers)
	stores  = make([]*kvstore.MemKVStore, NumPeers)
)

func initPeers() {
	for i := int64(0); i < NumPeers; i++ {
		configs[i] = config.DefaultConfig(i, NumPeers)
		stores[i] = kvstore.NewMemKVStore()
		logs[i] = log.NewLog(stores[i])
		peers[i] = NewMultipaxos(logs[i], configs[i])
	}
}

func setupOnePeer(id int64) {
	configs[id] = config.DefaultConfig(id, NumPeers)
	stores[id] = kvstore.NewMemKVStore()
	logs[id] = log.NewLog(stores[id])
	peers[id] = NewMultipaxos(logs[id], configs[id])
}

func tearDown() {
	for _, peer := range peers {
		peer.Stop()
	}
}

func tearDownServers() {
	for _, peer := range peers {
		peer.StopRPCServer()
	}
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

func Connect(multipaxos *Multipaxos, addrs []string) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	for i, addr := range addrs {
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			panic("dial error")
		}
		client := pb.NewMultiPaxosRPCClient(conn)
		multipaxos.rpcPeers.List[i] = NewRpcPeer(int64(i), client)
	}
}

func TestNewMultipaxos(t *testing.T) {
	setupOnePeer(0)

	assert.Equal(t, MaxNumPeers, LeaderByPeer(peers[0]))
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.False(t, IsSomeoneElseLeaderByPeer(peers[0]))
}

func TestNextBallot(t *testing.T) {
	initPeers()
	for id := 0; id < NumPeers; id++ {
		ballot := id
		ballot += RoundIncrement
		assert.EqualValues(t, ballot, peers[id].NextBallot())
	}
}

func TestRequestsWithLowerBallotIgnored(t *testing.T) {
	setupOnePeer(0)
	setupOnePeer(1)
	peers[0].StartRPCServer()
	stub := makeStub(configs[0].Peers[0])

	peers[0].BecomeLeader(peers[0].NextBallot(), logs[0].LastIndex())
	peers[0].BecomeLeader(peers[0].NextBallot(), logs[0].LastIndex())
	staleBallot := peers[1].NextBallot()

	r1 := sendPrepare(stub, staleBallot)
	assert.EqualValues(t, pb.ResponseType_REJECT, r1.GetType())
	assert.True(t, IsLeaderByPeer(peers[0]))

	index := logs[0].AdvanceLastIndex()
	instance := util.MakeInstance(staleBallot, index)
	r2 := sendAccept(stub, instance)
	assert.EqualValues(t, pb.ResponseType_REJECT, r2.GetType())
	assert.True(t, IsLeaderByPeer(peers[0]))
	assert.Nil(t, logs[0].At(index))

	r3 := sendCommit(stub, staleBallot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_REJECT, r3.GetType())
	assert.True(t, IsLeaderByPeer(peers[0]))

	peers[0].StopRPCServer()
}

func TestRequestsWithHigherBallotChangeLeaderToFollower(t *testing.T) {
	setupOnePeer(0)
	setupOnePeer(1)
	peers[0].StartRPCServer()
	stub := makeStub(configs[0].Peers[0])

	peers[0].BecomeLeader(peers[0].NextBallot(), logs[0].LastIndex())
	assert.True(t, IsLeaderByPeer(peers[0]))
	r1 := sendPrepare(stub, peers[1].NextBallot())
	assert.EqualValues(t, pb.ResponseType_OK, r1.GetType())
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	peers[1].BecomeLeader(peers[1].NextBallot(), logs[1].LastIndex())
	peers[0].BecomeLeader(peers[0].NextBallot(), logs[0].LastIndex())
	assert.True(t, IsLeaderByPeer(peers[0]))
	index := logs[0].AdvanceLastIndex()
	instance := util.MakeInstance(peers[1].NextBallot(), index)
	r2 := sendAccept(stub, instance)
	assert.EqualValues(t, pb.ResponseType_OK, r2.GetType())
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	peers[1].BecomeLeader(peers[1].NextBallot(), logs[1].LastIndex())
	peers[0].BecomeLeader(peers[0].NextBallot(), logs[0].LastIndex())
	assert.True(t, IsLeaderByPeer(peers[0]))
	r3 := sendCommit(stub, peers[1].NextBallot(), 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r3.GetType())
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	peers[0].StopRPCServer()
}

func TestNextBallotAfterCommit(t *testing.T) {
	initPeers()
	peers[0].StartRPCServer()
	stub := makeStub(configs[0].Peers[0])

	ballot := peers[0].Id()
	sendCommit(stub, peers[1].NextBallot(), 0, 0)
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	ballot += RoundIncrement
	ballot += RoundIncrement
	assert.EqualValues(t, ballot, peers[0].NextBallot())

	peers[0].StopRPCServer()
}

func TestCommitCommitsAndTrims(t *testing.T) {
	setupOnePeer(0)
	peers[0].StartRPCServer()
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
	assert.True(t, log.IsCommitted(logs[0].At(index1)))
	assert.True(t, log.IsCommitted(logs[0].At(index2)))
	assert.True(t, log.IsInProgress(logs[0].At(index3)))

	logs[0].ReadInstance()
	logs[0].ReadInstance()

	r2 := sendCommit(stub, ballot, index2, index2)
	assert.EqualValues(t, pb.ResponseType_OK, r2.GetType())
	assert.EqualValues(t, index2, r2.LastExecuted)
	assert.Nil(t, logs[0].At(index1))
	assert.Nil(t, logs[0].At(index2))
	assert.True(t, log.IsInProgress(logs[0].At(index3)))

	peers[0].StopRPCServer()
}

func TestPrepareRespondsWithCorrectInstances(t *testing.T) {
	setupOnePeer(0)
	peers[0].StartRPCServer()
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

	logs[0].ReadInstance()
	logs[0].ReadInstance()

	ballot = peers[0].NextBallot()
	r3 := sendPrepare(stub, ballot)
	assert.EqualValues(t, pb.ResponseType_OK, r3.GetType())
	assert.EqualValues(t, 3, len(r3.GetLogs()))
	assert.True(t, log.IsExecuted(r3.GetLogs()[0]))
	assert.True(t, log.IsExecuted(r3.GetLogs()[1]))
	assert.True(t, log.IsInProgress(r1.GetLogs()[2]))

	r4 := sendCommit(stub, ballot, index2, 2)
	assert.EqualValues(t, pb.ResponseType_OK, r4.GetType())

	ballot = peers[0].NextBallot()
	r5 := sendPrepare(stub, ballot)
	assert.EqualValues(t, pb.ResponseType_OK, r5.GetType())
	assert.EqualValues(t, 1, len(r5.GetLogs()))
	assert.True(t, log.IsEqualInstance(instance3, r5.GetLogs()[0]))

	peers[0].StopRPCServer()
}

func TestAcceptAppendsToLog(t *testing.T) {
	setupOnePeer(0)
	peers[0].StartRPCServer()
	stub := makeStub(configs[0].Peers[0])

	ballot := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	instance1 := util.MakeInstance(ballot, index1)
	index2 := logs[0].AdvanceLastIndex()
	instance2 := util.MakeInstance(ballot, index2)

	r1 := sendAccept(stub, instance1)
	assert.EqualValues(t, pb.ResponseType_OK, r1.GetType())
	assert.True(t, log.IsEqualInstance(instance1, logs[0].At(index1)))
	assert.Nil(t, logs[0].At(index2))

	r2 := sendAccept(stub, instance2)
	assert.EqualValues(t, pb.ResponseType_OK, r2.GetType())
	assert.True(t, log.IsEqualInstance(instance1, logs[0].At(index1)))
	assert.True(t, log.IsEqualInstance(instance2, logs[0].At(index2)))

	peers[0].StopRPCServer()
}

func TestPrepareResponseWithHigherBallotChangesLeaderToFollower(t *testing.T) {
	initPeers()
	defer tearDownServers()
	for _, peer := range peers {
		peer.StartRPCServer()
	}
	for id, peer := range peers {
		Connect(peer, configs[id].Peers)
	}
	stub1 := makeStub(configs[0].Peers[1])

	peer0Ballot := peers[0].NextBallot()
	peers[0].BecomeLeader(peer0Ballot, logs[0].LastIndex())
	peers[1].BecomeLeader(peers[1].NextBallot(), logs[1].LastIndex())
	peer2Ballot := peers[2].NextBallot()
	peers[2].BecomeLeader(peer2Ballot, logs[2].LastIndex())
	peer2Ballot = peers[2].NextBallot()
	peers[2].BecomeLeader(peer2Ballot, logs[2].LastIndex())

	r := sendCommit(stub1, peer2Ballot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r.GetType())
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
	for _, peer := range peers {
		peer.StartRPCServer()
	}
	for id, peer := range peers {
		Connect(peer, configs[id].Peers)
	}
	stub1 := makeStub(configs[0].Peers[1])

	peer0Ballot := peers[0].NextBallot()
	peers[0].BecomeLeader(peer0Ballot, logs[0].LastIndex())
	peers[1].BecomeLeader(peers[1].NextBallot(), logs[1].LastIndex())
	peer2Ballot := peers[2].NextBallot()
	peers[2].BecomeLeader(peer2Ballot, logs[2].LastIndex())

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
	initPeers()
	defer tearDownServers()
	for _, peer := range peers {
		peer.StartRPCServer()
	}
	Connect(peers[0], configs[0].Peers)
	stub1 := makeStub(configs[0].Peers[1])

	peer0Ballot := peers[0].NextBallot()
	peers[0].BecomeLeader(peer0Ballot, logs[0].LastIndex())
	peers[1].BecomeLeader(peers[1].NextBallot(), logs[1].LastIndex())
	peer2Ballot := peers[2].NextBallot()
	peers[2].BecomeLeader(peer2Ballot, logs[2].LastIndex())

	r := sendCommit(stub1, peer2Ballot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r.GetType())
	assert.False(t, IsLeaderByPeer(peers[1]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[1]))

	assert.True(t, IsLeaderByPeer(peers[0]))
	peers[0].RunCommitPhase(peer0Ballot, 0)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[0]))
}

func TestRunPreparePhase(t *testing.T) {
	initPeers()
	peers[0].StartRPCServer()
	defer peers[0].StopRPCServer()

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

	expectedLog := make(map[int64]*pb.Instance)

	logs[0].Append(util.MakeInstanceWithType(ballot0, index1,
		pb.CommandType_PUT))
	logs[1].Append(util.MakeInstanceWithType(ballot0, index1,
		pb.CommandType_PUT))
	expectedLog[index1] = util.MakeInstanceWithType(ballot0, index1,
		pb.CommandType_PUT)

	logs[1].Append(util.MakeInstance(ballot0, index2))
	expectedLog[index2] = util.MakeInstance(ballot0, index2)

	logs[0].Append(util.MakeInstanceWithAll(ballot0, index3,
		pb.InstanceState_COMMITTED, pb.CommandType_DEL))
	logs[1].Append(util.MakeInstanceWithAll(ballot1, index3,
		pb.InstanceState_INPROGRESS, pb.CommandType_DEL))
	expectedLog[index3] = util.MakeInstanceWithAll(ballot0, index3,
		pb.InstanceState_COMMITTED, pb.CommandType_DEL)

	logs[0].Append(util.MakeInstanceWithAll(ballot0, index4,
		pb.InstanceState_EXECUTED, pb.CommandType_DEL))
	logs[1].Append(util.MakeInstanceWithAll(ballot1, index4,
		pb.InstanceState_INPROGRESS, pb.CommandType_DEL))
	expectedLog[index4] = util.MakeInstanceWithAll(ballot0, index4,
		pb.InstanceState_EXECUTED, pb.CommandType_DEL)

	ballot0 = peers[0].NextBallot()
	peers[0].BecomeLeader(ballot0, logs[0].LastIndex())
	ballot1 = peers[1].NextBallot()
	peers[1].BecomeLeader(ballot1, logs[1].LastIndex())

	logs[0].Append(util.MakeInstanceWithAll(ballot0, index5,
		pb.InstanceState_INPROGRESS, pb.CommandType_GET))
	logs[1].Append(util.MakeInstanceWithAll(ballot1, index5,
		pb.InstanceState_INPROGRESS, pb.CommandType_PUT))
	expectedLog[index5] = util.MakeInstanceWithAll(ballot1, index5,
		pb.InstanceState_INPROGRESS, pb.CommandType_PUT)

	ballot := peers[0].NextBallot()
	_, logMap := peers[0].RunPreparePhase(ballot)
	assert.Nil(t, logMap)

	peers[1].StartRPCServer()
	defer peers[1].StopRPCServer()
	Connect(peers[0], configs[0].Peers)

	lastIndex, logMap := peers[0].RunPreparePhase(ballot)
	assert.EqualValues(t, 5, lastIndex)
	for index, instance := range logMap {
		if index == 3 || index == 4 {
			assert.True(t, log.IsEqualCommand(expectedLog[index].GetCommand(), instance.GetCommand()))
		} else {
			assert.True(t, log.IsEqualInstance(expectedLog[index], instance),
				"index: %v", index)
		}
	}
}

func TestRunAcceptPhase(t *testing.T) {
	initPeers()
	peers[0].StartRPCServer()
	defer peers[0].StopRPCServer()

	ballot := peers[0].NextBallot()
	peers[0].BecomeLeader(ballot, logs[0].LastIndex())
	index1 := logs[0].AdvanceLastIndex()
	instance1 := util.MakeInstanceWithType(ballot, index1, pb.CommandType_PUT)

	r1 := peers[0].RunAcceptPhase(ballot, index1, &pb.Command{Type: pb.CommandType_PUT}, 0)
	assert.EqualValues(t, Retry, r1.Type)
	assert.EqualValues(t, -1, r1.Leader)

	assert.True(t, log.IsInProgress(logs[0].At(index1)))
	assert.Nil(t, logs[1].At(index1))
	assert.Nil(t, logs[2].At(index1))

	peers[1].StartRPCServer()
	defer peers[1].StopRPCServer()
	Connect(peers[0], configs[0].Peers)

	r2 := peers[0].RunAcceptPhase(ballot, index1, &pb.Command{Type: pb.CommandType_PUT}, 0)
	assert.EqualValues(t, Ok, r2.Type)
	assert.EqualValues(t, -1, r2.Leader)

	assert.True(t, log.IsCommitted(logs[0].At(index1)))
	assert.True(t, log.IsEqualInstance(instance1, logs[1].At(index1)))
	assert.Nil(t, logs[2].At(index1))
}

func TestRunCommitPhase(t *testing.T) {
	initPeers()
	defer tearDownServers()
	peers[0].StartRPCServer()
	peers[1].StartRPCServer()

	numInstances := int64(3)
	ballot := peers[0].NextBallot()
	peers[0].BecomeLeader(ballot, logs[0].LastIndex())

	for index := int64(1); index <= numInstances; index++ {
		for peerId, log := range logs {
			if peerId == 2 && index == 3 {
				continue
			}
			instance := util.MakeInstanceWithState(ballot, index,
				pb.InstanceState_COMMITTED)
			log.Append(instance)
			log.ReadInstance()
		}
	}

	gle := int64(0)
	gle = peers[0].RunCommitPhase(ballot, gle)
	assert.EqualValues(t, 0, gle)

	peers[2].StartRPCServer()
	logs[2].Append(util.MakeInstance(ballot, 3))
	time.Sleep(2 * time.Second)

	gle = peers[0].RunCommitPhase(ballot, gle)
	assert.EqualValues(t, 2, gle)

	logs[2].ReadInstance()

	gle = peers[0].RunCommitPhase(ballot, gle)
	assert.EqualValues(t, numInstances, gle)
}

func TestReplay(t *testing.T) {
	initPeers()
	peers[0].StartRPCServer()
	peers[1].StartRPCServer()
	defer peers[0].StopRPCServer()
	defer peers[1].StopRPCServer()
	Connect(peers[0], configs[0].Peers)

	ballot := peers[0].NextBallot()
	peers[0].BecomeLeader(ballot, logs[0].LastIndex())

	const (
		index1 int64 = iota + 1
		index2
		index3
	)
	replayLog := map[int64]*pb.Instance{
		index1: util.MakeInstanceWithAll(ballot, index1,
			pb.InstanceState_COMMITTED, pb.CommandType_PUT),
		index2: util.MakeInstanceWithAll(ballot, index2,
			pb.InstanceState_EXECUTED, pb.CommandType_GET),
		index3: util.MakeInstanceWithAll(ballot, index3,
			pb.InstanceState_INPROGRESS, pb.CommandType_DEL),
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
		index1, pb.InstanceState_COMMITTED, pb.CommandType_PUT),
		logs[0].At(index1)))
	assert.True(t, log.IsEqualInstance(util.MakeInstanceWithAll(newBallot,
		index2, pb.InstanceState_COMMITTED, pb.CommandType_GET),
		logs[0].At(index2)))
	assert.True(t, log.IsEqualInstance(util.MakeInstanceWithAll(newBallot,
		index3, pb.InstanceState_COMMITTED, pb.CommandType_DEL),
		logs[0].At(index3)))

	assert.True(t, log.IsEqualInstance(util.MakeInstanceWithAll(newBallot,
		index1, pb.InstanceState_INPROGRESS, pb.CommandType_PUT),
		logs[1].At(index1)))
	assert.True(t, log.IsEqualInstance(util.MakeInstanceWithAll(newBallot,
		index2, pb.InstanceState_INPROGRESS, pb.CommandType_GET),
		logs[1].At(index2)))
	assert.True(t, log.IsEqualInstance(util.MakeInstanceWithAll(newBallot,
		index3, pb.InstanceState_INPROGRESS, pb.CommandType_DEL),
		logs[1].At(index3)))
}

func TestReplicate(t *testing.T) {
	initPeers()
	defer tearDown()
	peers[0].Start()

	r1 := peers[0].Replicate(&pb.Command{}, 0)
	assert.Equal(t, Retry, r1.Type)
	assert.EqualValues(t, -1, r1.Leader)

	peers[1].Start()
	peers[2].Start()

	time.Sleep(5 * time.Second)

	leader := oneLeader()
	assert.NotEqualValues(t, -1, leader)

	r2 := peers[leader].Replicate(&pb.Command{}, 0)
	assert.Equal(t, Ok, r2.Type)

	notLeader := (leader + 1) % NumPeers
	r3 := peers[notLeader].Replicate(&pb.Command{}, 0)
	assert.EqualValues(t, SomeElseLeader, r3.Type)
	assert.EqualValues(t, leader, r3.Leader)
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
