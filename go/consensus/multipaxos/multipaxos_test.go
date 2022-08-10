package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	"github.com/psu-csl/replicated-store/go/log"
	"github.com/psu-csl/replicated-store/go/store"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"testing"
	"time"
)

const N = 3

var (
	configs []*config.Config
	logs    []*log.Log
	peers   []*Multipaxos
	stores  []*store.MemKVStore
)

func setupPeers() {
	logs[0] = log.NewLog()
	logs[1] = log.NewLog()
	logs[2] = log.NewLog()
	peers[0] = NewMultipaxos(config.DefaultConfig(0, N), logs[0])
	peers[1] = NewMultipaxos(config.DefaultConfig(1, N), logs[1])
	peers[2] = NewMultipaxos(config.DefaultConfig(2, N), logs[2])
}

func setup() {
	setupPeers()
	peers[0].Start()
	peers[1].Start()
	peers[2].Start()
}

func setupOnePeer() {
	logs[0] = log.NewLog()
	peers[0] = NewMultipaxos(config.DefaultConfig(0, 1), logs[0])
	peers[0].StartServer()
}

func tearDown() {
	peers[0].Stop()
	peers[1].Stop()
	peers[2].Stop()
}

func TestNewMultipaxos(t *testing.T) {
	logs[0] = log.NewLog()
	peers[0] = NewMultipaxos(config.DefaultConfig(0, N), logs[0])

	assert.Equal(t, MaxNumPeers, LeaderByPeer(peers[0]))
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.False(t, IsSomeoneElseLeaderByPeer(peers[0]))
}

func TestNextBallot(t *testing.T) {
	logs[2] = log.NewLog()
	peers[2] = NewMultipaxos(config.DefaultConfig(2, N), logs[0])
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

func TestHeartbeatIgnoreStaleRPC(t *testing.T) {
	setupPeers()
	peers[0].StartServer()
	stub := createStub()

	peers[0].NextBallot()
	peers[0].NextBallot()

	ctx := context.Background()
	request := pb.HeartbeatRequest{
		Ballot: peers[1].NextBallot(),
	}
	_, err := stub.Heartbeat(ctx, &request)

	assert.Nil(t, err)
	assert.True(t, IsLeaderByPeer(peers[0]))

	peers[0].Stop()
}

func TestHeartbeatChangesLeaderToFollower(t *testing.T) {
	setupPeers()
	peers[0].StartServer()
	stub := createStub()

	peers[0].NextBallot()

	ctx := context.Background()
	request := pb.HeartbeatRequest{
		Ballot: peers[1].NextBallot(),
	}
	_, err := stub.Heartbeat(ctx, &request)

	assert.Nil(t, err)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	peers[0].Stop()
}

func TestNextBallotAfterHeartbeat(t *testing.T) {
	setupPeers()
	peers[0].StartServer()
	stub := createStub()
	ballot := peers[0].Id()

	ctx := context.Background()
	request := pb.HeartbeatRequest{
		Ballot: peers[1].NextBallot(),
	}
	stub.Heartbeat(ctx, &request)
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	ballot += RoundIncrement
	ballot += RoundIncrement
	assert.EqualValues(t, ballot, peers[0].NextBallot())

	peers[0].Stop()
}

func TestOneLeaderElected(t *testing.T) {
	setup()
	defer tearDown()

	time.Sleep(1000 * time.Millisecond)
	assert.True(t, oneLeader())
}

func TestReplicateRetry(t *testing.T) {
	setupPeers()

	result := peers[0].Replicate(&pb.Command{Type: pb.CommandType_GET}, 0)
	assert.Equal(t, Retry, result.Type)
	assert.Equal(t, NoLeader, result.Leader)
}

func TestReplicateWithLeader(t *testing.T) {
	setupPeers()

	peers[1].StartServer()
	peers[2].StartServer()
	peers[0].Start()
	time.Sleep(1000 * time.Millisecond)
	result := peers[0].Replicate(&pb.Command{Type: pb.CommandType_PUT}, 0)
	assert.Equal(t, Ok, result.Type)
	assert.Equal(t, int64(0), result.Leader)

	result = peers[1].Replicate(&pb.Command{Type: pb.CommandType_DEL}, 0)
	assert.Equal(t, SomeElseLeader, result.Type)
	assert.Equal(t, int64(0), result.Leader)

	peers[0].Stop()
}

func createStub() pb.MultiPaxosRPCClient {
	conn, err := grpc.Dial(":3000", grpc.WithTransportCredentials(insecure.
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

func sendHeartbeat(stub pb.MultiPaxosRPCClient, ballot int64,
	lastExecuted int64, globalLastExecuted int64) *pb.HeartbeatResponse {
	ctx := context.Background()
	request := pb.HeartbeatRequest{
		Ballot:             ballot,
		LastExecuted:       lastExecuted,
		GlobalLastExecuted: globalLastExecuted,
	}
	response, err := stub.Heartbeat(ctx, &request)
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
