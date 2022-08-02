package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	"github.com/psu-csl/replicated-store/go/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"testing"
	"time"
)

const N = 3

var (
	log0  *log.Log
	log1  *log.Log
	log2  *log.Log
	peer0 *Multipaxos
	peer1 *Multipaxos
	peer2 *Multipaxos
)

func setupPeers() {
	log0 = log.NewLog()
	log1 = log.NewLog()
	log2 = log.NewLog()
	peer0 = NewMultipaxos(config.DefaultConfig(0, N), log0)
	peer1 = NewMultipaxos(config.DefaultConfig(1, N), log1)
	peer2 = NewMultipaxos(config.DefaultConfig(2, N), log2)
}

func setup() {
	setupPeers()
	peer0.Start()
	peer1.Start()
	peer2.Start()
}

func setupOnePeer() {
	log0 = log.NewLog()
	peer0 = NewMultipaxos(config.DefaultConfig(0, 1), log0)
	peer0.StartServer()
}

func tearDown() {
	peer0.Stop()
	peer1.Stop()
	peer2.Stop()
}

func TestNewMultipaxos(t *testing.T) {
	log0 = log.NewLog()
	peer0 = NewMultipaxos(config.DefaultConfig(0, N), log0)

	assert.Equal(t, MaxNumPeers, LeaderByPeer(peer0))
	assert.False(t, IsLeaderByPeer(peer0))
	assert.False(t, IsSomeoneElseLeaderByPeer(peer0))
}

func TestNextBallot(t *testing.T) {
	log2 = log.NewLog()
	peer2 = NewMultipaxos(config.DefaultConfig(2, N), log0)
	id := peer2.Id()
	ballot := id

	ballot += RoundIncrement
	assert.Equal(t, ballot, peer2.NextBallot())
	ballot += RoundIncrement
	assert.Equal(t, ballot, peer2.NextBallot())

	assert.True(t, IsLeaderByPeer(peer2))
	assert.False(t, IsSomeoneElseLeaderByPeer(peer2))
	assert.Equal(t, id, LeaderByPeer(peer2))
}

func TestHeartbeatIgnoreStaleRPC(t *testing.T) {
	setupPeers()
	peer0.StartServer()
	stub := createStub()

	peer0.NextBallot()
	peer0.NextBallot()

	ctx := context.Background()
	request := pb.HeartbeatRequest{
		Ballot: peer1.NextBallot(),
	}
	_, err := stub.Heartbeat(ctx, &request)

	assert.Nil(t, err)
	assert.True(t, IsLeaderByPeer(peer0))

	peer0.Stop()
}

func TestHeartbeatChangesLeaderToFollower(t *testing.T) {
	setupPeers()
	peer0.StartServer()
	stub := createStub()

	peer0.NextBallot()

	ctx := context.Background()
	request := pb.HeartbeatRequest{
		Ballot: peer1.NextBallot(),
	}
	_, err := stub.Heartbeat(ctx, &request)

	assert.Nil(t, err)
	assert.False(t, IsLeaderByPeer(peer0))
	assert.EqualValues(t, 1, LeaderByPeer(peer0))

	peer0.Stop()
}

func TestNextBallotAfterHeartbeat(t *testing.T) {
	setupPeers()
	peer0.StartServer()
	stub := createStub()
	ballot := peer0.Id()

	ctx := context.Background()
	request := pb.HeartbeatRequest{
		Ballot: peer1.NextBallot(),
	}
	stub.Heartbeat(ctx, &request)
	assert.EqualValues(t, 1, LeaderByPeer(peer0))

	ballot += RoundIncrement
	ballot += RoundIncrement
	assert.EqualValues(t, ballot, peer0.NextBallot())

	peer0.Stop()
}

func TestOneLeaderElected(t *testing.T) {
	setup()
	defer tearDown()

	time.Sleep(1000 * time.Millisecond)
	assert.True(t, oneLeader())
}

func TestReplicateRetry(t *testing.T) {
	setupPeers()

	result := peer0.Replicate(&pb.Command{Type: pb.CommandType_GET}, 0)
	assert.Equal(t, Retry, result.Type)
	assert.Equal(t, NoLeader, result.Leader)
}

func TestReplicateWithLeader(t *testing.T) {
	setupPeers()

	peer1.StartServer()
	peer2.StartServer()
	peer0.Start()
	time.Sleep(1000 * time.Millisecond)
	result := peer0.Replicate(&pb.Command{Type: pb.CommandType_PUT}, 0)
	assert.Equal(t, Ok, result.Type)
	assert.Equal(t, int64(0), result.Leader)

	result = peer1.Replicate(&pb.Command{Type: pb.CommandType_DEL}, 0)
	assert.Equal(t, SomeElseLeader, result.Type)
	assert.Equal(t, int64(0), result.Leader)

	peer0.Stop()
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
	if IsLeaderByPeer(peer0) {
		return !IsLeaderByPeer(peer1) && !IsLeaderByPeer(peer2) &&
			LeaderByPeer(peer1) == 0 && LeaderByPeer(peer2) == 0
	}
	if IsLeaderByPeer(peer1) {
		return !IsLeaderByPeer(peer0) && !IsLeaderByPeer(peer2) &&
			LeaderByPeer(peer0) == 1 && LeaderByPeer(peer2) == 1
	}
	if IsLeaderByPeer(peer2) {
		return !IsLeaderByPeer(peer0) && !IsLeaderByPeer(peer1) &&
			LeaderByPeer(peer0) == 2 && LeaderByPeer(peer1) == 2
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
