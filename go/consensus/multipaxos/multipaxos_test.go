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

	assert.Equal(t, MaxNumPeers, peer0.Leader())
	assert.False(t, peer0.IsLeader())
	assert.False(t, peer0.IsSomeoneElseLeader())
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

	assert.True(t, peer2.IsLeader())
	assert.False(t, peer2.IsSomeoneElseLeader())
	assert.Equal(t, id, peer2.Leader())
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
	assert.True(t, peer0.IsLeader())

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
	assert.False(t, peer0.IsLeader())
	assert.EqualValues(t, 1, peer0.Leader())

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
	assert.EqualValues(t, 1, peer0.Leader())

	ballot += RoundIncrement
	ballot += RoundIncrement
	assert.EqualValues(t, ballot, peer0.NextBallot())

	peer0.Stop()
}

//func TestOneLeaderElected(t *testing.T) {
//	setup()
//	defer tearDown()
//
//	time.Sleep(1 * time.Second)
//
//}

func createStub() pb.MultiPaxosRPCClient {
	conn, err := grpc.Dial(":3000", grpc.WithTransportCredentials(insecure.
		NewCredentials()))
	if err != nil {
		panic("dial error")
	}
	stub := pb.NewMultiPaxosRPCClient(conn)
	return stub
}
