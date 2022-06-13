package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/command"
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	"github.com/psu-csl/replicated-store/go/log"
	"github.com/psu-csl/replicated-store/go/store"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"sync"
	"testing"
)

var (
	multiPaxos *Multipaxos
)

const BufSize = 1024 * 1024

func setup() {
	config := config.Config{
		Id: 0,
	}
	multiPaxos = NewMultipaxos(config, log.NewLog())
}

func setupBatchPeers(numPeers int64) []*Multipaxos {
	peers := make([]*Multipaxos, numPeers)
	for id := int64(0); id < numPeers; id++ {
		config := config.Config{
			Id: id,
		}
		multiPaxos = NewMultipaxos(config, log.NewLog())
		peers[id] = multiPaxos
	}
	return peers
}

func initConn(t *testing.T) (pb.MultiPaxosRPCClient, context.Context) {
	setup()
	ctx := context.Background()
	// Create a server
	listener := bufconn.Listen(BufSize)
	server := grpc.NewServer()
	pb.RegisterMultiPaxosRPCServer(server, multiPaxos)
	go server.Serve(listener)

	// Create the connection
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(ctx context.Context,
			s string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to init connection to grpc server\n")
	}
	client := pb.NewMultiPaxosRPCClient(conn)
	return client, ctx
}

func makeAcceptRequest(ballot int64) *pb.AcceptRequest {
	return &pb.AcceptRequest{Ballot: ballot, Command: &pb.Command{Type: pb.
		Command_Put}, Index: multiPaxos.log.AdvanceLastIndex(), ClientId: 0}
}

func makeAcceptRequestByIndex(ballot int64, index int64) *pb.AcceptRequest {
	return &pb.AcceptRequest{Ballot: ballot, Command: &pb.Command{Type: pb.
		Command_Put}, Index: index, ClientId: 0}
}

func TestNewMultipaxos(t *testing.T) {
	setup()

	assert.Equal(t, MaxNumPeers, multiPaxos.Leader())
	assert.False(t, multiPaxos.IsLeader())
	assert.False(t, multiPaxos.IsSomeoneElseLeader())
}

func TestNextBallot(t *testing.T) {
	log_ := log.NewLog()
	for id := int64(0); id <= MaxNumPeers; id++ {
		config := config.Config{
			Id: id,
		}
		mp := NewMultipaxos(config, log_)
		ballot := id

		ballot += RoundIncrement
		assert.Equal(t, ballot, mp.NextBallot())
		ballot += RoundIncrement
		assert.Equal(t, ballot, mp.NextBallot())

		assert.True(t, mp.IsLeader())
		assert.False(t, mp.IsSomeoneElseLeader())
		assert.Equal(t, id, mp.Leader())
	}
}

func TestNextBallotFromFollower(t *testing.T) {
	log_ := log.NewLog()
	ctx := context.Background()
	for id := int64(0); id <= MaxNumPeers; id++ {
		config := config.Config{
			Id: id,
		}
		mp := NewMultipaxos(config, log_)
		ballot := id

		ballot += RoundIncrement
		heartbeatRequest := pb.HeartbeatRequest{
			Ballot:             ballot + 1,
			LastExecuted:       1,
			GlobalLastExecuted: 0,
		}
		_, _ = mp.HeartbeatHandler(ctx, &heartbeatRequest)
		assert.NotEqual(t, ballot, mp.Ballot())
		ballot += RoundIncrement
		assert.Equal(t, ballot, mp.NextBallot())

		ballot += RoundIncrement
		ballot += RoundIncrement
		heartbeatRequest.Ballot = ballot + 1
		_, _ = mp.HeartbeatHandler(ctx, &heartbeatRequest)
		assert.NotEqual(t, ballot, mp.Ballot())
		ballot += RoundIncrement
		assert.Equal(t, ballot, mp.NextBallot())
	}
}

func TestAcceptHandler(t *testing.T) {
	client, ctx := initConn(t)

	const(
		index1 int64 = iota + 1
		index2
	)
	ballot := MaxNumPeers - 1 + RoundIncrement
	resp, err := client.AcceptHandler(ctx, makeAcceptRequestByIndex(ballot, index1))
	assert.Nil(t, err)
	assert.Equal(t, pb.AcceptResponse_ok, resp.GetType())
	assert.Equal(t, command.Put, multiPaxos.log.Find(index1).Command().Type)

	resp, err = client.AcceptHandler(ctx, makeAcceptRequestByIndex(ballot, index2))
	assert.Nil(t, err)
	assert.Equal(t, pb.AcceptResponse_ok, resp.GetType())
	assert.Equal(t, command.Put, multiPaxos.log.Find(index1).Command().Type)
}

func TestAcceptHandlerWithGap(t *testing.T) {
	client, ctx := initConn(t)

	var(
		index1 int64 = 41
		index2 int64 = 2
	)
	ballot := MaxNumPeers - 1 + RoundIncrement
	resp, err := client.AcceptHandler(ctx, makeAcceptRequestByIndex(ballot, index1))
	assert.Nil(t, err)
	assert.Equal(t, pb.AcceptResponse_ok, resp.GetType())
	assert.Equal(t, command.Put, multiPaxos.log.Find(index1).Command().Type)

	resp, err = client.AcceptHandler(ctx, makeAcceptRequestByIndex(ballot, index2))
	assert.Nil(t, err)
	assert.Equal(t, pb.AcceptResponse_ok, resp.GetType())
	assert.Equal(t, command.Put, multiPaxos.log.Find(index2).Command().Type)
}

func TestAcceptHandlerDuplicatedIndex(t *testing.T) {
	client, ctx := initConn(t)

	const(
		index1 int64 = iota + 1
		index2
		highBallot = MaxNumPeers -1 + RoundIncrement
	)
	ballot := MaxNumPeers + 1
	_, _ = client.AcceptHandler(ctx, makeAcceptRequestByIndex(ballot, index1))
	_, _ = client.AcceptHandler(ctx, makeAcceptRequestByIndex(ballot, index2))
	multiPaxos.log.Commit(index1)
	expected1 := multiPaxos.log.Find(index1)
	expected2 := multiPaxos.log.Find(index2)

	resp, err := client.AcceptHandler(ctx, makeAcceptRequestByIndex(highBallot,
		index1))
	assert.Nil(t, err)
	assert.Equal(t, pb.AcceptResponse_ok, resp.GetType())
	assert.Equal(t, expected1, multiPaxos.log.Find(index1))

	resp, err = client.AcceptHandler(ctx, makeAcceptRequestByIndex(highBallot,
		index2))
	assert.Nil(t, err)
	assert.Equal(t, pb.AcceptResponse_ok, resp.GetType())
	assert.NotEqual(t, expected2, multiPaxos.log.Find(index2))
}

func TestAcceptHandlerBallot(t *testing.T) {
	client, ctx := initConn(t)
	const (
		index1 int64 = iota + 1
		index2
		index3
		leaderId = MaxNumPeers - 1
		highBallot = MaxNumPeers -1 + RoundIncrement
		lowBallot  = MaxNumPeers + 1
	)

	// Higher ballot number
	resp, err := client.AcceptHandler(ctx, makeAcceptRequestByIndex(highBallot, index1))
	assert.Nil(t, err)
	assert.Equal(t, pb.AcceptResponse_ok, resp.GetType())
	assert.Equal(t, highBallot, multiPaxos.Ballot())
	assert.Equal(t, leaderId, multiPaxos.Leader())

	// Lower ballot number
	resp, err = client.AcceptHandler(ctx, makeAcceptRequestByIndex(lowBallot, index2))
	assert.Nil(t, err)
	assert.Equal(t, pb.AcceptResponse_reject, resp.GetType())
	assert.Equal(t, highBallot, multiPaxos.Ballot())
	assert.Equal(t, leaderId, multiPaxos.Leader())

	// Same ballot number
	resp, err = client.AcceptHandler(ctx, makeAcceptRequestByIndex(highBallot, index3))
	assert.Nil(t, err)
	assert.Equal(t, pb.AcceptResponse_ok, resp.GetType())
	assert.Equal(t, highBallot, multiPaxos.Ballot())
	assert.Equal(t, leaderId, multiPaxos.Leader())
}

func TestHeartbeatHandlerBallot(t *testing.T) {
	const (
		leaderId = MaxNumPeers - 1
		staleBallot  = MaxNumPeers + 1
	)
	client, ctx := initConn(t)
	ballot := MaxNumPeers - 1 + RoundIncrement

	// Higher Ballot number
	request := pb.HeartbeatRequest{
		Ballot:             ballot,
		LastExecuted:       1,
		GlobalLastExecuted: 0,
	}
	_, err := client.HeartbeatHandler(ctx, &request)
	ts := multiPaxos.LastHeartbeat()
	assert.Nil(t, err)
	assert.Equal(t, ballot, multiPaxos.Ballot())
	assert.True(t, multiPaxos.IsSomeoneElseLeader())
	assert.Equal(t, leaderId, multiPaxos.Leader())

	// Stale heartbeat with lower ballot number
	request2 := pb.HeartbeatRequest{
		Ballot:             staleBallot,
		LastExecuted:       1,
		GlobalLastExecuted: 0,
	}
	_, err = client.HeartbeatHandler(ctx, &request2)
	assert.Nil(t, err)
	assert.Equal(t, ballot, multiPaxos.Ballot())
	assert.Equal(t, ts, multiPaxos.LastHeartbeat())

	// The repeated heartbeat from the same leader
	_, err = client.HeartbeatHandler(ctx, &request)
	assert.Nil(t, err)
	assert.Equal(t, ballot, multiPaxos.Ballot())
	assert.True(t, multiPaxos.LastHeartbeat().After(ts))
}

func TestHeartbeatHandlerLastExecuted(t *testing.T) {
	client, ctx := initConn(t)
	ballot := MaxNumPeers - 1 + RoundIncrement
	var index1 int64 = 1
	var wg sync.WaitGroup

	_, _ = client.AcceptHandler(ctx, makeAcceptRequestByIndex(ballot, index1))

	request := pb.HeartbeatRequest{
		Ballot:             ballot,
		LastExecuted:       1,
		GlobalLastExecuted: 0,
	}
	_, _ = client.HeartbeatHandler(ctx, &request)
	assert.True(t, multiPaxos.log.Find(index1).IsCommitted())

	wg.Add(1)
	go func() {
		store_ := store.NewMemKVStore()
		multiPaxos.log.Execute(store_)
		wg.Done()
	}()
	wg.Wait()

	resp, err := client.HeartbeatHandler(ctx, &request)
	assert.Nil(t, err)
	assert.Equal(t, index1, resp.GetLastExecuted())
}