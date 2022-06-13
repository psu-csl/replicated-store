package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	"github.com/psu-csl/replicated-store/go/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"net"
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

func initConn(ctx context.Context) (*grpc.ClientConn, error) {
	setup()
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
	return conn, err
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

func TestHeartbeatHandlerBallot(t *testing.T) {
	const (
		staleBallot  = MaxNumPeers + 1
		leaderId = MaxNumPeers - 1
	)
	ctx := context.Background()
	conn, err := initConn(ctx)
	if err != nil {
		t.Fatalf("Failed to init connection to grpc server\n")
	}
	defer conn.Close()
	client := pb.NewMultiPaxosRPCClient(conn)
	leader := NewMultipaxos(config.Config{Id: leaderId}, log.NewLog())
	ballot := leader.NextBallot()

	// Higher Ballot number
	request := pb.HeartbeatRequest{
		Ballot:             ballot,
		LastExecuted:       1,
		GlobalLastExecuted: 0,
	}
	_, err = client.HeartbeatHandler(context.Background(), &request)
	ts := multiPaxos.LastHeartbeat()
	// maybe useful for grpc testing
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
	_, err = client.HeartbeatHandler(context.Background(), &request2)
	assert.Nil(t, err)
	assert.Equal(t, ballot, multiPaxos.Ballot())
	assert.Equal(t, ts, multiPaxos.LastHeartbeat())

	// The repeated heartbeat from the same leader
	_, err = client.HeartbeatHandler(context.Background(), &request)
	assert.Nil(t, err)
	assert.Equal(t, ballot, multiPaxos.Ballot())
	assert.True(t, multiPaxos.LastHeartbeat().After(ts))
}
