package multipaxos

import (
	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	multiPaxos *Multipaxos
)

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
