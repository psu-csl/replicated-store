package config

import "strconv"

type Config struct {
	Id                int64
	Peers             []string
	HeartbeatInterval int64
	HeartbeatDelta    int64
}

func DefaultConfig(id int64, n int) Config {
	peers := make([]string, n)
	for i := 0; i < n; i++ {
		peers[i] = "127.0.0.1:" + strconv.Itoa(3000 + i)
	}

	config := Config{
		Id:                id,
		HeartbeatInterval: 300,
		HeartbeatDelta:    10,
		Peers:             peers,
	}
	return config
}