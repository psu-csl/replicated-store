package config

import "strconv"

type Config struct {
	Id             int64
	Peers          []string
	CommitInterval int64
	CommitDelta    int64
}

func DefaultConfig(id int64, n int) Config {
	peers := make([]string, n)
	for i := 0; i < n; i++ {
		peers[i] = "127.0.0.1:" + strconv.Itoa(10000 + i * 1000)
	}

	config := Config{
		Id:             id,
		CommitInterval: 3000,
		CommitDelta:    10,
		Peers:          peers,
	}
	return config
}