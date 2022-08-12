package config

import "strconv"

type Config struct {
	Id             int64
	Peers          []string
	CommitInterval int64
	CommitDelta    int64
	ClientPorts    []string
}

func DefaultConfig(id int64, n int) Config {
	peers := make([]string, n)
	for i := 0; i < n; i++ {
		peers[i] = "127.0.0.1:" + strconv.Itoa(3000 + i)
	}
	clientPorts := make([]string, n)
	for i := 0; i < n; i++ {
		clientPorts[i] = "127.0.0.1:" + strconv.Itoa(8000 + i)
	}

	config := Config{
		Id:             id,
		CommitInterval: 300,
		CommitDelta:    10,
		Peers:          peers,
		ClientPorts:    clientPorts,
	}
	return config
}