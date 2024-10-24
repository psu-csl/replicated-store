package config

import (
	"encoding/json"
	"os"
	"strconv"
)

type Config struct {
	Id             int64
	Peers          []string `json:"peers"`
	CommitInterval int64    `json:"commit_interval"`
	Store          string   `json:"store"`
	DbPath         string   `json:"db_path"`
	ElectionLimit  int64    `json:"election_limit"`
	Threshold      int64    `json:"threshold"`
	WindowSize     int64    `json:"window_size"`
	QueueSize      int64    `json:"queue_size"`
	UpThreshold    float64  `json:"up_threshold"`
	DownThreshold  float64  `json:"down_threshold"`
}

func DefaultConfig(id int64, n int) Config {
	peers := make([]string, n)
	for i := 0; i < n; i++ {
		peers[i] = "127.0.0.1:" + strconv.Itoa(10000+i*1000)
	}

	config := Config{
		Id:             id,
		CommitInterval: 3000,
		Peers:          peers,
		Store:          "memory",
	}
	return config
}

func LoadConfig(id int64, configPath string) (Config, error) {
	var config Config
	file, err := os.Open(configPath)
	if err != nil {
		return config, err
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		return config, err
	}
	config.Id = id
	return config, nil
}
