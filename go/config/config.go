package config

type Config struct {
	Id                int64
	Peers             []string
	HeartbeatInterval int64
	HeartbeatDelta    int64
}