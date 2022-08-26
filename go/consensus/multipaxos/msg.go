package multipaxos

type ResultType int

const (
	Ok ResultType = iota
	Retry
	SomeElseLeader
)

const (
	IdBits               = 0xff
	RoundIncrement       = IdBits + 1
	MaxNumPeers    int64 = 0xf
	NoLeader       int64 = -1
)

type Result struct {
	Type   ResultType
	Leader int64
}
