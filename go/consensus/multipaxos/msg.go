package multipaxos

type ResultType int

const (
	Ok ResultType = iota
	Retry
	SomeElseLeader
	Timeout
)

type Result struct {
	Type   ResultType
	Leader int64
}
