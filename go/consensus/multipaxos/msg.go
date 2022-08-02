package multipaxos

type ResultType int

const (
	Ok ResultType = iota
	Retry
	SomeElseLeader
)

type Result struct {
	Type   ResultType
	Leader int64
}
