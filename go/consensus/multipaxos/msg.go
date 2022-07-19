package multipaxos

type ResultType int

const (
	Ok ResultType = iota
	Reject
	Retry
	Timeout
	IgnoreLeader int64 = -1
)

type AcceptResult struct {
	Type   ResultType
	Leader int64
}
