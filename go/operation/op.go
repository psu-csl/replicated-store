package operation

type Command struct {
	CommandID int64
	Key       []byte
	Value     []byte
	Type      string
}

type CommandResult struct {
	CommandID int64
	IsSuccess bool
	Value     []byte
	Error     string
}