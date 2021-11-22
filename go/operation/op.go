package operation

type Command struct {
	CommandID int64
	Key       string
	Value     string
	Type      string
}

type CommandResult struct {
	CommandID int64
	IsSuccess bool
	Value     string
	Error     string
}