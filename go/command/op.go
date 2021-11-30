package command

type Command struct {
	Key         string
	Value       string
	CommandType string
}

type CommandResult struct {
	IsSuccess bool
	Value     string
}