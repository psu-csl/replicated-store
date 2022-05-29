package instance

import "github.com/psu-csl/replicated-store/go/command"

type State int

const (
	InProgress State = iota
	Committed
	Executed
)

type Instance struct {
	ballot   int64
	command  command.Command
	index    int64
	state    State
	clientId int64
}

//type Instance interface {
//	Ballot() int64
//	Command() Command
//	State() State
//	Index() int64
//	ClientId() int64
//	IsInProgress() bool
//	IsCommitted() bool
//	IsExecuted() bool
//	SetCommitted()
//	SetExecuted()
//}

func MakeInstance(ballot int64, cmd command.Command, index int64,
	state State, clientId int64) Instance {
	return Instance{ballot: ballot, command: cmd, index: index, state: state,
		clientId: clientId}
}

func (i *Instance) IsInProgress() bool {
	return i.state == InProgress
}

func (i *Instance) IsCommitted() bool {
	return i.state == Committed
}

func (i *Instance) IsExecuted() bool {
	return i.state == Executed
}

func (i *Instance) SetCommitted() {
	i.state = Committed
}

func (i *Instance) SetExecuted() {
	i.state = Executed
}

func (i *Instance) Ballot() int64 {
	return i.ballot
}

func (i *Instance) Command() command.Command {
	return i.command
}

func (i *Instance) State() State {
	return i.state
}

func (i *Instance) Index() int64 {
	return i.index
}

func (i *Instance) ClientId() int64 {
	return i.clientId
}