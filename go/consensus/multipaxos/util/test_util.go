package util

import "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"

func MakeInstance(ballot int64, index int64) *comm.Instance {
	return &comm.Instance{Ballot: ballot, Command: &comm.Command{},
		Index: index, State: comm.InstanceState_INPROGRESS, ClientId: 0}
}

func MakeInstanceWithState(ballot int64, index int64,
	state comm.InstanceState) *comm.Instance {
	instance := MakeInstance(ballot, index)
	instance.State = state
	return instance
}

func MakeInstanceWithType(ballot int64, index int64,
	cmdType comm.CommandType) *comm.Instance {
	instance := MakeInstance(ballot, index)
	instance.Command.Type = cmdType
	return instance
}

func MakeInstanceWithAll(ballot int64, index int64, state comm.InstanceState,
	cmdType comm.CommandType) *comm.Instance {
	instance := MakeInstance(ballot, index)
	instance.State = state
	instance.Command.Type = cmdType
	return instance
}

