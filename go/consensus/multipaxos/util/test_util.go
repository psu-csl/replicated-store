package util

import "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"

func MakeInstance(ballot int64, index int64) *comm.Instance {
	return &comm.Instance{Ballot: ballot, Command: &comm.Command{},
		Index: index, State: comm.InstanceState_INPROGRESS, ClientId: 0}
}

func MakeInstanceWithIndexAndType(ballot int64, index int64,
	cmdType comm.CommandType) *comm.Instance {
	return &comm.Instance{Ballot: ballot, Command: &comm.Command{Type: cmdType},
		Index: index, State: comm.InstanceState_INPROGRESS, ClientId: 0}
}

func MakeInstanceWithAll(ballot int64, index int64, state comm.InstanceState,
	cmdType comm.CommandType) *comm.Instance {
	return &comm.Instance{Ballot: ballot, Command: &comm.Command{Type: cmdType},
		Index: index, State: state, ClientId: 0}
}

