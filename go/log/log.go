package log

import (
	"github.com/golang/protobuf/proto"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	"github.com/psu-csl/replicated-store/go/store"
	"log"
	"sync"
)

type Log struct {
	mu                 sync.Mutex
	cvExecutable       *sync.Cond
	cvCommitable       *sync.Cond
	lastIndex          int64
	lastExecuted       int64
	globalLastExecuted int64
	log                map[int64]*pb.Instance
}

func NewLog() *Log {
	l := Log{
		mu:                 sync.Mutex{},
		lastIndex:          0,
		lastExecuted:       0,
		globalLastExecuted: 0,
		log:                make(map[int64]*pb.Instance),
	}
	l.cvExecutable = sync.NewCond(&l.mu)
	l.cvCommitable = sync.NewCond(&l.mu)
	return &l
}

func (l *Log) AdvanceLastIndex() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lastIndex += 1
	return l.lastIndex
}

func (l *Log) GlobalLastExecuted() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.globalLastExecuted
}

func (l *Log) LastExecuted() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.lastExecuted
}

func (l *Log) IsExecutable() bool {
	inst, ok := l.log[l.lastExecuted+ 1]
	if ok && inst.GetState() == pb.InstanceState_COMMITTED {
		return true
	}
	return false
}

func (l *Log) Append(inst *pb.Instance) {
	l.mu.Lock()
	defer l.mu.Unlock()

	i := inst.GetIndex()
	if i <= l.globalLastExecuted {
		return
	}

	if Insert(l.log, inst) {
		l.cvCommitable.Broadcast()
		if i > l.lastIndex {
			l.lastIndex = i
		}
	}
}

func Insert(insertLog map[int64]*pb.Instance, inst *pb.Instance) bool {
	// Case 1
	i := inst.GetIndex()
	if _, ok := insertLog[i]; !ok {
		insertLog[i] = inst
		return true
	}

	// Case 2
	if insertLog[i].GetState() == pb.InstanceState_COMMITTED ||
		insertLog[i].GetState() == pb.InstanceState_EXECUTED {
		if !IsEqualCommand(insertLog[i].GetCommand(), inst.GetCommand()) {
			log.Panicf("case 2 violation\n")
		}
		return false
	}

	// Case 3
	if insertLog[i].GetBallot() < inst.GetBallot() {
		insertLog[i] = inst
		return false
	}

	if insertLog[i].GetBallot() == inst.GetBallot() {
		if !IsEqualCommand(insertLog[i].GetCommand(), inst.GetCommand()) {
			log.Panicf("case 3 violation\n")
		}
	}
	return false
}

func (l *Log) Commit(index int64) {
	if index <= 0 {
		log.Panicf("Index %v < 0\n", index)
	}

	l.mu.Lock()

	_, ok := l.log[index]
	for !ok {
		l.cvCommitable.Wait()
		_, ok = l.log[index]
	}

	if l.log[index].GetState() == pb.InstanceState_INPROGRESS {
		l.log[index].State = pb.InstanceState_COMMITTED
	}
	if l.IsExecutable() {
		l.cvExecutable.Signal()
	}
	l.mu.Unlock()
}

func (l *Log) Execute(kv *store.MemKVStore) (int64, store.Result) {
	l.mu.Lock()
	for !l.IsExecutable() {
		l.cvExecutable.Wait()
	}
	inst, ok := l.log[l.lastExecuted+ 1]
	if !ok {
		log.Panicf("Instance at Index %v empty\n", l.lastExecuted+ 1)
	}
	result := kv.Execute(inst.GetCommand())
	l.log[l.lastExecuted+ 1].State = pb.InstanceState_EXECUTED
	l.lastExecuted += 1
	l.mu.Unlock()
	return 0, result
}

func (l *Log) CommitUntil(leaderLastExecuted int64, ballot int64) {
	if leaderLastExecuted <0 {
		log.Panic("invalid leader_last_executed in commit_until")
	}
	if ballot < 0 {
		log.Panic("invalid ballot in commit_until")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	for i := l.lastExecuted + 1; i <= leaderLastExecuted; i++ {
		inst, ok := l.log[i]
		if !ok {
			break
		}
		if ballot < inst.GetBallot() {
			log.Panic("CommitUntil case 2 - a smaller ballot")
		}
		if inst.GetBallot() == ballot {
			inst.State = pb.InstanceState_COMMITTED
		}
	}
	if l.IsExecutable() {
		l.cvExecutable.Signal()
	}
}

func (l *Log) TrimUntil(minTailLeader int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for l.globalLastExecuted < minTailLeader {
		l.globalLastExecuted += 1
		if l.log[l.globalLastExecuted].GetState() != pb.InstanceState_EXECUTED {
			log.Panicf("Not Executed at Index %d\n", l.globalLastExecuted)
		}
		delete(l.log, l.globalLastExecuted)
	}
}

func (l *Log) InstancesSinceGlobalLastExecuted() []*pb.Instance {
	l.mu.Lock()
	defer l.mu.Unlock()

	instances := make([]*pb.Instance, 0, len(l.log))
	for index := l.globalLastExecuted + 1; index <= l.lastIndex; index++ {
		instance := proto.Clone(l.log[index]).(*pb.Instance)
		instances = append(instances, instance)
	}
	return instances
}

// Helper functions for testing
func (l *Log) Find(index int64) *pb.Instance {
	l.mu.Lock()
	defer l.mu.Unlock()
	if inst, ok := l.log[index]; ok {
		return inst
	}
	return nil
}

func IsEqualCommand(cmd1, cmd2 *pb.Command) bool {
	return cmd1.GetType() == cmd2.GetType() && cmd1.GetKey() == cmd2.GetKey() &&
		cmd1.GetValue() == cmd2.GetValue()
}

func IsEqualInstance(a, b *pb.Instance) bool {
	return a.GetBallot() == b.GetBallot() && a.GetIndex() == b.GetIndex() &&
		a.GetClientId() == b.GetClientId() && a.GetState() == b.GetState() &&
		IsEqualCommand(a.GetCommand(), b.GetCommand())
}

func IsCommitted(instance *pb.Instance) bool {
	return instance.GetState() == pb.InstanceState_COMMITTED
}

func IsExecuted(instance *pb.Instance) bool {
	return instance.GetState() == pb.InstanceState_EXECUTED
}

func IsInProgress(instance *pb.Instance) bool {
	return instance.GetState() == pb.InstanceState_INPROGRESS
}
