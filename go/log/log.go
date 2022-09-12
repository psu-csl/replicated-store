package log

import (
	"github.com/golang/protobuf/proto"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	"github.com/psu-csl/replicated-store/go/store"
	logger "github.com/sirupsen/logrus"
	"sync"
)

func IsCommitted(instance *pb.Instance) bool {
	return instance.GetState() == pb.InstanceState_COMMITTED
}

func IsExecuted(instance *pb.Instance) bool {
	return instance.GetState() == pb.InstanceState_EXECUTED
}

func IsInProgress(instance *pb.Instance) bool {
	return instance.GetState() == pb.InstanceState_INPROGRESS
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

func Insert(insertLog map[int64]*pb.Instance, instance *pb.Instance) bool {
	// Case 1
	i := instance.GetIndex()
	if _, ok := insertLog[i]; !ok {
		insertLog[i] = instance
		return true
	}

	// Case 2
	if IsCommitted(insertLog[i]) || IsExecuted(insertLog[i]) {
		if !IsEqualCommand(insertLog[i].GetCommand(), instance.GetCommand()) {
			logger.Panicf("case 2 violation\n")
		}
		return false
	}

	// Case 3
	if instance.GetBallot() > insertLog[i].GetBallot() {
		insertLog[i] = instance
		return false
	}

	if insertLog[i].GetBallot() == instance.GetBallot() {
		if !IsEqualCommand(insertLog[i].GetCommand(), instance.GetCommand()) {
			logger.Panicf("case 3 violation\n")
		}
	}
	return false
}

type Log struct {
	running            bool
	store              store.KVStore
	log                map[int64]*pb.Instance
	lastIndex          int64
	lastExecuted       int64
	globalLastExecuted int64
	mu                 sync.Mutex
	cvExecutable       *sync.Cond
	cvCommitable       *sync.Cond
}

func NewLog(s store.KVStore) *Log {
	l := Log{
		running:            true,
		store:              s,
		log:                make(map[int64]*pb.Instance),
		lastIndex:          0,
		lastExecuted:       0,
		globalLastExecuted: 0,
		mu:                 sync.Mutex{},
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

func (l *Log) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.running = false
	l.cvExecutable.Signal()
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
		if i > l.lastIndex {
			l.lastIndex = i
		}
		l.cvCommitable.Broadcast()
	}
}

func (l *Log) Commit(index int64) {
	if index <= 0 {
		logger.Panicf("Index %v < 0\n", index)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	instance, ok := l.log[index]
	for !ok {
		l.cvCommitable.Wait()
		instance, ok = l.log[index]
	}

	if IsInProgress(instance) {
		instance.State = pb.InstanceState_COMMITTED
	}
	if l.IsExecutable() {
		l.cvExecutable.Signal()
	}
}

func (l *Log) Execute() (int64, *store.KVResult) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for l.running && !l.IsExecutable() {
		l.cvExecutable.Wait()
	}

	if !l.running {
		return -1, nil
	}

	inst, ok := l.log[l.lastExecuted+ 1]
	if !ok {
		logger.Panicf("Instance at Index %v empty\n", l.lastExecuted+ 1)
	}
	result := store.Execute(inst.GetCommand(), l.store)
	inst.State = pb.InstanceState_EXECUTED
	l.lastExecuted += 1
	return inst.ClientId, &result
}

func (l *Log) CommitUntil(leaderLastExecuted int64, ballot int64) {
	if leaderLastExecuted < 0 {
		logger.Panic("invalid leader_last_executed in commit_until")
	}
	if ballot < 0 {
		logger.Panic("invalid ballot in commit_until")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	for i := l.lastExecuted + 1; i <= leaderLastExecuted; i++ {
		inst, ok := l.log[i]
		if !ok {
			break
		}
		if ballot < inst.GetBallot() {
			panic("CommitUntil case 2")
		}
		if inst.GetBallot() == ballot {
			inst.State = pb.InstanceState_COMMITTED
		}
	}
	if l.IsExecutable() {
		l.cvExecutable.Signal()
	}
}

func (l *Log) TrimUntil(leaderGlobalLastExecuted int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for l.globalLastExecuted < leaderGlobalLastExecuted {
		l.globalLastExecuted += 1
		instance, ok := l.log[l.globalLastExecuted]
		if !ok || !IsExecuted(instance) {
			logger.Panicf("Not Executed at Index %d\n", l.globalLastExecuted)
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
		if instance != nil {
			instances = append(instances, instance)
		}
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
