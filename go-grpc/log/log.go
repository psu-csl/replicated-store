package log

import (
	"github.com/golang/protobuf/proto"
	"github.com/psu-csl/replicated-store/go/kvstore"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
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

func Insert(log map[int64]*pb.Instance, instance *pb.Instance) bool {
	i := instance.GetIndex()
	if _, ok := log[i]; !ok {
		log[i] = instance
		return true
	}

	if IsCommitted(log[i]) || IsExecuted(log[i]) {
		if !IsEqualCommand(log[i].GetCommand(), instance.GetCommand()) {
			logger.Panicf("case 2 violation\n")
		}
		return false
	}

	if instance.GetBallot() > log[i].GetBallot() {
		log[i] = instance
		return false
	}

	if instance.GetBallot() == log[i].GetBallot() {
		if !IsEqualCommand(log[i].GetCommand(), instance.GetCommand()) {
			logger.Panicf("case 3 violation\n")
		}
	}
	return false
}

type Log struct {
	running            bool
	kvStore            kvstore.KVStore
	log                map[int64]*pb.Instance
	lastIndex          int64
	lastExecuted       int64
	globalLastExecuted int64
	mu                 sync.Mutex
	cvExecutable       *sync.Cond
	cvCommittable      *sync.Cond
}

func NewLog(s kvstore.KVStore) *Log {
	l := Log{
		running:            true,
		kvStore:            s,
		log:                make(map[int64]*pb.Instance),
		lastIndex:          0,
		lastExecuted:       0,
		globalLastExecuted: 0,
		mu:                 sync.Mutex{},
	}
	l.cvExecutable = sync.NewCond(&l.mu)
	l.cvCommittable = sync.NewCond(&l.mu)
	return &l
}

func (l *Log) LastExecuted() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.lastExecuted
}

func (l *Log) GlobalLastExecuted() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.globalLastExecuted
}

func (l *Log) AdvanceLastIndex() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lastIndex += 1
	return l.lastIndex
}

func (l *Log) SetLastIndex(lastIndex int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lastIndex > l.lastIndex {
		l.lastIndex = lastIndex
	}
}

func (l *Log) LastIndex() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastIndex
}

func (l *Log) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.running = false
	l.kvStore.Close()
	l.cvExecutable.Signal()
}

func (l *Log) IsExecutable() bool {
	instance, ok := l.log[l.lastExecuted+1]
	if ok && IsCommitted(instance) {
		return true
	}
	return false
}

func (l *Log) Append(instance *pb.Instance) {
	l.mu.Lock()
	defer l.mu.Unlock()

	i := instance.GetIndex()
	if i <= l.globalLastExecuted {
		return
	}

	if Insert(l.log, instance) {
		if i > l.lastIndex {
			l.lastIndex = i
		}
		l.cvCommittable.Broadcast()
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
		l.cvCommittable.Wait()
		instance, ok = l.log[index]
	}

	if IsInProgress(instance) {
		instance.State = pb.InstanceState_COMMITTED
	}
	if l.IsExecutable() {
		l.cvExecutable.Signal()
	}
}

func (l *Log) Execute() (int64, *kvstore.KVResult) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for l.running && !l.IsExecutable() {
		l.cvExecutable.Wait()
	}

	if !l.running {
		return -1, nil
	}

	instance, ok := l.log[l.lastExecuted+1]
	if !ok {
		logger.Panicf("Instance at Index %v empty\n", l.lastExecuted+1)
	}
	result := kvstore.Execute(instance.GetCommand(), l.kvStore)
	instance.State = pb.InstanceState_EXECUTED
	l.lastExecuted += 1
	return instance.ClientId, &result
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
		instance, ok := l.log[i]
		if !ok {
			break
		}
		if ballot < instance.GetBallot() {
			panic("CommitUntil case 2")
		}
		if instance.GetBallot() == ballot {
			instance.State = pb.InstanceState_COMMITTED
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
			logger.Panicln("TrimUntil case 1")
		}
		delete(l.log, l.globalLastExecuted)
	}
}

func (l *Log) Instances() []*pb.Instance {
	l.mu.Lock()
	defer l.mu.Unlock()

	instances := make([]*pb.Instance, 0, len(l.log))
	for i := l.globalLastExecuted + 1; i <= l.lastIndex; i++ {
		instance := proto.Clone(l.log[i]).(*pb.Instance)
		if instance != nil {
			instances = append(instances, instance)
		}
	}
	return instances
}

func (l *Log) At(index int64) *pb.Instance {
	l.mu.Lock()
	defer l.mu.Unlock()
	if instance, ok := l.log[index]; ok {
		return instance
	}
	return nil
}

func (l *Log) GetLog() map[int64]*pb.Instance {
	l.mu.Lock()
	defer l.mu.Unlock()

	logMap := make(map[int64]*pb.Instance)
	for index, instance := range l.log {
		logMap[index] = proto.Clone(instance).(*pb.Instance)
	}
	return logMap
}
