package log

import (
	"github.com/psu-csl/replicated-store/go/kvstore"
	tcp "github.com/psu-csl/replicated-store/go/multipaxos/network"
	logger "github.com/sirupsen/logrus"
	"sync"
)

func IsCommitted(instance *tcp.Instance) bool {
	return instance.State == tcp.Committed
}

func IsExecuted(instance *tcp.Instance) bool {
	return instance.State == tcp.Executed
}

func IsInProgress(instance *tcp.Instance) bool {
	return instance.State == tcp.Inprogress
}

func IsEqualCommand(cmd1, cmd2 *tcp.Command) bool {
	return cmd1.Type == cmd2.Type && cmd1.Key == cmd2.Key &&
		cmd1.Value == cmd2.Value
}

func IsEqualInstance(a, b *tcp.Instance) bool {
	return a.Ballot == b.Ballot && a.Index == b.Index &&
		a.ClientId == b.ClientId && a.State == b.State &&
		IsEqualCommand(a.Command, b.Command)
}

func Insert(log map[int64]*tcp.Instance, instance *tcp.Instance) bool {
	i := instance.Index
	if _, ok := log[i]; !ok {
		log[i] = instance
		return true
	}

	if IsCommitted(log[i]) || IsExecuted(log[i]) {
		if !IsEqualCommand(log[i].Command, instance.Command) {
			logger.Panicf("case 2 violation\n")
		}
		return false
	}

	if instance.Ballot > log[i].Ballot {
		log[i] = instance
		return false
	}

	if instance.Ballot == log[i].Ballot {
		if !IsEqualCommand(log[i].Command, instance.Command) {
			logger.Panicf("case 3 violation\n")
		}
	}
	return false
}

type Log struct {
	running            bool
	kvStore            kvstore.KVStore
	log                map[int64]*tcp.Instance
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
		log:                make(map[int64]*tcp.Instance),
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

func (l *Log) Append(instance *tcp.Instance) {
	l.mu.Lock()
	defer l.mu.Unlock()

	i := instance.Index
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
		instance.State = tcp.Committed
	}
	if l.IsExecutable() {
		l.cvExecutable.Signal()
	}
}

func (l *Log) ReadInstance() *tcp.Instance {
	l.mu.Lock()
	defer l.mu.Unlock()

	for l.running && !l.IsExecutable() {
		l.cvExecutable.Wait()
	}

	if !l.running {
		return nil
	}

	instance, ok := l.log[l.lastExecuted+1]
	if !ok {
		logger.Panicf("Instance at Index %v empty\n", l.lastExecuted+1)
	}
	instance.State = tcp.Executed
	l.lastExecuted += 1
	return instance
}

func (l *Log) Execute(instance *tcp.Instance) (int64, *kvstore.KVResult) {
	result := kvstore.Execute(instance.Command, l.kvStore)
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
		if ballot < instance.Ballot {
			panic("CommitUntil case 2")
		}
		if instance.Ballot == ballot {
			instance.State = tcp.Committed
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

func (l *Log) Instances() []*tcp.Instance {
	l.mu.Lock()
	defer l.mu.Unlock()

	instances := make([]*tcp.Instance, 0, len(l.log))
	for i := l.globalLastExecuted + 1; i <= l.lastIndex; i++ {
		if i, ok := l.log[i]; ok {
			instance := *i
			instances = append(instances, &instance)
		}
	}
	return instances
}

func (l *Log) At(index int64) *tcp.Instance {
	l.mu.Lock()
	defer l.mu.Unlock()
	if instance, ok := l.log[index]; ok {
		return instance
	}
	return nil
}

func (l *Log) GetLog() map[int64]*tcp.Instance {
	l.mu.Lock()
	defer l.mu.Unlock()

	logMap := make(map[int64]*tcp.Instance)
	for index, instance := range l.log {
		copyInstance := *instance
		logMap[index] = &copyInstance
	}
	return logMap
}
