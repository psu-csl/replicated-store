package log

import (
	"github.com/psu-csl/replicated-store/go/command"
	"github.com/psu-csl/replicated-store/go/instance"
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
	log                map[int64]*instance.Instance
}

func NewLog() *Log {
	l := Log{
		mu:                 sync.Mutex{},
		lastIndex:          0,
		lastExecuted:       0,
		globalLastExecuted: 0,
		log:                make(map[int64]*instance.Instance),
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
	if ok && inst.State() == instance.Committed {
		return true
	}
	return false
}

func (l *Log) Append(inst instance.Instance) {
	l.mu.Lock()
	defer l.mu.Unlock()

	i := inst.Index()
	if i <= l.globalLastExecuted {
		return
	}

	if l.Insert(l.log, inst) {
		l.cvCommitable.Broadcast()
		if i > l.lastIndex {
			l.lastIndex = i
		}
	}
}

func (l *Log) Insert(log1 map[int64]*instance.Instance,
	inst instance.Instance) bool {
	// Case 1
	i := inst.Index()
	if _, ok := log1[i]; !ok {
		log1[i] = &inst
		return true
	}

	// Case 2
	if log1[i].State() == instance.Committed || log1[i].State() == instance.
		Executed {
		if log1[i].Command() != inst.Command() {
			log.Panicf("case 3 violation\n")
		}
		return false
	}

	// Case 3
	if log1[i].Ballot() < inst.Ballot() {
		l.log[i] = &inst
		return false
	}

	if log1[i].Ballot() == inst.Ballot() {
		if log1[i].Command() != inst.Command() {
			log.Panicf("case 4 violation\n")
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

	if l.log[index].State() == instance.InProgress {
		l.log[index].SetCommitted()
	}
	if l.IsExecutable() {
		l.cvExecutable.Signal()
	}
	l.mu.Unlock()
}

func (l *Log) Execute(kv *store.MemKVStore) (int64, command.Result) {
	l.mu.Lock()
	for !l.IsExecutable() {
		l.cvExecutable.Wait()
	}
	inst, ok := l.log[l.lastExecuted+ 1]
	if !ok {
		log.Panicf("Instance at Index %v empty\n", l.lastExecuted+ 1)
	}
	result := kv.Execute(inst.Command())
	l.log[l.lastExecuted+ 1].SetExecuted()
	l.lastExecuted += 1
	l.mu.Unlock()
	return 0, result
}

func (l *Log) CommitUntil(leaderLastExecuted int64, ballot int64) {
	if leaderLastExecuted <=0 {
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
		if ballot < inst.Ballot() {
			log.Panic("CommitUntil case 2 - a smaller ballot")
		}
		if inst.Ballot() == ballot {
			inst.SetCommitted()
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
		if l.log[l.globalLastExecuted].State() != instance.Executed {
			log.Panicf("Not Executed at Index %d\n", l.globalLastExecuted)
		}
		delete(l.log, l.globalLastExecuted)
	}
}

func (l *Log) InstancesForPrepare() []instance.Instance {
	l.mu.Lock()
	defer l.mu.Unlock()

	instances := make([]instance.Instance, 0, len(l.log))
	for index := l.globalLastExecuted + 1; index <= l.lastIndex; index++ {
		instances = append(instances, *l.log[index])
	}
	return instances
}