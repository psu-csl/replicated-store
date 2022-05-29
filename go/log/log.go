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

	// Case 1
	i := inst.Index()
	if i <= l.globalLastExecuted {
		return
	}

	if inst.State() == instance.Executed {
		inst.SetCommitted()
	}

	// Case 2
	if _, ok := l.log[i]; !ok {
		if i <= l.lastExecuted {
			log.Panicf("i <= lastExecuted in append\n")
		}
		l.log[i] = &inst
		if i > l.lastIndex {
			l.lastIndex = i
		}
		l.cvCommitable.Broadcast()
		return
	}

	// Case 3
	if l.log[i].State() == instance.Committed || l.log[i].State() == instance.Executed {
		if l.log[i].Command() != inst.Command() {
			log.Panicf("case 3 violation\n")
		}
		return
	}

	// Case 4
	if l.log[i].Ballot() < inst.Ballot() {
		l.log[i] = &inst
		return
	}

	if l.log[i].Ballot() == inst.Ballot() {
		if l.log[i].Command() != inst.Command() {
			log.Panicf("case 4 violation\n")
		}
	}
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