package log

import (
	"github.com/psu-csl/replicated-store/go/instance"
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