package log

import (
	"bytes"
	"encoding/gob"
	"github.com/golang/protobuf/proto"
	"github.com/psu-csl/replicated-store/go/kvstore"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
	logger "github.com/sirupsen/logrus"
	"sync"
)

type Snapshot struct {
	LastIncludedIndex int64
	SnapshotData      []byte
	Ballot            int64
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
		return false
	}

	if instance.GetBallot() > log[i].GetBallot() {
		log[i] = instance
		return false
	}

	if instance.GetBallot() == log[i].GetBallot() {
		return false
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

	if index <= l.globalLastExecuted {
		return
	}

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

func (l *Log) ReadInstance() *pb.Instance {
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
	instance.State = pb.InstanceState_EXECUTED
	l.lastExecuted += 1
	return instance
}

func (l *Log) Execute(instance *pb.Instance) (int64, *kvstore.KVResult) {
	result := kvstore.Execute(instance.GetCommand(), l.kvStore)
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
			return
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

func (l *Log) InstancesRange(lastExecuted int64, lastIndex int64) []*pb.Instance {
	l.mu.Lock()
	defer l.mu.Unlock()

	instances := make([]*pb.Instance, 0, len(l.log))
	if lastIndex == -1 {
		lastIndex = l.lastIndex
	}
	for i := lastExecuted + 1; i <= lastIndex; i++ {
		instance := proto.Clone(l.log[i]).(*pb.Instance)
		if instance != nil {
			instance.State = pb.InstanceState_INPROGRESS
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

func (l *Log) MakeSnapshot(ballot int64) (*bytes.Buffer, error) {
	storeData, err := l.kvStore.MakeSnapshot()
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	snapshot := Snapshot{
		LastIncludedIndex: l.lastExecuted,
		SnapshotData:      storeData,
		Ballot:            ballot,
	}
	buffer := &bytes.Buffer{}
	err = gob.NewEncoder(buffer).Encode(&snapshot)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	logger.Infof("snapshot last index: %v\n", snapshot.LastIncludedIndex)
	return buffer, nil
}

func (l *Log) ResumeSnapshot(snapshot *Snapshot) {
	l.mu.Lock()
	if l.lastExecuted >= snapshot.LastIncludedIndex {
		l.mu.Unlock()
		return
	}

	l.lastExecuted = snapshot.LastIncludedIndex
	l.globalLastExecuted = snapshot.LastIncludedIndex
	if l.lastIndex < snapshot.LastIncludedIndex {
		l.lastIndex = snapshot.LastIncludedIndex
	}
	l.mu.Unlock()

	l.kvStore.RestoreSnapshot(snapshot.SnapshotData)
}

func (l *Log) GetIndexes() (int64, int64, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastExecuted, l.globalLastExecuted, len(l.log)
}

func (l *Log) GetLogStatus() (int, int64, int64, int64, []int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	length := len(l.log)
	indice := make([]int64, 0)
	if l.lastIndex-l.globalLastExecuted != int64(length) {
		for i := l.globalLastExecuted + 1; i <= l.lastIndex; i++ {
			if _, ok := l.log[i]; !ok {
				indice = append(indice, i)
			}
		}
	}

	list := make([]string, 0, 10)
	for i := l.lastExecuted + 1; i <= l.lastExecuted+10; i++ {
		if inst, ok := l.log[i]; !ok {
			list = append(list, "No_Instance")
		} else {
			list = append(list, inst.State.String())
		}
	}
	logger.Errorln(list)

	return length, l.lastIndex, l.lastExecuted, l.globalLastExecuted, indice
}
