package latencymap

import (
	logger "github.com/sirupsen/logrus"
	"sync"
)

type LatencyMap struct {
	mu         sync.Mutex
	latencyMap map[int64]int64
}

func NewLatencyMap() *LatencyMap {
	return &LatencyMap{
		latencyMap: make(map[int64]int64),
	}
}

func (l *LatencyMap) AddTimeFromClient(rId int64, tp int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.latencyMap[rId]; !ok {
		l.latencyMap[rId] = tp
	} else {
		logger.Errorln("timepoint already exists")
	}
}

func (l *LatencyMap) AddTimeFromExecutor(rId int64, tp int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.latencyMap[rId]; !ok {
		logger.Errorln("timepoint not exists")
	} else {
		l.latencyMap[rId] = tp - l.latencyMap[rId]
	}
}

func (l *LatencyMap) GetMap() map[int64]int64 {
	return l.latencyMap
}
