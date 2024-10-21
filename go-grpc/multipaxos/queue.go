package multipaxos

import (
	"gonum.org/v1/gonum/stat"
	"math"
	"sync"
)

type Queue struct {
	mu        sync.RWMutex
	data      []float64
	capacity  int64
	mean      float64
	threshold float64
	drift     float64
	posVarSum float64
	negVarSum float64
}

func NewQueue(capacity int64, threshold float64, drift float64) *Queue {
	return &Queue{
		data:      make([]float64, 0, capacity),
		capacity:  capacity,
		mean:      0,
		threshold: threshold,
		drift:     drift,
		posVarSum: 0,
		negVarSum: 0,
	}
}

func (q *Queue) AppendAndCalculate(val int64) float64 {
	q.data = append(q.data, float64(val))
	if int64(len(q.data)) == q.capacity {
		variance := stat.Variance(q.data, nil)
		q.Clear()
		return variance
	}
	return -1.0
}

func (q *Queue) PushAndUpdate(val float64) int {
	if int64(len(q.data)) == q.capacity {
		q.data = q.data[1:]
	}
	q.data = append(q.data, val)
	return q.Update(val)
}

func (q *Queue) Pop() float64 {
	val := q.data[0]
	q.data = q.data[1:]
	return val
}

func (q *Queue) GetData() []float64 {
	return q.data
}

func (q *Queue) Len() int {
	return len(q.data)
}

func (q *Queue) Clear() {
	q.data = q.data[:0]
}

func (q *Queue) Update(value float64) int {
	if len(q.data) < 4 {
		return 0
	}
	mean := stat.Mean(q.data, nil)
	deviation := value - mean

	q.posVarSum = math.Max(0, q.posVarSum+deviation-q.drift)
	q.negVarSum = math.Max(0, q.negVarSum-deviation-q.drift)

	if q.posVarSum > q.threshold || q.negVarSum > q.threshold {
		q.posVarSum, q.negVarSum = 0, 0
		if q.posVarSum > q.threshold {
			return 1
		} else {
			return -1
		}
	}
	return 0
}
