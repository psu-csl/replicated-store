package multipaxos

import (
	"gonum.org/v1/gonum/stat"
)

type Queue struct {
	data     []float64
	capacity int64
}

func NewQueue(capacity int64) *Queue {
	return &Queue{
		data:     make([]float64, 0, capacity),
		capacity: capacity,
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

func (q *Queue) Push(val float64) {
	if int64(len(q.data)) == q.capacity {
		q.data = q.data[1:]
	}
	q.data = append(q.data, val)
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
