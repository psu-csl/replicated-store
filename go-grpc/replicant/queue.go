package replicant

import (
	"sort"
	"sync"
)

type Queue struct {
	mu       sync.RWMutex
	data     []int64
	capacity int
}

func NewQueue(capacity int) *Queue {
	return &Queue{
		data:     make([]int64, 0, capacity),
		capacity: capacity,
	}
}

func (q *Queue) Append(val int64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.data) == q.capacity {
		q.data = q.data[1:]
	}
	q.data = append(q.data, val)
}

func (q *Queue) Pop() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()

	val := q.data[0]
	q.data = q.data[1:]
	return val
}

func (q *Queue) GetData() []int64 {
	return q.data
}

func (q *Queue) Stats(percentiles ...float32) []int64 {
	q.mu.RLock()
	data := make([]int64, len(q.data), len(q.data))
	copy(data, q.data)
	q.mu.RUnlock()

	sort.Slice(data, func(i, j int) bool {
		return data[i] < data[j]
	})

	length := len(percentiles)
	results := make([]int64, length, length)
	for i, p := range percentiles {
		index := int((p / 100) * float32(length))
		if index >= length {
			results[i] = data[length-1]
		} else {
			results[i] = data[index]
		}
	}
	return results
}
