package multipaxos

import (
	"gonum.org/v1/gonum/stat"
	"math"
)

type OverloadedDetector struct {
	varianceQueue *Queue
	inflightQueue *Queue
	executedQueue *Queue

	upThreshold   float64
	downThreshold float64
	drift         float64
	posVarSum     float64
	negVarSum     float64

	prevAvgInflight float64
	prevAvgExecuted float64
	numThreshold    float64
}

func NewOverloadedDetector(
	capacity int64,
	upThreshold float64,
	downThreshold float64,
	drift float64,
) *OverloadedDetector {
	return &OverloadedDetector{
		varianceQueue: NewQueue(capacity),
		inflightQueue: NewQueue(capacity),
		executedQueue: NewQueue(capacity),
		upThreshold:   upThreshold,
		downThreshold: downThreshold,
		drift:         drift,
		posVarSum:     0,
		negVarSum:     0,
		numThreshold:  0.1,
	}
}

func (od *OverloadedDetector) Push(
	variance float64,
	numInflight float64,
	numExec float64,
) (int, float64, float64) {
	od.varianceQueue.Push(variance)
	od.inflightQueue.Push(numInflight)
	od.executedQueue.Push(numExec)
	return od.Update(variance)
}

func (od *OverloadedDetector) SetVarSum(posVarSum, negVarSum float64) {
	od.posVarSum = posVarSum
	od.negVarSum = negVarSum
}

func (od *OverloadedDetector) Update(variance float64) (int, float64, float64) {
	if od.varianceQueue.Len() < 4 {
		return 0, 0, 0
	}
	inflightMean := stat.Mean(od.inflightQueue.GetData(), nil)
	executedMean := stat.Mean(od.executedQueue.GetData(), nil)

	varianceMean := stat.Mean(od.varianceQueue.GetData(), nil)
	deviation := variance - varianceMean

	od.posVarSum = math.Max(0, od.posVarSum+deviation-od.drift)
	od.negVarSum = math.Max(0, od.negVarSum-deviation-od.drift)

	flag := 0
	posVarSum := od.posVarSum
	negVarSum := od.negVarSum
	if od.posVarSum > od.upThreshold || od.negVarSum > od.downThreshold {
		if od.posVarSum > od.upThreshold {
			flag = 1
		} else {
			flag = -1
		}
		od.posVarSum, od.negVarSum = 0, 0
	} else if inflightMean > od.prevAvgInflight &&
		executedMean < od.prevAvgExecuted {
		avgInflightDiff := inflightMean - od.prevAvgInflight
		avgExecutedDiff := od.prevAvgExecuted - executedMean
		if avgInflightDiff/od.prevAvgInflight > od.numThreshold &&
			avgExecutedDiff/od.prevAvgExecuted > od.numThreshold {
			flag = 1
		}
	}
	od.prevAvgInflight = inflightMean
	od.prevAvgExecuted = executedMean
	return flag, posVarSum, negVarSum
}

func (od *OverloadedDetector) Clear() {
	od.inflightQueue.Clear()
	od.executedQueue.Clear()
	od.varianceQueue.Clear()
}
