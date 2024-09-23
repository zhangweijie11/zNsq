package pqueue

import "container/heap"

type Item struct {
	Value    interface{}
	Priority int64
	Index    int
}

// PriorityQueue 优先级队列，0 为最低优先级
type PriorityQueue []*Item

// New 初始化优先级队列
func New(capacity int) PriorityQueue {
	return make(PriorityQueue, 0, capacity)
}

// Len 返回优先队列中元素的数量。
func (pq PriorityQueue) Len() int {
	return len(pq)
}

// Less 比较优先队列中两个元素的优先级。如果第一个元素的优先级小于第二个元素，则返回true。
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

// Swap 交换优先队列中两个元素的位置，并更新它们的索引值。
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// Push 向优先队列中添加一个新元素。如果队列容量不足，则扩大队列的容量。
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		npq := make(PriorityQueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	item := x.(*Item)
	item.Index = n
	(*pq)[n] = item
}

// Pop 移除并返回优先队列中的最后一个元素。如果队列容量大于当前长度的一半，则减小队列的容量。
func (pq *PriorityQueue) Pop() interface{} {
	n := len(*pq)
	c := cap(*pq)
	if n < (c/2) && c > 25 {
		npq := make(PriorityQueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	item := (*pq)[n-1]
	item.Index = -1
	*pq = (*pq)[0 : n-1]
	return item
}

// PeekAndShift 查看并可能移除优先队列中优先级不超过给定最大值的第一个元素。
// 如果第一个元素的优先级大于最大值，则返回该元素的优先级与最大值之差。
// 如果队列为空，则返回nil和0。
func (pq *PriorityQueue) PeekAndShift(max int64) (*Item, int64) {
	if pq.Len() == 0 {
		return nil, 0
	}

	item := (*pq)[0]
	if item.Priority > max {
		return nil, item.Priority - max
	}
	heap.Remove(pq, 0)

	return item, 0
}
