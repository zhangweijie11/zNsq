package nsqd

type inFlightPqueue []*Message

func newInFlightPqueue(capacity int) inFlightPqueue {
	return make(inFlightPqueue, 0, capacity)
}

// Push 向优先队列中添加一个新消息。
// 该方法首先检查队列是否需要扩容，然后将消息插入队列，并调整队列保持堆属性。
// 参数:
//
//	x - 要添加到队列的消息指针。
func (pq *inFlightPqueue) Push(x *Message) {
	// 获取当前队列的长度和容量
	n := len(*pq)
	c := cap(*pq)

	// 如果添加新消息后队列长度超过容量，则进行扩容
	if n+1 > c {
		// 创建一个新的队列，容量为原队列的两倍，并将原队列内容复制到新队列
		npq := make(inFlightPqueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}

	// 将队列长度增加1，并将消息插入队列的末尾
	*pq = (*pq)[0 : n+1]
	x.index = n
	(*pq)[n] = x

	// 调整队列，确保堆属性
	pq.up(n)
}

// up 调整指定元素至合适位置，以维持优先队列的性质
// 参数:
//
//	j - 调整元素的索引
//
// 该方法不返回任何值
func (pq *inFlightPqueue) up(j int) {
	for {
		// 计算当前调整元素的父元素索引
		i := (j - 1) / 2
		// 如果当前元素优先级不高于父元素，或已达到队列顶部，则停止调整
		if i == j || (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		// 将当前元素与父元素交换，以趋向满足优先队列的性质
		pq.Swap(i, j)
		// 更新当前元素索引为其父元素索引，以便继续向上调整
		j = i
	}
}

// Swap 交换在处理中的两个元素的位置
// 该方法主要用于在优先队列中调整元素的位置
// 参数:
//
//	i, j - 要交换的两个元素的索引
//
// 通过交换元素 pq[i] 和 pq[j] 以及更新它们的索引，来实现队列中元素位置的调整
func (pq inFlightPqueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Remove 从优先队列中删除指定索引的消息，并返回该消息。
// 该方法通过交换待删除元素与队列末尾元素并调整队列结构来实现快速删除。
// 参数:
//
//	i - 要删除的消息在队列中的索引。
//
// 返回值:
//
//	被删除的消息。
func (pq *inFlightPqueue) Remove(i int) *Message {
	// 获取队列当前长度
	n := len(*pq)

	// 如果待删除元素不是队列最后一个元素
	if n-1 != i {
		// 将待删除元素与队列最后一个元素交换位置
		pq.Swap(i, n-1)
		// 从堆顶向下调整交换后的子树，以维持堆的性质
		pq.down(i, n-1)
		// 从堆底向上调整交换后的子树，以维持堆的性质
		pq.up(i)
	}
	// 获取并移除队列最后一个元素
	x := (*pq)[n-1]
	// 将移除元素的索引标记为-1，表示已移除
	x.index = -1
	// 调整队列，移除最后一个元素（即已移除的元素）
	*pq = (*pq)[0 : n-1]

	// 返回移除的消息
	return x
}

// down 是优先队列 pq 的向下调整操作，用于确保索引 i 处的元素不大于其子节点。
// 这个方法是 inFlightPqueue 类型的一部分，用于维护其堆属性。
// 参数 i 是当前需要调整的索引位置，n 是队列中元素的数量。
func (pq *inFlightPqueue) down(i, n int) {
	for {
		// 计算左子节点的索引
		j1 := 2*i + 1
		// 如果左子节点超出范围或由于整数溢出导致 j1 为负值，结束调整
		if j1 >= n || j1 < 0 {
			break
		}
		// 默认选择左子节点 j1 作为下一个潜在交换的位置
		j := j1
		// 如果右子节点存在且右子节点的优先级比左子节点高，则选择右子节点
		if j2 := j1 + 1; j2 < n && (*pq)[j1].pri >= (*pq)[j2].pri {
			j = j2 // 直接赋值给 j，指向优先级更高的右子节点
		}
		// 如果选择的子节点优先级不小于当前节点 i 的优先级，则说明调整完成，退出循环
		if (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		// 交换当前节点 i 和优先级更高的子节点 j
		pq.Swap(i, j)
		// 更新当前节点 i 为交换后的索引 j，继续向下调整
		i = j
	}
}

// Pop 从优先队列 pq 中移除并返回优先级最高的消息。
// 此方法主要用于当队列中的消息需要被处理时。
// 参数：无
// 返回值：被移除的消息，类型为 *Message
func (pq *inFlightPqueue) Pop() *Message {
	// 获取当前队列的长度和容量
	n := len(*pq)
	c := cap(*pq)

	// 将优先级最高的消息与队列中最后一个消息交换位置
	pq.Swap(0, n-1)

	// 调整堆，确保交换后的新元素位置正确
	pq.down(0, n-1)

	// 当队列长度小于容量的一半且容量大于25时，进行缩容
	if n < (c/2) && c > 25 {
		// 创建一个新的较小容量的队列
		npq := make(inFlightPqueue, n, c/2)

		// 将原队列的数据复制到新队列中
		copy(npq, *pq)

		// 更新原队列的引用，指向新的较小容量的队列
		*pq = npq
	}

	// 获取并移除原队列中的最后一个消息，即优先级最高的消息
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]

	// 返回优先级最高的消息
	return x
}

// PeekAndShift 检查队列中优先级最高的消息，并根据给定的阈值处理队列。
// 该函数首先检查队列是否为空。如果为空，则返回nil和0。
// 如果队列不为空，获取队列中的第一个消息x。
// 如果x的优先级高于给定的max，则返回nil和优先级差值x.pri - max。
// 否则，从队列中移除消息x，并返回x和0。
// 参数max用于指定优先级的阈值，返回值是一个消息指针和一个int64类型的值。
// 返回值中的int64类型值表示超过阈值的优先级差值（当队列不为空且首消息优先级高于阈值时）或0（其他情况）。
func (pq *inFlightPqueue) PeekAndShift(max int64) (*Message, int64) {
	if len(*pq) == 0 {
		return nil, 0
	}

	x := (*pq)[0]
	if x.pri > max {
		return nil, x.pri - max
	}
	pq.Pop()

	return x, 0
}
