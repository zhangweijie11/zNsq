package nsqd

// dummyBackendQueue是一个模拟的后端队列结构，用于演示或测试。
// 它实现了BackendQueue接口，提供了一些基本的队列操作方法，但这些方法在该模拟结构中不执行任何实际操作。
type dummyBackendQueue struct {
	readChan chan []byte // readChan用于模拟队列的读取操作。
}

// newDummyBackendQueue创建并返回一个新的dummyBackendQueue实例。
// 该函数初始化了readChan，用于后续的队列操作。
func newDummyBackendQueue() BackendQueue {
	return &dummyBackendQueue{readChan: make(chan []byte)}
}

// Put 将数据放入队列，但在dummyBackendQueue中，该方法不执行任何操作。
// 参数:
// - data []byte: 要放入队列的数据。
// 返回值:
// - error: 总是返回nil，因为该方法不执行任何实际操作。
func (d *dummyBackendQueue) Put(data []byte) error {
	// 作为模拟方法，不执行任何操作。
	return nil
}

// ReadChan 返回队列的读取通道，用于从队列中读取数据。
// 返回值:
// - <-chan []byte: 一个只读的字节切片通道，模拟队列中的数据读取操作。
func (d *dummyBackendQueue) ReadChan() <-chan []byte {
	return d.readChan
}

// Close 关闭队列，但在dummyBackendQueue中，该方法不执行任何操作。
// 返回值:
// - error: 总是返回nil，因为该方法不执行任何实际操作。
func (d *dummyBackendQueue) Close() error {
	// 作为模拟方法，不执行任何操作。
	return nil
}

// Delete 删除队列，但在dummyBackendQueue中，该方法不执行任何操作。
// 返回值:
// - error: 总是返回nil，因为该方法不执行任何实际操作。
func (d *dummyBackendQueue) Delete() error {
	// 作为模拟方法，不执行任何操作。
	return nil
}

// Depth 返回队列的深度，但在dummyBackendQueue中，该方法总是返回0。
// 返回值:
// - int64: 队列的深度，对于dummyBackendQueue总是0。
func (d *dummyBackendQueue) Depth() int64 {
	// 作为模拟方法，假定队列深度总是0。
	return int64(0)
}

// Empty 清空队列，但在dummyBackendQueue中，该方法不执行任何操作。
// 返回值:
// - error: 总是返回nil，因为该方法不执行任何实际操作。
func (d *dummyBackendQueue) Empty() error {
	// 作为模拟方法，不执行任何操作。
	return nil
}
