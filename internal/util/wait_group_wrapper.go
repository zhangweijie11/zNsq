package util

import "sync"

type WaitGroupWrapper struct {
	sync.WaitGroup
}

// Wrap Wrap通过封装一个回调函数cb，实现对并发控制的简化。
// 该方法首先增加等待计数，然后在新的goroutine中执行回调函数，
// 并在回调函数执行完毕后减少等待计数。
// 这对于需要在并发环境中确保某些操作按顺序执行或在所有并发操作完成后执行非常有用。
func (w *WaitGroupWrapper) Wrap(cb func()) {
	// 增加等待计数，表示有一个新的并发任务将被执行。
	w.Add(1)

	// 启动一个新的goroutine，用于执行回调函数cb。
	// 这样做允许并发地执行任务，同时保持对并发数的控制。
	go func() {
		// 执行回调函数，这是封装的主要逻辑。
		cb()

		// 回调函数执行完毕后，减少等待计数。
		// 这表示已完成一个并发任务，可以继续后续的流程。
		w.Done()
	}()
}
