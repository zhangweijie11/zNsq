package writers

import (
	"io"
	"time"
)

type SpreadWriter struct {
	w        io.Writer
	interval time.Duration
	buf      [][]byte
	exitCh   chan int
}

// NewSpreadWriter 创建一个新的 SpreadWriter 实例。
// 该函数接收一个 io.Writer 类型的写入目标 w，一个 time.Duration 类型的时间间隔 interval，以及一个用于退出通知的通道 exitCh。
// 返回值是一个指向 SpreadWriter 的指针，它配置有给定的写入目标、时间间隔和退出通知通道。
func NewSpreadWriter(w io.Writer, interval time.Duration, exitCh chan int) *SpreadWriter {
	return &SpreadWriter{
		w:        w,                 // 设置写入目标
		interval: interval,          // 设置时间间隔
		buf:      make([][]byte, 0), // 初始化缓冲区
		exitCh:   exitCh,            // 设置退出通知通道
	}
}

// Write 将接收到的字节切片写入到内部缓冲区。
// 这个方法首先为输入的字节切片 p 创建一个副本，然后将该副本追加到内部缓冲区 s.buf。
// 这样做是为了保持 s.buf 的独立性，避免受到外部数据的直接修改。
// 参数:
//
//	p: 一个字节切片，表示需要写入的数据。
//
// 返回值:
//
//	int: 实际写入的字节数，等同于参数 p 的长度。
//	error: 当前实现中不会发生错误，所以返回 nil。
func (s *SpreadWriter) Write(p []byte) (int, error) {
	// 创建输入字节切片的副本，以避免对外部数据进行直接修改。
	b := make([]byte, len(p))
	copy(b, p)
	// 将副本追加到内部缓冲区。
	s.buf = append(s.buf, b)
	// 返回本次写入操作的字节数，此处即为输入切片 p 的长度。
	return len(p), nil
}

// Flush 方法负责将缓冲区中的数据刷新写入到底层 Writer 中。
// 它会根据预设的时间间隔和缓冲区数据量来控制写入的时机和频率。
func (s *SpreadWriter) Flush() {
	// 如果缓冲区为空，说明没有数据需要写入，只需等待一段时间或接收退出信号
	if len(s.buf) == 0 {
		select {
		case <-time.After(s.interval):
		case <-s.exitCh:
		}
		return
	}

	// 计算每个缓冲区数据块之间的等待时间，以分散写入时间
	sleep := s.interval / time.Duration(len(s.buf))
	// 创建一个定时器，用于控制写入的时机
	ticker := time.NewTicker(sleep)

	// 遍历缓冲区中的每个数据块并写入到底层 Writer 中
	for _, b := range s.buf {
		s.w.Write(b)
		// 等待直到下一个写入时间点或接收到退出信号
		select {
		case <-ticker.C:
		case <-s.exitCh: // 如果接收到退出信号，则跳过剩余的等待，完成当前的写入后停止
		}
	}

	// 停止定时器并清空缓冲区
	ticker.Stop()
	s.buf = s.buf[:0]
}
