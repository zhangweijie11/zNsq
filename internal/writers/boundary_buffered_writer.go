package writers

import (
	"bufio"
	"io"
)

type BoundaryBufferedWriter struct {
	bw *bufio.Writer
}

// NewBoundaryBufferedWriter 创建并返回一个新的 BoundaryBufferedWriter 实例。
// 该实例在 io.Writer 接口之上增加了缓冲功能，以提高写入性能，尤其是对于大量小写入操作。
// 参数 w 是底层写入对象，所有写入操作最终将被定向到此对象。
// 参数 size 指定了缓冲区的大小，以字节为单位。缓冲区是用于暂存写入数据的区域，
// 数据会在缓冲区满或调用 Flush 方法时被写入到底层写入对象。
// 返回值是一个指向 BoundaryBufferedWriter 类型的指针，它实现了 io.Writer 接口，
// 允许用户通过它进行写入操作，这些操作会被缓冲并最终写入到底层写入对象。
func NewBoundaryBufferedWriter(w io.Writer, size int) *BoundaryBufferedWriter {
	return &BoundaryBufferedWriter{
		bw: bufio.NewWriterSize(w, size),
	}
}

// Write 将数据写入BoundaryBufferedWriter。
// 如果待写入的数据长度超过当前缓冲区可用空间，则尝试刷新缓冲区。
// 如果刷新成功，或者待写入的数据长度未超过缓冲区可用空间，则执行写入操作。
// 返回实际写入的字节数和可能的错误。
func (b *BoundaryBufferedWriter) Write(p []byte) (int, error) {
	// 检查待写入数据长度是否超过缓冲区当前可用空间
	if len(p) > b.bw.Available() {
		// 尝试刷新缓冲区以腾出空间
		err := b.bw.Flush()
		if err != nil {
			// 如果刷新失败，返回错误
			return 0, err
		}
	}
	// 执行写入操作
	return b.bw.Write(p)
}

// Flush 刷新BoundaryBufferedWriter中的缓冲区
//
// 返回值：
//
//	可能出现的错误，如果缓冲区无法刷新则返回相应错误
//
// 说明：
//
//	本方法实际上调用了底层BufferedWriter的Flush方法来确保所有缓冲区的数据都被写入到底层的Writer
//	这在需要立即写入数据到底层Writer而不管缓冲区策略时非常有用
func (b *BoundaryBufferedWriter) Flush() error {
	return b.bw.Flush()
}
