package nsqd

import (
	"bytes"
	"sync"
)

var bp sync.Pool

// 初始化函数，用于设置buffer池的新实例创建方法
func init() {
	// 设置bp.New，当调用时返回一个新的bytes.Buffer实例
	bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

// 获取一个buffer实例
// 返回值: *bytes.Buffer - 从buffer池中获取的buffer实例
func bufferPoolGet() *bytes.Buffer {
	return bp.Get().(*bytes.Buffer)
}

// 将buffer实例放回buffer池中
// 参数: b *bytes.Buffer - 需要放回buffer池的buffer实例
func bufferPoolPut(b *bytes.Buffer) {
	// 重置buffer，以便下次使用
	b.Reset()
	// 将buffer放回池中
	bp.Put(b)
}
