package protocol

import (
	"fmt"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"net"
	"runtime"
	"strings"
	"sync"
)

type TCPHandler interface {
	Handle(net.Conn)
}

// TCPServer 创建一个TCP服务器并监听传入的连接。
// 参数listener用于指定监听的网络端点。
// 参数handler用于处理接收到的TCP连接。
// 参数logf是一个日志记录函数，用于输出服务器状态信息。
// 该函数返回一个错误，表示服务器启动或运行中遇到的问题。
func TCPServer(listener net.Listener, handler TCPHandler, logf lg.AppLogFunc) error {
	// 记录TCP服务器开始监听的日志。
	logf(lg.INFO, "TCP: listening on %s", listener.Addr())

	// 使用sync.WaitGroup来等待所有处理goroutine完成。
	var wg sync.WaitGroup

	// 服务器主循环，等待并处理 incoming connections.
	for {
		// 接受一个新的客户端连接。
		clientConn, err := listener.Accept()
		if err != nil {
			// 检查错误是否是临时性的，如果是，则继续尝试接受其他连接。
			if te, ok := err.(interface{ Temporary() bool }); ok && te.Temporary() {
				// 记录临时错误并尝试继续接受连接。
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}
			// 如果错误不是因为使用了已关闭的网络连接，则返回错误。
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}

		// 为每个连接启动一个goroutine来处理它。
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler.Handle(clientConn)
		}()
	}

	// 等待所有处理goroutine完成。
	wg.Wait()

	// 记录TCP服务器关闭的日志。
	logf(lg.INFO, "TCP: closing %s", listener.Addr())

	return nil
}
