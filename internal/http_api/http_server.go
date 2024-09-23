package http_api

import (
	"fmt"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"log"
	"net"
	"net/http"
	"strings"
)

type logWriter struct {
	logf lg.AppLogFunc
}

// logWriter的Write方法用于将输入的日志信息写入到日志中。
// 该方法通过logf函数将接收到的字节切片转换为字符串格式的日志信息，并以WARN级别记录。
// 随后，该方法返回写入的日志信息的长度以及nil作为错误值，表示操作成功。
// 参数:
//
//	p []byte: 待写入日志的字节切片。
//
// 返回值:
//
//	int: 写入的日志信息的长度。
//	error: 错误值，此处始终返回nil，表示无错误。
func (l logWriter) Write(p []byte) (int, error) {
	// 使用logf函数以WARN级别记录日志信息。
	l.logf(lg.WARN, "%s", string(p))
	// 返回写入的日志信息的长度和nil作为错误值。
	return len(p), nil
}

// Serve Serve启动一个HTTP服务器并监听传入的网络连接。
// 它接受一个net.Listener用于监听网络连接，一个http.Handler用于处理HTTP请求，
// 一个字符串proto用于标识服务器类型（如"HTTP"或"HTTPS"），以及一个日志记录函数logf用于记录服务器事件。
// 如果在服务器启动或运行过程中发生无法恢复的错误，该函数将返回该错误。
// 注意：该函数不直接暴露“使用已关闭的网络连接”错误，因为这是服务器正常关闭的预期行为。
func Serve(listener net.Listener, handler http.Handler, proto string, logf lg.AppLogFunc) error {
	// 记录服务器启动信息，包括协议类型和监听的地址。
	logf(lg.INFO, "%s: listening on %s", proto, listener.Addr())

	// 初始化http.Server实例，配置请求处理程序和自定义错误日志记录器。
	// 自定义日志记录器允许将日志消息转换为应用程序日志格式或进行其他处理。
	server := &http.Server{
		Handler:  handler,
		ErrorLog: log.New(logWriter{logf}, "", 0),
	}

	// 尝试使用提供的监听器启动服务器。
	err := server.Serve(listener)
	// 检查错误，排除“使用已关闭的网络连接”错误，因为这可能是服务器正常关闭的结果。
	// 如果遇到其他错误，记录并返回错误。
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		return fmt.Errorf("http.Serve() error - %s", err)
	}

	// 服务器关闭后记录信息，表明服务器已成功关闭。
	logf(lg.INFO, "%s: closing %s", proto, listener.Addr())

	// 如果没有遇到错误，或者错误是预期的“使用已关闭的网络连接”错误，则返回nil。
	return nil
}
