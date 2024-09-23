package nsqd

import (
	"github.com/zhangweijie11/zNsq/internal/protocol"
	"io"
	"net"
	"sync"
)

const (
	typeConsumer = iota
	typeProducer
)

// Client 是一个接口，定义了客户端需要实现的方法。
// Type 方法用于返回客户端的类型。
// Stats 方法根据给定的主题返回客户端的统计信息。
type Client interface {
	Type() int
	Stats(string) ClientStats
}

// tcpServer 结构体表示一个TCP服务器，它包含一个NSQD实例和一个连接管理器。
type tcpServer struct {
	nsqd  *NSQD
	conns sync.Map
}

// Handle Handle函数处理新的TCP连接
// 该函数接收一个net.Conn作为参数，代表一个新的TCP连接
// 它首先记录连接的信息，然后读取客户端发送的协议版本
// 根据读取的协议版本，创建相应的协议对象，并将其与连接关联
// 最后，它启动一个循环来处理客户端的请求，直到连接关闭
func (p *tcpServer) Handle(conn net.Conn) {
	// 记录新连接的信息
	p.nsqd.logf(LOG_INFO, "TCP: new client(%s)", conn.RemoteAddr())

	// 创建一个缓冲区，用于读取协议版本
	buf := make([]byte, 4)

	// 试图从连接中读取完整的协议版本
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		// 如果读取失败，记录错误并关闭连接
		p.nsqd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		conn.Close()
		return
	}

	// 将读取到的字节转换为字符串，以确定协议版本
	protocolMagic := string(buf)

	// 记录客户端请求的协议版本
	p.nsqd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		conn.RemoteAddr(), protocolMagic)

	// 根据协议版本创建相应的协议对象
	var prot protocol.Protocol
	switch protocolMagic {
	case "  V2":
		prot = &protocolV2{nsqd: p.nsqd}
	default:
		// 如果协议版本不支持，发送错误响应并关闭连接
		protocol.SendFramedResponse(conn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		conn.Close()
		p.nsqd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			conn.RemoteAddr(), protocolMagic)
		return
	}

	// 创建一个新的客户端对象，并将其存储在连接映射中
	client := prot.NewClient(conn)
	p.conns.Store(conn.RemoteAddr(), client)

	// 处理客户端的请求，直到出现错误或连接关闭
	err = prot.IOLoop(client)
	if err != nil {
		// 如果出现错误，记录错误信息
		p.nsqd.logf(LOG_ERROR, "client(%s) - %s", conn.RemoteAddr(), err)
	}

	// 从连接映射中移除客户端，并关闭客户端连接
	p.conns.Delete(conn.RemoteAddr())
	client.Close()
}

// Close 方法关闭TCP服务器，并关闭所有已建立的客户端连接。
func (p *tcpServer) Close() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(protocol.Client).Close()
		return true
	})
}
