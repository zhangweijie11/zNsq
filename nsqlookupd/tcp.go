package nsqlookupd

import (
	"github.com/zhangweijie11/zNsq/internal/protocol"
	"io"
	"net"
	"sync"
)

type tcpServer struct {
	nsqlookupd *NSQLookupd
	conns      sync.Map
}

// Handle Handle函数处理新的TCP连接。
// 当一个新客户端连接到服务器时，该函数被调用以处理连接初始化和协议协商。
// 参数conn代表新建立的TCP连接。
func (p *tcpServer) Handle(conn net.Conn) {
	// 记录新客户端的连接信息。
	p.nsqlookupd.logf(LOG_INFO, "TCP: new client(%s)", conn.RemoteAddr())

	// 客户端应该通过发送一个4字节的序列来初始化自己，这个序列指示了它打算使用的协议版本。
	// 这样我们可以优雅地将协议从文本/行导向升级到其他任何协议。
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		// 如果无法读取协议版本，记录错误并关闭连接。
		p.nsqlookupd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		conn.Close()
		return
	}
	// 将读取到的字节序列转换为字符串，以确定客户端请求的协议版本。
	protocolMagic := string(buf)

	// 记录客户端请求的协议版本信息。
	p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		conn.RemoteAddr(), protocolMagic)

	// 根据协议版本选择相应的协议处理方式。
	var prot protocol.Protocol
	switch protocolMagic {
	case "  V1":
		// 如果客户端请求的协议版本是"  V1"，则使用LookupProtocolV1处理。
		prot = &LookupProtocolV1{nsqlookupd: p.nsqlookupd}
	default:
		// 如果客户端请求的协议版本不受支持，发送错误响应并关闭连接。
		protocol.SendResponse(conn, []byte("E_BAD_PROTOCOL"))
		conn.Close()
		p.nsqlookupd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			conn.RemoteAddr(), protocolMagic)
		return
	}

	// 创建一个新的客户端实例，并将其添加到活动连接的映射中。
	client := prot.NewClient(conn)
	p.conns.Store(conn.RemoteAddr(), client)

	// 执行客户端的I/O循环，处理读写操作。
	err = prot.IOLoop(client)
	if err != nil {
		// 如果在处理过程中发生错误，记录错误信息。
		p.nsqlookupd.logf(LOG_ERROR, "client(%s) - %s", conn.RemoteAddr(), err)
	}

	// 从活动连接的映射中移除客户端，并关闭客户端连接。
	p.conns.Delete(conn.RemoteAddr())
	client.Close()
}

// Close 关闭TCP服务器的所有连接。
//
// 该函数遍历连接映射并关闭每个连接。它接受一个key-value对的迭代器，
// 其中value被强制转换为protocol.Client类型并调用其Close方法。
// 迭代器返回true以继续遍历所有条目。
func (p *tcpServer) Close() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(protocol.Client).Close()
		return true
	})
}
