package nsqd

import (
	"encoding/binary"
	"fmt"
	"github.com/nsqio/go-nsq"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"io"
	"net"
	"time"
)

// lookupPeer 是一个结构体，用于表示查找节点的信息。
// 它包含了日志功能、网络连接、状态管理、回调函数和一些其他相关信息。
type lookupPeer struct {
	logf            lg.AppLogFunc     // logf 是一个应用日志功能，用于记录查找节点的操作日志。
	addr            string            // addr 是查找节点的网络地址。
	conn            net.Conn          // conn 是与查找节点的网络连接。
	state           int32             // state 是查找节点的当前状态，使用 int32 类型支持原子操作。
	connectCallback func(*lookupPeer) // connectCallback 是一个回调函数，在与查找节点连接成功时会被调用。
	maxBodySize     int64             // maxBodySize 是查找节点支持的最大请求体大小。
	Info            peerInfo          // Info 是关于查找节点的一些额外信息。
}

type peerInfo struct {
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
	BroadcastAddress string `json:"broadcast_address"`
}

// Connect 连接lookupPeer，并返回错误。
func (lp *lookupPeer) Connect() error {
	lp.logf(lg.INFO, "LOOKUP connecting to %s", lp.addr)
	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}
	lp.conn = conn
	return nil
}

// Write 向对等连接写入数据。
//
// 该函数首先为lp.conn设置写入超时时间，以确保不会发生死锁。
// 然后它调用lp.conn的Write方法将数据写入到对等连接。
// 它返回写入的字节数和一个错误（如果有）。
//
// 参数:
//
//	data: 要写入对等连接的数据。
//
// 返回值:
//
//	int: 成功写入的字节数。
//	error: 如果写入操作失败，则返回错误。
func (lp *lookupPeer) Write(data []byte) (int, error) {
	// 设置写入操作的超时时间为1秒。
	lp.conn.SetWriteDeadline(time.Now().Add(time.Second))

	// 执行写入操作。
	return lp.conn.Write(data)
}

// Read 从连接中读取数据，并将其存储在data切片中。
// 它设置了一个1秒的读取超时，以防止读取操作永久阻塞。
// 参数:
//
//	data: 一个字节切片，用于存储读取的数据。
//
// 返回值:
//
//	int: 成功读取的字节数。
//	error: 如果读取发生错误，则返回该错误。
func (lp *lookupPeer) Read(data []byte) (int, error) {
	// 设置读取操作的超时时间为1秒。
	lp.conn.SetReadDeadline(time.Now().Add(time.Second))
	// 执行读取操作。
	return lp.conn.Read(data)
}

// newLookupPeer 创建并返回一个新的 lookupPeer 实例。
// 该函数不进行实际的连接操作，仅用于初始化 lookupPeer 对象。
// 参数:
//
//	addr: 用于指定 peer 的地址。
//	maxBodySize: 指定允许的最大请求体大小。
//	l: 日志记录函数，用于发送日志消息。
//	connectCallback: 当 peer 成功连接时将被调用的回调函数。
//
// 返回值:
//
//	返回一个初始化的 lookupPeer 指针。
func newLookupPeer(addr string, maxBodySize int64, l lg.AppLogFunc, connectCallback func(*lookupPeer)) *lookupPeer {
	// 使用给定的参数初始化 lookupPeer 实例并返回。
	return &lookupPeer{
		logf:            l,
		addr:            addr,
		state:           stateDisconnected, // 初始状态为断开连接。
		maxBodySize:     maxBodySize,
		connectCallback: connectCallback,
	}
}

// Close 关闭 lookupPeer 的连接。
// 它将状态设置为断开连接状态，并尝试关闭底层连接（如果存在）。
func (lp *lookupPeer) Close() error {
	// 设置状态为断开连接，确保后续操作知道连接已经关闭。
	lp.state = stateDisconnected

	// 如果连接存在，则调用 Close 方法关闭连接。
	// 这是一个重要的清理步骤，以避免资源泄露。
	if lp.conn != nil {
		return lp.conn.Close()
	}

	// 如果没有建立连接，那么就没有关闭的必要，返回 nil。
	return nil
}

// Command 发送命令到 lookupPeer，并等待响应。
func (lp *lookupPeer) Command(cmd *nsq.Command) ([]byte, error) {
	// 检查并确保 lookupPeer 处于连接状态
	initialState := lp.state
	if lp.state != stateConnected {
		err := lp.Connect()
		if err != nil {
			return nil, err
		}
		lp.state = stateConnected
		_, err = lp.Write(nsq.MagicV1)
		if err != nil {
			lp.Close()
			return nil, err
		}
		// 如果之前是断开状态，则调用连接回调
		if initialState == stateDisconnected {
			lp.connectCallback(lp)
		}
		// 确保 lookupPeer 仍处于连接状态
		if lp.state != stateConnected {
			return nil, fmt.Errorf("lookupPeer connectCallback() failed")
		}
	}
	// 检查命令是否为 nil，并发送命令
	if cmd == nil {
		return nil, nil
	}
	_, err := cmd.WriteTo(lp)
	if err != nil {
		lp.Close()
		return nil, err
	}
	// 读取响应并返回
	resp, err := readResponseBounded(lp, lp.maxBodySize)
	if err != nil {
		lp.Close()
		return nil, err
	}
	return resp, nil
}

// readResponseBounded 读取一个受大小限制的响应消息。
//
// r 是一个 io.Reader，用于读取响应数据。
// limit 是响应消息大小的限制。
//
// 返回值是一个字节切片，包含读取的消息数据，
// 和一个错误值，如果在读取过程中发生错误或者消息大小超过限制，则返回错误。
//
// 该函数首先读取并解析消息的大小，然后根据大小读取具体的消息数据。
// 如果消息大小超过限制，将返回错误，避免读取过多数据。
func readResponseBounded(r io.Reader, limit int64) ([]byte, error) {
	// 声明一个 int32 类型的变量，用于存储消息的大小。
	var msgSize int32

	// 从 io.Reader 中读取消息的大小，使用大端字节序来解析。
	// 这里使用 binary.Read 一次性读取固定大小的数据，确保效率。
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		// 如果读取消息大小时发生错误，返回错误。
		return nil, err
	}

	// 检查消息大小是否超过限制。
	if int64(msgSize) > limit {
		// 如果超过限制，返回错误。
		return nil, fmt.Errorf("response body size (%d) is greater than limit (%d)", msgSize, limit)
	}

	// 创建一个大小等于消息大小的字节切片，用于存储消息数据。
	buf := make([]byte, msgSize)
	// 从 io.Reader 中读取完整的消息数据。
	// 使用 io.ReadFull 确保读取到足够数量的数据。
	_, err = io.ReadFull(r, buf)
	if err != nil {
		// 如果读取消息数据时发生错误，返回错误。
		return nil, err
	}

	// 返回包含消息数据的字节切片。
	return buf, nil
}
