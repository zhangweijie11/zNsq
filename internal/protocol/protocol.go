package protocol

import (
	"encoding/binary"
	"io"
	"net"
)

// Client 定义了一个可以关闭的客户端接口。
// 该接口表示能够执行客户端关闭操作。
type Client interface {
	// Close 关闭客户端连接，并可能返回一个错误。
	Close() error
}

// Protocol 定义了创建新客户端和进行I/O循环的接口。
// 该接口表示一种协议，能够创建客户端并处理其上的I/O操作。
type Protocol interface {
	// NewClient 使用给定的网络连接创建一个新的客户端实例。
	NewClient(net.Conn) Client
	// IOLoop 执行客户端上的I/O循环，可能返回一个错误。
	IOLoop(Client) error
}

// SendResponse 将数据作为响应发送到指定的写入器。
// 该函数首先将数据长度作为int32类型以大端字节序写入，然后将数据本身写入。
// 参数:
//
//	w - 响应数据的写入器。
//	data - 要发送的数据。
//
// 返回值:
//
//	写入的总字节数（包括数据长度），可能的错误。
func SendResponse(w io.Writer, data []byte) (int, error) {
	// 写入数据长度
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	// 写入数据
	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	// 返回写入的数据总长度（包括数据长度）
	return (n + 4), nil
}

// SendFramedResponse 发送带帧头的响应数据。
// 该函数在数据前后添加额外的帧头和帧类型信息，以支持特定协议的数据封装。
// 参数:
//
//	w - 响应数据的写入器。
//	frameType - 帧类型标识符。
//	data - 要发送的数据负载。
//
// 返回值:
//
//	写入的总字节数（包括帧头和帧类型），可能的错误。
func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	// 创建4字节的大端字节序缓冲区
	beBuf := make([]byte, 4)
	// 计算数据大小加上帧头大小
	size := uint32(len(data)) + 4

	// 写入帧头（数据大小+4）
	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	// 写入帧类型
	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	// 写入数据负载
	n, err = w.Write(data)
	// 返回写入的总字节数（包括帧头和帧类型）
	return n + 8, err
}
