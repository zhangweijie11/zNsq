package nsqd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zhangweijie11/zNsq/internal/protocol"
	"github.com/zhangweijie11/zNsq/internal/version"
	"io"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")

type protocolV2 struct {
	nsqd *NSQD
}

// NewClient 创建一个新的客户端实例。
//
// 该函数在协议版本2的上下文中，为每个新连接的客户端分配一个唯一的ID，并使用该ID以及连接对象初始化一个客户端实例。
// 客户端实例是基于nsqd的协议版本2创建的，clientIDSequence用于生成全局唯一的客户端ID。
//
// 参数:
//
//	conn - 网络连接对象，表示与客户端的TCP连接。
//
// 返回值:
//
//	protocol.Client - 实现了协议版本2的客户端实例。
func (p *protocolV2) NewClient(conn net.Conn) protocol.Client {
	clientID := atomic.AddInt64(&p.nsqd.clientIDSequence, 1)
	return newClientV2(clientID, conn, p.nsqd)
}

// IOLoop IOLoop处理与客户端的通信循环。它负责读取客户端发送的命令，
// 执行相应的处理逻辑，并将结果发送回客户端。通信循环的结束条件包括
// 读取命令失败、客户端主动断开连接或发生致命错误。
func (p *protocolV2) IOLoop(c protocol.Client) error {
	// 定义变量用于错误处理、命令读取和时间处理
	var err error
	var line []byte
	var zeroTime time.Time

	// 将客户端断言为clientV2类型
	client := c.(*clientV2)

	// 创建一个通道用于通知消息泵启动完成
	messagePumpStartedChan := make(chan bool)
	// 启动消息泵goroutine，并等待其启动完成
	go p.messagePump(client, messagePumpStartedChan)
	<-messagePumpStartedChan

	// 主通信循环
	for {
		// 根据心跳间隔设置读取操作的超时时间
		if client.HeartbeatInterval > 0 {
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2))
		} else {
			client.SetReadDeadline(zeroTime)
		}

		// 读取一行命令
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			// 处理读取错误，如果是EOF错误则转换为nil
			if err == io.EOF {
				err = nil
			} else {
				// 记录读取命令失败的错误
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}

		// 去除行尾的换行符和回车符
		line = line[:len(line)-1]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		// 根据空格分割命令参数
		params := bytes.Split(line, separatorBytes)

		// 记录协议调试信息
		p.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %s", client, params)

		// 执行命令并获取响应
		var response []byte
		response, err = p.Exec(client, params)
		if err != nil {
			// 处理执行命令过程中的错误
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.nsqd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

			// 向客户端发送错误响应
			sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
			if sendErr != nil {
				p.nsqd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}

			// 检查错误是否为致命错误，若是则退出循环
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		// 如果有响应数据，则发送响应数据给客户端
		if response != nil {
			err = p.Send(client, frameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}

	// 记录客户端通信循环退出信息
	p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] exiting ioloop", client)
	// 关闭客户端退出通道，并从所在频道移除客户端
	close(client.ExitChan)
	if client.Channel != nil {
		client.Channel.RemoveClient(client.ID)
	}

	// 返回错误，如果无错误则返回nil
	return err
}

// 消息泵
// 该函数作为V2协议的一个核心协程，负责处理客户端的消息发送逻辑。
// 它从不同的通道中选择消息，并根据客户端的状态和消息泵的配置将消息发送给客户端。
func (p *protocolV2) messagePump(client *clientV2, startedChan chan bool) {
	// 定义各种通道和变量，用于消息选择和发送
	var err error
	var memoryMsgChan chan *Message
	var backendMsgChan <-chan []byte
	var subChannel *Channel
	var flusherChan <-chan time.Time
	var sampleRate int32

	// 初始化订阅事件通道、身份验证事件通道和定时器
	subEventChan := client.SubEventChan
	identifyEventChan := client.IdentifyEventChan
	outputBufferTicker := time.NewTicker(client.OutputBufferTimeout)
	heartbeatTicker := time.NewTicker(client.HeartbeatInterval)
	heartbeatChan := heartbeatTicker.C
	msgTimeout := client.MsgTimeout

	// 初始化是否刷新标志
	flushed := true

	// 通知启动messagePump协程的协程，messagePump已启动
	close(startedChan)

	// 主循环，用于处理消息发送逻辑
	for {
		// 根据客户端是否准备接收消息和上一次是否已经刷新，来决定本次循环的行为
		if subChannel == nil || !client.IsReadyForMessages() {
			// 客户端未准备好接收消息，重置所有通道并尝试刷新
			memoryMsgChan = nil
			backendMsgChan = nil
			flusherChan = nil
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		} else if flushed {
			// 上次循环已经刷新，本次直接选择消息通道
			memoryMsgChan = subChannel.memoryMsgChan
			backendMsgChan = subChannel.backend.ReadChan()
			flusherChan = nil
		} else {
			// 已经缓冲了一些数据，选择包括刷新定时器的通道
			memoryMsgChan = subChannel.memoryMsgChan
			backendMsgChan = subChannel.backend.ReadChan()
			flusherChan = outputBufferTicker.C
		}

		// 选择从哪个通道接收消息或命令
		select {
		case <-flusherChan:
			// 强制刷新
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		case <-client.ReadyStateChan:
		case subChannel = <-subEventChan:
			// 禁止再订阅
			subEventChan = nil
		case identifyData := <-identifyEventChan:
			// 禁止再进行身份验证
			identifyEventChan = nil

			outputBufferTicker.Stop()
			if identifyData.OutputBufferTimeout > 0 {
				outputBufferTicker = time.NewTicker(identifyData.OutputBufferTimeout)
			}

			heartbeatTicker.Stop()
			heartbeatChan = nil
			if identifyData.HeartbeatInterval > 0 {
				heartbeatTicker = time.NewTicker(identifyData.HeartbeatInterval)
				heartbeatChan = heartbeatTicker.C
			}

			if identifyData.SampleRate > 0 {
				sampleRate = identifyData.SampleRate
			}

			msgTimeout = identifyData.MsgTimeout
		case <-heartbeatChan:
			err = p.Send(client, frameTypeResponse, heartbeatBytes)
			if err != nil {
				goto exit
			}
		case b := <-backendMsgChan:
			// 解码并发送消息
			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				continue
			}

			msg, err := decodeMessage(b)
			if err != nil {
				p.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
			msg.Attempts++

			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout)
			client.SendingMessage()
			err = p.SendMessage(client, msg)
			if err != nil {
				goto exit
			}
			flushed = false
		case msg := <-memoryMsgChan:
			// 从内存中选择消息并发送
			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				continue
			}
			msg.Attempts++

			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout)
			client.SendingMessage()
			err = p.SendMessage(client, msg)
			if err != nil {
				goto exit
			}
			flushed = false
		case <-client.ExitChan:
			goto exit
		}
	}

exit:
	// 清理资源并记录退出日志
	p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] exiting messagePump", client)
	heartbeatTicker.Stop()
	outputBufferTicker.Stop()
	if err != nil {
		p.nsqd.logf(LOG_ERROR, "PROTOCOL(V2): [%s] messagePump error - %s", client, err)
	}
}

// Exec 执行客户端请求的特定操作。
// 该函数根据提供的参数决定执行哪种操作。
// 参数：
//
//	client *clientV2 - 调用此方法的客户端实例。
//	params [][]byte - 包含操作所需参数的二维字节切片，第一个元素通常是操作类型。
//
// 返回值：
//
//	[]byte - 操作执行后返回的响应数据。
//	error - 如果执行过程中发生错误，返回该错误。
func (p *protocolV2) Exec(client *clientV2, params [][]byte) ([]byte, error) {
	// 检查是否是识别操作，是则调用识别方法处理。
	if bytes.Equal(params[0], []byte("IDENTIFY")) {
		return p.IDENTIFY(client, params)
	}

	// 强制执行TLS策略，确保连接符合安全策略。
	err := enforceTLSPolicy(client, p, params[0])
	if err != nil {
		return nil, err
	}

	// 根据第一个参数params[0]的值选择要执行的操作。
	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		// 处理FIN操作。
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("RDY")):
		// 处理RDY操作。
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("REQ")):
		// 处理REQ操作。
		return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		// 处理PUB操作。
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("MPUB")):
		// 处理MPUB操作。
		return p.MPUB(client, params)
	case bytes.Equal(params[0], []byte("DPUB")):
		// 处理DPUB操作。
		return p.DPUB(client, params)
	case bytes.Equal(params[0], []byte("NOP")):
		// 处理NOP操作。
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("TOUCH")):
		// 处理TOUCH操作。
		return p.TOUCH(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		// 处理SUB操作。
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("CLS")):
		// 处理CLS操作。
		return p.CLS(client, params)
	case bytes.Equal(params[0], []byte("AUTH")):
		// 处理AUTH操作。
		return p.AUTH(client, params)
	}
	// 如果操作类型不匹配任何已知类型，返回错误。
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

// IDENTIFY 处理IDENTIFY命令。
// 该命令用于客户端向服务器标识自己，包含特征协商和TLS、压缩等选项的配置。
func (p *protocolV2) IDENTIFY(client *clientV2, params [][]byte) ([]byte, error) {
	// 检查客户端是否处于初始化状态，只有在初始化状态才能进行IDENTIFY操作。
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot IDENTIFY in current state")
	}

	// 读取IDENTIFY请求的消息体长度。
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	// 检查消息体长度是否超过最大允许大小。
	if int64(bodyLen) > p.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.nsqd.getOpts().MaxBodySize))
	}

	// 检查消息体长度是否为非正数。
	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY invalid body size %d", bodyLen))
	}

	// 读取IDENTIFY请求的完整消息体。
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// 解析IDENTIFY请求的JSON消息体，包含生产者信息。
	var identifyData identifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	// 记录IDENTIFY请求的调试信息。
	p.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %+v", client, identifyData)

	// 调用客户端的Identify方法处理IDENTIFY请求的数据。
	err = client.Identify(identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	// 如果不进行特征协商，直接返回OK响应。
	if !identifyData.FeatureNegotiation {
		return okBytes, nil
	}

	// 特征协商部分，确定TLS、压缩等特性是否启用。
	tlsv1 := p.nsqd.tlsConfig != nil && identifyData.TLSv1
	deflate := p.nsqd.getOpts().DeflateEnabled && identifyData.Deflate
	deflateLevel := 6
	if deflate && identifyData.DeflateLevel > 0 {
		deflateLevel = identifyData.DeflateLevel
	}
	if max := p.nsqd.getOpts().MaxDeflateLevel; max < deflateLevel {
		deflateLevel = max
	}
	snappy := p.nsqd.getOpts().SnappyEnabled && identifyData.Snappy

	// 检查TLS和压缩特性是否冲突。
	if deflate && snappy {
		return nil, protocol.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "cannot enable both deflate and snappy compression")
	}

	// 构建IDENTIFY响应的JSON数据。
	resp, err := json.Marshal(struct {
		MaxRdyCount         int64  `json:"max_rdy_count"`
		Version             string `json:"version"`
		MaxMsgTimeout       int64  `json:"max_msg_timeout"`
		MsgTimeout          int64  `json:"msg_timeout"`
		TLSv1               bool   `json:"tls_v1"`
		Deflate             bool   `json:"deflate"`
		DeflateLevel        int    `json:"deflate_level"`
		MaxDeflateLevel     int    `json:"max_deflate_level"`
		Snappy              bool   `json:"snappy"`
		SampleRate          int32  `json:"sample_rate"`
		AuthRequired        bool   `json:"auth_required"`
		OutputBufferSize    int    `json:"output_buffer_size"`
		OutputBufferTimeout int64  `json:"output_buffer_timeout"`
	}{
		MaxRdyCount:         p.nsqd.getOpts().MaxRdyCount,
		Version:             version.Binary,
		MaxMsgTimeout:       int64(p.nsqd.getOpts().MaxMsgTimeout / time.Millisecond),
		MsgTimeout:          int64(client.MsgTimeout / time.Millisecond),
		TLSv1:               tlsv1,
		Deflate:             deflate,
		DeflateLevel:        deflateLevel,
		MaxDeflateLevel:     p.nsqd.getOpts().MaxDeflateLevel,
		Snappy:              snappy,
		SampleRate:          client.SampleRate,
		AuthRequired:        p.nsqd.IsAuthEnabled(),
		OutputBufferSize:    client.OutputBufferSize,
		OutputBufferTimeout: int64(client.OutputBufferTimeout / time.Millisecond),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	// 发送IDENTIFY响应给客户端。
	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	// 根据IDENTIFY响应的结果，可能升级客户端连接到TLS、Snappy或Deflate。
	if tlsv1 {
		p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to TLS", client)
		err = client.UpgradeTLS()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if snappy {
		p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to snappy", client)
		err = client.UpgradeSnappy()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if deflate {
		p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to deflate (level %d)", client, deflateLevel)
		err = client.UpgradeDeflate(deflateLevel)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	// IDENTIFY操作成功完成，返回nil表示无错误。
	return nil, nil
}

// Send 发送一个帧到指定的客户端。
// 该函数首先获取写锁，然后根据客户端的心跳间隔设置写入超时。
// 接着，使用协议的SendFramedResponse方法发送帧类型和数据。
// 如果发送的帧不是消息帧，则在发送后刷新客户端的写入缓冲区。
// 参数:
//
//	client - 目标客户端。
//	frameType - 要发送的帧类型。
//	data - 要发送的数据。
//
// 返回值:
//
//	错误 - 如果发送过程中发生错误，则返回该错误。
func (p *protocolV2) Send(client *clientV2, frameType int32, data []byte) error {
	// 获取写锁，以确保并发安全。
	client.writeLock.Lock()

	// 根据客户端的心跳间隔设置写入超时。
	var zeroTime time.Time
	if client.HeartbeatInterval > 0 {
		client.SetWriteDeadline(time.Now().Add(client.HeartbeatInterval))
	} else {
		client.SetWriteDeadline(zeroTime)
	}

	// 发送帧类型和数据。
	_, err := protocol.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		// 如果发送发生错误，释放写锁并返回错误。
		client.writeLock.Unlock()
		return err
	}

	// 如果发送的帧不是消息帧，则刷新客户端的写入缓冲区。
	if frameType != frameTypeMessage {
		err = client.Flush()
	}

	// 释放写锁。
	client.writeLock.Unlock()

	// 返回可能的错误。
	return err
}

// SendMessage 向指定客户端发送消息。
//
// p 是协议V2的指针，client 是客户端V2的指针，msg 是要发送的消息指针。
// 该函数没有返回结果，但可能返回错误。
//
// 该函数的主要作用是将一个消息(msg)通过协议V2发送给客户端(client)。
// 它首先记录发送消息的日志，然后从缓冲池中获取一个缓冲区用于消息的暂存，
// 使用该缓冲区写入消息内容，最后通过客户端发送出去。
// 如果在写入消息或发送过程中发生错误，该函数会返回第一个遇到的错误。
func (p *protocolV2) SendMessage(client *clientV2, msg *Message) error {
	// 记录消息发送的日志，包括消息ID，客户端信息和消息内容。
	p.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): writing msg(%s) to client(%s) - %s", msg.ID, client, msg.Body)

	// 从缓冲池中获取一个缓冲区，用于消息的写入。
	// 使用缓冲池可以减少内存分配的开销。
	buf := bufferPoolGet()
	// 确保在函数退出前，缓冲区被回收到缓冲池中。
	defer bufferPoolPut(buf)

	// 将消息写入到缓冲区中。
	// 这里使用msg的WriteTo方法来写入消息，以利用可能的消息写入优化。
	_, err := msg.WriteTo(buf)
	if err != nil {
		// 如果写入过程中发生错误，直接返回该错误。
		return err
	}

	// 通过客户端发送消息。
	// 使用协议的Send方法，将消息类型和缓冲区中的数据发送给客户端。
	err = p.Send(client, frameTypeMessage, buf.Bytes())
	if err != nil {
		// 如果发送过程中发生错误，直接返回该错误。
		return err
	}

	// 如果消息成功发送，返回nil。
	return nil
}

// readLen 从 io.Reader 中读取固定长度的字节，并将其解析为 int32 类型的数据。
// 该函数主要用于从数据流中读取固定长度的消息头，例如消息的长度前缀。
// 参数:
//
//	r: io.Reader 类型的接口，用于读取数据。
//	tmp: 字节数组，用作临时缓冲区来存储读取的数据。
//
// 返回值:
//
//	int32: 返回解析后的 int32 类型数据。
//	error: 如果读取过程中发生错误，返回错误信息。
func readLen(r io.Reader, tmp []byte) (int32, error) {
	// 使用 io.ReadFull 确保从 r 中准确读取 len(tmp) 个字节到 tmp 中。
	// 这里不关心返回的字节数（因为总与 len(tmp) 相同），但需要检查错误。
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		// 如果发生错误，则返回 0 和错误信息。
		// 注释解释了为什么这里会返回 0：因为没有成功读取和解析数据，实际的返回值对于错误情况不重要。
		return 0, err
	}
	// 将 tmp 中的字节按照 big-endian 的顺序解析为 uint32，然后转换为 int32 并返回。
	// 这里不需要错误处理，因为如果 tmp 中的字节数据不能正确解析为 uint32（例如长度不足4），前面的 io.ReadFull 已经报错。
	return int32(binary.BigEndian.Uint32(tmp)), nil
}

// enforceTLSPolicy 检查客户端是否在需要TLS连接时确实建立了TLS连接。
//
// 参数:
//
//	client - 指向客户端的指针，用于检查其TLS连接状态。
//	p - 指向协议的指针，用于访问nsqd的选项。
//	command - 当前要执行的命令，用于错误信息中指出操作。
//
// 返回值:
//
//	如果TLS连接不是活动状态且根据nsqd的配置TLS是必需的，则返回错误。
//	否则，返回nil表示操作可以继续。
//
// 该函数确保所有需要TLS连接的操作在不满足条件时及时返回错误，提高安全性。
func enforceTLSPolicy(client *clientV2, p *protocolV2, command []byte) error {
	// 检查是否需要TLS连接，以及客户端是否已经建立了TLS连接。
	// 如果需要TLS但客户端没有建立TLS连接，则返回错误。
	if p.nsqd.getOpts().TLSRequired != TLSNotRequired && atomic.LoadInt32(&client.TLS) != 1 {
		// 生成错误信息，指出在当前状态下不能执行指定命令。
		return protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("cannot %s in current state (TLS required)", command))
	}
	// 如果TLS连接状态符合要求，继续操作前不返回错误。
	return nil
}

// SUB 方法允许客户端订阅特定的主题和频道。
// 它检查客户端的状态、心跳间隔、参数数量和参数的有效性。
// 然后它验证主题和频道名称是否有效，并确保有权限进行订阅。
// 最后，它将客户端添加到指定的频道，并更新客户端状态为已订阅。
func (p *protocolV2) SUB(client *clientV2, params [][]byte) ([]byte, error) {
	// 检查客户端是否处于初始化状态，只有在初始化状态下才能进行订阅操作。
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB in current state")
	}

	// 确保心跳功能已启用，因为订阅操作依赖于心跳机制。
	if client.HeartbeatInterval <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB with heartbeats disabled")
	}

	// 检查参数数量是否满足最小要求，至少需要3个参数。
	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "SUB insufficient number of parameters")
	}

	// 解析主题名称并验证其有效性。
	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("SUB topic name %q is not valid", topicName))
	}

	// 解析频道名称并验证其有效性。
	channelName := string(params[2])
	if !protocol.IsValidChannelName(channelName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL",
			fmt.Sprintf("SUB channel name %q is not valid", channelName))
	}

	// 检查客户端是否有权限订阅指定的主题和频道。
	if err := p.CheckAuth(client, "SUB", topicName, channelName); err != nil {
		return nil, err
	}

	// 由于存在竞态条件，即最后一个客户端可能在获取频道和添加客户端之间离开，
	// 因此使用重试循环来解决这个问题。避免向已经开始退出的临时频道/主题添加客户端。
	var channel *Channel
	for i := 1; ; i++ {
		topic := p.nsqd.GetTopic(topicName)
		channel = topic.GetChannel(channelName)
		// 将客户端添加到频道，并处理可能的错误。
		if err := channel.AddClient(client.ID, client); err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_SUB_FAILED", "SUB failed "+err.Error())
		}

		// 如果频道或主题是临时的并且已经开始退出，则移除客户端并重试。
		if (channel.ephemeral && channel.Exiting()) || (topic.ephemeral && topic.Exiting()) {
			channel.RemoveClient(client.ID)
			if i < 2 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return nil, protocol.NewFatalClientErr(nil, "E_SUB_FAILED", "SUB failed to deleted topic/channel")
		}
		break
	}

	// 更新客户端状态为已订阅，并设置相应的频道。
	atomic.StoreInt32(&client.State, stateSubscribed)
	client.Channel = channel

	// 通知消息泵已订阅事件。
	client.SubEventChan <- channel

	// 返回成功订阅的响应。
	return okBytes, nil
}

// RDY 处理客户端发送的RDY命令，该命令用于通知服务器客户端准备接收的消息数量。
// client 是发送RDY命令的客户端实例，params 是命令参数，其中params[0]是命令名称，params[1]是准备接收的消息数量的字符串表示。
// 返回值是响应数据或错误。
func (p *protocolV2) RDY(client *clientV2, params [][]byte) ([]byte, error) {
	// 获取客户端当前的状态
	state := atomic.LoadInt32(&client.State)

	// 如果客户端状态为关闭中，忽略RDY命令
	if state == stateClosing {
		p.nsqd.logf(LOG_INFO,
			"PROTOCOL(V2): [%s] ignoring RDY after CLS in state ClientStateV2Closing",
			client)
		return nil, nil
	}

	// 如果客户端不在订阅状态，返回错误
	if state != stateSubscribed {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot RDY in current state")
	}

	// 默认设置客户端准备接收的消息数量为1
	count := int64(1)
	// 如果参数长度大于1，尝试解析参数中的消息数量
	if len(params) > 1 {
		b10, err := protocol.ByteToBase10(params[1])
		if err != nil {
			// 如果解析失败，返回错误
			return nil, protocol.NewFatalClientErr(err, "E_INVALID",
				fmt.Sprintf("RDY could not parse count %s", params[1]))
		}
		count = int64(b10)
	}

	// 检查消息数量是否在有效范围内
	if count < 0 || count > p.nsqd.getOpts().MaxRdyCount {
		// 如果不在范围内，返回致命错误
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("RDY count %d out of range 0-%d", count, p.nsqd.getOpts().MaxRdyCount))
	}

	// 设置客户端准备接收的消息数量
	client.SetReadyCount(count)

	// RDY命令处理成功，不返回响应数据
	return nil, nil
}

// FIN 处理客户端的FIN请求，用于结束一个消息。
// client 是客户端实例，params 是请求参数。
// 返回错误如果操作失败。
func (p *protocolV2) FIN(client *clientV2, params [][]byte) ([]byte, error) {
	// 检查客户端状态，确保客户端处于已订阅或关闭中的状态。
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot FIN in current state")
	}

	// 确保参数数量足够。
	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "FIN insufficient number of params")
	}

	// 解析消息ID。
	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	// 尝试结束消息。
	err = client.Channel.FinishMessage(client.ID, *id)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_FIN_FAILED",
			fmt.Sprintf("FIN %s failed %s", *id, err.Error()))
	}

	// 通知客户端一个消息已结束。
	client.FinishedMessage()

	return nil, nil
}

// REQ REQ处理客户端的REQ命令
// 该函数根据提供的参数重新排队消息
// 参数：
//
//	client *clientV2 - 发送REQ命令的客户端
//	params [][]byte - REQ命令的参数，包括消息ID和超时时间
//
// 返回值：
//
//	[]byte - 该函数不返回任何数据，错误时返回error类型
//	error - 如果发生错误，则返回相应的错误信息
func (p *protocolV2) REQ(client *clientV2, params [][]byte) ([]byte, error) {
	// 检查客户端状态，确保客户端已订阅或正在关闭
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot REQ in current state")
	}

	// 确保REQ命令有足够数量的参数
	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "REQ insufficient number of params")
	}

	// 解析消息ID
	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	// 解析超时时间，并将其转换为时间.Duration类型
	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("REQ could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	// 获取最大请求超时时间，并根据需要调整超时时间
	maxReqTimeout := p.nsqd.getOpts().MaxReqTimeout
	clampedTimeout := timeoutDuration
	if timeoutDuration < 0 {
		clampedTimeout = 0
	} else if timeoutDuration > maxReqTimeout {
		clampedTimeout = maxReqTimeout
	}
	if clampedTimeout != timeoutDuration {
		p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] REQ timeout %d out of range 0-%d. Setting to %d",
			client, timeoutDuration, maxReqTimeout, clampedTimeout)
		timeoutDuration = clampedTimeout
	}

	// 重新排队消息
	err = client.Channel.RequeueMessage(client.ID, *id, timeoutDuration)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_REQ_FAILED",
			fmt.Sprintf("REQ %s failed %s", *id, err.Error()))
	}

	// 更新客户端状态，表示消息已重新排队
	client.RequeuedMessage()

	return nil, nil
}

// CLS 处理客户端关闭请求
//
// 该函数应在一个已订阅的客户端上调用。当客户端的状态不是订阅状态时，
// 它会返回一个错误。函数的主要作用是启动客户端的关闭流程，并返回相应的状态码。
//
// 参数:
//
//	p: 协议V2的指针，用于调用该协议的方法。
//	client: 客户端V2的指针，表示要关闭的客户端。
//	params: 预留参数，当前未使用。
//
// 返回值:
//
//	一个字节切片，表示客户端关闭请求的响应。
//	一个错误对象，如果客户端不处于订阅状态则返回错误。
func (p *protocolV2) CLS(client *clientV2, params [][]byte) ([]byte, error) {
	// 检查客户端是否处于订阅状态
	if atomic.LoadInt32(&client.State) != stateSubscribed {
		// 如果不是订阅状态，则返回致命错误
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot CLS in current state")
	}

	// 开始客户端的关闭流程
	client.StartClose()

	// 返回表示关闭等待状态的响应
	return []byte("CLOSE_WAIT"), nil
}

// NOP 是一个处理NO OPERATION命令的函数。
// 对于协议版本2的客户端，该函数目前不执行任何操作，并返回nil作为结果。
// 这个函数主要是作为一个示例或者占位符存在。
//
// 参数:
//   - p: protocolV2的指针，表示协议版本2。
//   - client: *clientV2的指针，表示协议版本2的客户端。
//   - params: 一个二维字节切片，用于传递额外的参数。
//
// 返回值:
//   - []byte: 总是返回nil，因为当前实现中没有操作需要执行。
//   - error: 总是返回nil，表示没有错误发生。
func (p *protocolV2) NOP(client *clientV2, params [][]byte) ([]byte, error) {
	return nil, nil
}

// PUB PUB处理客户端的PUB消息请求。
// 它接收一个clientV2实例和一个二维字节切片作为参数，
// 返回一个字节切片作为响应，或一个错误。
func (p *protocolV2) PUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	// 检查参数数量是否符合要求
	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
	}

	// 解析主题名称并验证其有效性
	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("PUB topic name %q is not valid", topicName))
	}

	// 读取消息体长度并处理错误
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body size")
	}

	// 验证消息体长度是否有效
	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB invalid message body size %d", bodyLen))
	}

	// 检查消息体长度是否超过最大允许大小
	if int64(bodyLen) > p.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB message too big %d > %d", bodyLen, p.nsqd.getOpts().MaxMsgSize))
	}

	// 读取并验证消息体
	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body")
	}

	// 执行授权检查
	if err := p.CheckAuth(client, "PUB", topicName, ""); err != nil {
		return nil, err
	}

	// 获取或创建主题实例，并创建新消息
	topic := p.nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), messageBody)
	// 将消息添加到主题中，并处理错误
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_PUB_FAILED", "PUB failed "+err.Error())
	}

	// 更新客户端的已发布消息统计
	client.PublishedMessage(topicName, 1)

	// 返回成功的响应
	return okBytes, nil
}

// MPUB MPUB处理客户端的MPUB协议命令。
// MPUB协议用于批量发布消息到指定的主题。
// 参数说明：
// client - 发送请求的客户端对象。
// params - 请求的参数，以字节数组形式表示，包括主题名和消息体长度等。
// 返回值说明：
// []byte - 操作成功后的响应数据。
// error - 可能发生的错误，如果操作失败返回相应的错误信息。
func (p *protocolV2) MPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	// 检查参数数量是否满足最低需求
	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "MPUB insufficient number of parameters")
	}

	// 获取并验证主题名
	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("E_BAD_TOPIC MPUB topic name %q is not valid", topicName))
	}

	// 验证客户端权限
	if err := p.CheckAuth(client, "MPUB", topicName, ""); err != nil {
		return nil, err
	}

	// 获取主题对象
	topic := p.nsqd.GetTopic(topicName)

	// 读取并验证消息体长度
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid body size %d", bodyLen))
	}

	if int64(bodyLen) > p.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB body too big %d > %d", bodyLen, p.nsqd.getOpts().MaxBodySize))
	}

	// 读取并验证消息
	messages, err := readMPUB(client.Reader, client.lenSlice, topic,
		p.nsqd.getOpts().MaxMsgSize, p.nsqd.getOpts().MaxBodySize)
	if err != nil {
		return nil, err
	}

	// 将消息添加到主题中
	err = topic.PutMessages(messages)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_MPUB_FAILED", "MPUB failed "+err.Error())
	}

	// 记录发布的消息数量
	client.PublishedMessage(topicName, uint64(len(messages)))

	// 操作成功，返回确认响应
	return okBytes, nil
}

// DPUB 是一个处理DPUB协议请求的函数，它接收一个clientV2实例和参数列表，
// 并尝试将消息发布到指定的主题中，但消息的处理（如延迟处理）取决于参数中指定的超时时间。
func (p *protocolV2) DPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	// 检查参数数量是否满足最小需求
	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "DPUB insufficient number of parameters")
	}

	// 解析并验证主题名称
	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("DPUB topic name %q is not valid", topicName))
	}

	// 解析并验证超时时间
	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("DPUB could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	// 检查超时时间是否在允许的范围内
	if timeoutDuration < 0 || timeoutDuration > p.nsqd.getOpts().MaxReqTimeout {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("DPUB timeout %d out of range 0-%d",
				timeoutMs, p.nsqd.getOpts().MaxReqTimeout/time.Millisecond))
	}

	// 读取并验证消息体长度
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body size")
	}

	// 检查消息体长度是否有效
	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB invalid message body size %d", bodyLen))
	}

	// 再次检查消息体长度是否超过允许的最大值
	if int64(bodyLen) > p.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB message too big %d > %d", bodyLen, p.nsqd.getOpts().MaxMsgSize))
	}

	// 读取消息体
	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body")
	}

	// 验证客户端权限
	if err := p.CheckAuth(client, "DPUB", topicName, ""); err != nil {
		return nil, err
	}

	// 获取或创建主题，并创建消息
	topic := p.nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), messageBody)
	msg.deferred = timeoutDuration
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_DPUB_FAILED", "DPUB failed "+err.Error())
	}

	// 记录发布的消息
	client.PublishedMessage(topicName, 1)

	// 返回成功响应
	return okBytes, nil
}

// TOUCH TOUCH处理客户端的TOUCH操作。
// 它检查客户端的状态，确保客户端处于可以执行TOUCH操作的状态。
// 然后它验证参数数量和消息ID，准备进行TOUCH操作。
// 最后，它尝试在消息队列中TouchMessage，并返回结果或错误。
func (p *protocolV2) TOUCH(client *clientV2, params [][]byte) ([]byte, error) {
	// 检查客户端当前状态是否允许TOUCH操作。
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot TOUCH in current state")
	}

	// 确保参数数量足够进行TOUCH操作。
	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "TOUCH insufficient number of params")
	}

	// 解析消息ID，确保消息ID有效。
	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	// 获取客户端的消息超时设置，用于TouchMessage操作。
	client.writeLock.RLock()
	msgTimeout := client.MsgTimeout
	client.writeLock.RUnlock()

	// 尝试TouchMessage操作，如果失败则返回错误。
	err = client.Channel.TouchMessage(client.ID, *id, msgTimeout)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_TOUCH_FAILED",
			fmt.Sprintf("TOUCH %s failed %s", *id, err.Error()))
	}

	// TOUC操作成功，返回nil, nil表示没有需要返回的数据且没有错误。
	return nil, nil
}

// getMessageID 从字节切片创建一个MessageID对象。
// 这个函数主要用于解析字节数据并将其转换为MessageID对象。
// 参数p是一个字节切片，预期长度与MsgIDLength定义的长度相匹配。
// 如果p的长度不符合预期，函数会返回一个错误。
// 返回值包括一个指向MessageID的指针和一个可能的错误。
// 注意：这个函数直接操作内存，因此使用时需要确保p的内容符合安全和预期格式。
func getMessageID(p []byte) (*MessageID, error) {
	// 检查p的长度是否符合预期的MessageID长度
	if len(p) != MsgIDLength {
		// 如果长度不符合，返回一个错误，指示无效的消息ID
		return nil, errors.New("invalid message ID")
	}
	// 将p的起始地址转换为MessageID的指针，并返回
	// 这里使用了unsafe包，需要确保p的数据符合MessageID的布局
	return (*MessageID)(unsafe.Pointer(&p[0])), nil
}

// CheckAuth 检查客户端的认证状态是否允许其执行特定操作。
// 对于启用了认证的客户端，必须已经完成授权。
// 该函数通过比较主题/频道名称与缓存的授权数据来检查授权状态，
// 如果缓存数据过期，则会重新获取。
func (p *protocolV2) CheckAuth(client *clientV2, cmd, topicName, channelName string) error {
	// 检查是否启用了认证机制
	if client.nsqd.IsAuthEnabled() {
		// 如果客户端没有授权信息，则返回错误
		if !client.HasAuthorizations() {
			return protocol.NewFatalClientErr(nil, "E_AUTH_FIRST",
				fmt.Sprintf("AUTH required before %s", cmd))
		}
		// 检查客户端是否对给定的主题和频道有权限
		ok, err := client.IsAuthorized(topicName, channelName)
		if err != nil {
			// 记录认证失败的错误，但不向客户端泄露具体错误信息
			p.nsqd.logf(LOG_WARN, "PROTOCOL(V2): [%s] AUTH failed %s", client, err)
			return protocol.NewFatalClientErr(nil, "E_AUTH_FAILED", "AUTH failed")
		}
		// 如果客户端没有相应的权限，则返回未经授权错误
		if !ok {
			return protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED",
				fmt.Sprintf("AUTH failed for %s on %q %q", cmd, topicName, channelName))
		}
	}
	// 客户端通过认证检查，返回nil表示没有错误
	return nil
}

// readMPUB 从 io.Reader 中读取多条由 MPUB 协议编码的消息
//
// r 是用于读取数据的接口。
// tmp 是用于临时存储数据的切片，用于在读取过程中减少内存分配。
// topic 是消息的主题信息。
// maxMessageSize 是单条消息允许的最大大小。
// maxBodySize 是所有消息体允许的最大总大小。
//
// 返回值是读取到的消息切片和可能的错误。
// 如果发生错误，则返回 nil 和错误信息。
func readMPUB(r io.Reader, tmp []byte, topic *Topic, maxMessageSize int64, maxBodySize int64) ([]*Message, error) {
	// 读取消息数量
	numMessages, err := readLen(r, tmp)
	if err != nil {
		// 如果无法读取消息数量，返回错误
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read message count")
	}

	// 计算最大消息数量
	// 4 == 总数, 5 == 长度 + 最小 1
	maxMessages := (maxBodySize - 4) / 5
	if numMessages <= 0 || int64(numMessages) > maxMessages {
		// 如果消息数量无效，返回错误
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid message count %d", numMessages))
	}

	// 初始化消息切片
	messages := make([]*Message, 0, numMessages)
	for i := int32(0); i < numMessages; i++ {
		// 读取消息大小
		messageSize, err := readLen(r, tmp)
		if err != nil {
			// 如果无法读取消息大小，返回错误
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB failed to read message(%d) body size", i))
		}

		if messageSize <= 0 {
			// 如果消息大小无效，返回错误
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB invalid message(%d) body size %d", i, messageSize))
		}

		if int64(messageSize) > maxMessageSize {
			// 如果消息超过允许的最大大小，返回错误
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB message too big %d > %d", messageSize, maxMessageSize))
		}

		// 读取消息体
		msgBody := make([]byte, messageSize)
		_, err = io.ReadFull(r, msgBody)
		if err != nil {
			// 如果无法读取消息体，返回错误
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "MPUB failed to read message body")
		}

		// 创建并添加消息到切片
		messages = append(messages, NewMessage(topic.GenerateID(), msgBody))
	}

	// 返回所有读取到的消息
	return messages, nil
}

// AUTH AUTH是协议V2中处理客户端认证请求的方法。
// 它接收来自客户端的认证信息并进行验证，然后根据验证结果返回相应的响应。
func (p *protocolV2) AUTH(client *clientV2, params [][]byte) ([]byte, error) {
	// 检查客户端状态是否允许进行认证。
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot AUTH in current state")
	}

	// 确保认证参数的数量正确。
	if len(params) != 1 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH invalid number of parameters")
	}

	// 读取认证请求的消息体长度。
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body size")
	}

	// 检查消息体长度是否超过最大允许大小。
	if int64(bodyLen) > p.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH body too big %d > %d", bodyLen, p.nsqd.getOpts().MaxBodySize))
	}

	// 确认消息体长度为正数。
	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH invalid body size %d", bodyLen))
	}

	// 读取认证请求的消息体内容。
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body")
	}

	// 检查客户端是否已经设置了认证信息。
	if client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH already set")
	}

	// 确认服务器启用了认证。
	if !client.nsqd.IsAuthEnabled() {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_DISABLED", "AUTH disabled")
	}

	// 尝试对客户端进行认证。
	if err := client.Auth(string(body)); err != nil {
		p.nsqd.logf(LOG_WARN, "PROTOCOL(V2): [%s] AUTH failed %s", client, err)
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_FAILED", "AUTH failed")
	}

	// 确保客户端通过了认证。
	if !client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED", "AUTH no authorizations found")
	}

	// 构建认证成功的响应数据。
	resp, err := json.Marshal(struct {
		Identity        string `json:"identity"`
		IdentityURL     string `json:"identity_url"`
		PermissionCount int    `json:"permission_count"`
	}{
		Identity:        client.AuthState.Identity,
		IdentityURL:     client.AuthState.IdentityURL,
		PermissionCount: len(client.AuthState.Authorizations),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	// 发送认证成功响应给客户端。
	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	// 认证处理完成，无错误返回。
	return nil, nil
}
