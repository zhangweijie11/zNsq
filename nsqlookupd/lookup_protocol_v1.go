package nsqlookupd

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/zhangweijie11/zNsq/internal/protocol"
	"github.com/zhangweijie11/zNsq/internal/version"
)

type LookupProtocolV1 struct {
	nsqlookupd *NSQLookupd
}

func (p *LookupProtocolV1) NewClient(conn net.Conn) protocol.Client {
	return NewClientV1(conn)
}

// IOLoop 循环处理客户端的请求
func (p *LookupProtocolV1) IOLoop(c protocol.Client) error {
	var err error
	var line string

	// 将客户端断言为ClientV1类型
	client := c.(*ClientV1)

	// 创建一个带缓冲的读取器，用于从客户端读取数据
	reader := bufio.NewReader(client)

	// 无限循环，等待客户端请求
	for {
		// 读取一行客户端发送的数据，直到换行符
		line, err = reader.ReadString('\n')
		if err != nil {
			// 如果发生错误，跳出循环
			break
		}

		// 去除行首尾的空白字符
		line = strings.TrimSpace(line)
		// 根据空格分割命令行参数
		params := strings.Split(line, " ")

		// 响应初始化为nil
		var response []byte
		// 执行具体命令，可能返回响应数据和错误
		response, err = p.Exec(client, reader, params)
		if err != nil {
			// 如果有错误，处理错误
			ctx := ""
			// 判断错误是否包含父错误
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			// 记录错误日志
			p.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

			// 发送错误响应到客户端
			_, sendErr := protocol.SendResponse(client, []byte(err.Error()))
			if sendErr != nil {
				// 如果发送错误，记录日志并跳出循环
				p.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}

			// 如果错误是FatalClientErr类型，强制关闭连接
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			// 继续等待下一个请求
			continue
		}

		// 如果有响应数据，发送响应数据到客户端
		if response != nil {
			_, err = protocol.SendResponse(client, response)
			if err != nil {
				// 如果发送错误，跳出循环
				break
			}
		}
	}

	// 记录客户端退出日志
	p.nsqlookupd.logf(LOG_INFO, "PROTOCOL(V1): [%s] exiting ioloop", client)

	// 如果客户端有peer信息，处理相关注册信息
	if client.peerInfo != nil {
		// 查找该客户端的注册信息
		registrations := p.nsqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		// 遍历注册信息，移除相关生产者信息
		for _, r := range registrations {
			if removed, _ := p.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				// 记录客户端注销日志
				p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
					client, r.Category, r.Key, r.SubKey)
			}
		}
	}

	// 返回错误，结束客户端处理
	return err
}

// Exec 根据给定的参数执行相应的协议方法。
// 该函数是LookupProtocolV1接口的核心逻辑，负责解析和执行不同命令。
// 参数:
// - client: 发起请求的客户端实例。
// - reader: 用于读取客户端请求数据的缓冲读取器。
// - params: 包含命令和参数的字符串切片，命令位于params[0]。
// 返回值:
// - []byte: 执行命令后的响应数据。
// - error: 如果命令无效或执行出错，则返回错误。
func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	// 根据params[0]的值选择不同的执行路径。
	switch params[0] {
	case "PING":
		// 当命令为PING时，调用PING方法处理请求。
		return p.PING(client, params)
	case "IDENTIFY":
		// 当命令为IDENTIFY时，调用IDENTIFY方法处理请求，需要从reader读取更多信息。
		return p.IDENTIFY(client, reader, params[1:])
	case "REGISTER":
		// 当命令为REGISTER时，调用REGISTER方法处理请求，需要从reader读取注册信息。
		return p.REGISTER(client, reader, params[1:])
	case "UNREGISTER":
		// 当命令为UNREGISTER时，调用UNREGISTER方法处理请求，需要从reader读取注销信息。
		return p.UNREGISTER(client, reader, params[1:])
	}
	// 如果命令不匹配任何已知命令，则返回错误。
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

// getTopicChan 从命令和参数中提取主题名和可选的频道名。
// 该函数接收一个命令字符串和一个字符串参数数组。
// 它首先检查参数数组是否为空，如果为空，则返回一个错误，指示命令参数数量不足。
// 然后它提取主题名作为第一个参数，并根据参数数量决定是否存在频道名。
// 接着，它会验证主题名的有效性，如果主题名无效，则返回一个错误，指示主题名无效。
// 最后，如果提供了频道名，它还会验证频道名的有效性。如果频道名无效，则返回一个错误，指示频道名无效。
// 如果所有验证都通过，它将返回提取的主题名、频道名（如果存在）以及一个nil错误。
func getTopicChan(command string, params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("%s insufficient number of params", command))
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}

	if !protocol.IsValidTopicName(topicName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_TOPIC", fmt.Sprintf("%s topic name '%s' is not valid", command, topicName))
	}

	if channelName != "" && !protocol.IsValidChannelName(channelName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL", fmt.Sprintf("%s channel name '%s' is not valid", command, channelName))
	}

	return topicName, channelName, nil
}

// REGISTER REGISTER处理客户端的注册请求。
// 它尝试将客户端注册到指定的主题和频道。
// 如果客户端尚未识别，将返回错误。
// 参数：
//
//	client *ClientV1 - 已连接的客户端实例
//	reader *bufio.Reader - 用于从客户端读取数据的缓冲区读取器
//	params []string - 注册请求的参数
//
// 返回值：
//
//	[]byte - 如果注册成功，返回"OK"消息
//	error - 如果发生错误，返回相应的错误信息
func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	// 检查客户端是否已提供peerInfo，如果没有，则返回错误。
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	// 从params中提取主题和频道信息。
	topic, channel, err := getTopicChan("REGISTER", params)
	if err != nil {
		return nil, err
	}

	// 尝试将客户端注册到指定的频道。
	if channel != "" {
		key := Registration{"channel", topic, channel}
		if p.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
	}

	// 将客户端注册到主题。
	key := Registration{"topic", topic, ""}
	if p.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
		p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
	}

	// 注册成功，返回"OK"消息。
	return []byte("OK"), nil
}

// UNREGISTER 处理客户端的注销请求。
// 该函数根据提供的参数注销主题或频道的生产者信息。
// 如果客户端尚未识别自己，则返回错误。
// 如果参数中指定的是频道，则注销该频道的生产者信息；
// 如果未指定频道，则注销主题的所有生产者信息。
func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	// 检查客户端是否已识别自己
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	// 解析参数，获取主题和频道信息
	topic, channel, err := getTopicChan("UNREGISTER", params)
	if err != nil {
		return nil, err
	}

	// 如果指定了解绑频道
	if channel != "" {
		// 构建注册信息键
		key := Registration{"channel", topic, channel}
		// 从数据库中移除生产者信息
		removed, left := p.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		// 如果成功移除，则记录日志
		if removed {
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
		// 如果频道为空且为临时频道，则从数据库中移除注册信息
		if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
			p.nsqlookupd.DB.RemoveRegistration(key)
		}
	} else {
		// 如果未指定频道，则认为是解绑主题
		// 查找与主题相关的所有注册信息
		registrations := p.nsqlookupd.DB.FindRegistrations("channel", topic, "*")
		// 遍历并移除每个注册信息的生产者
		for _, r := range registrations {
			removed, _ := p.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id)
			// 如果成功移除，则记录警告日志
			if removed {
				p.nsqlookupd.logf(LOG_WARN, "client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
					client, "channel", topic, r.SubKey)
			}
		}

		// 构建主题的注册信息键
		key := Registration{"topic", topic, ""}
		// 从数据库中移除主题的生产者信息
		removed, left := p.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		// 如果成功移除，则记录日志
		if removed {
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "topic", topic, "")
		}
		// 如果主题为空且为临时主题，则从数据库中移除注册信息
		if left == 0 && strings.HasSuffix(topic, "#ephemeral") {
			p.nsqlookupd.DB.RemoveRegistration(key)
		}
	}

	// 返回成功响应
	return []byte("OK"), nil
}

// IDENTIFY 是 LookupProtocolV1 类的一个方法，用于处理客户端的 IDENTIFY 请求。
// 它从客户端读取请求的正文，解析 JSON 格式的生产者信息，并在数据库中注册生产者。
// 随后，它会生成一个包含 Lookup 服务器信息的响应，以便客户端使用。
//
// 参数:
// - client: ClientV1 类的指针，表示与客户端的连接。
// - reader: bufio.Reader 指针，用于从客户端读取数据。
// - params: 参数列表，目前未使用。
//
// 返回值:
// - []byte: 成功时返回包含服务器信息的 JSON 格式字节切片。
// - error: 如果发生错误，返回相应的错误。
func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	// 检查 client 的 peerInfo 是否已存在，如果存在则不能再次 IDENTIFY
	if client.peerInfo != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID", "cannot IDENTIFY again")
	}

	// 读取请求的正文长度
	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	// 读取请求的正文
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// 解析 JSON 格式的生产者信息
	peerInfo := PeerInfo{id: client.RemoteAddr().String()}
	err = json.Unmarshal(body, &peerInfo)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	// 更新生产者信息中的远程地址
	peerInfo.RemoteAddress = client.RemoteAddr().String()

	// 确保生产者信息中的所有必要字段都已填写
	if peerInfo.BroadcastAddress == "" || peerInfo.TCPPort == 0 || peerInfo.HTTPPort == 0 || peerInfo.Version == "" {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY", "IDENTIFY missing fields")
	}

	// 更新生产者信息的最后更新时间
	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano())

	// 记录生产者信息的日志
	p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d Version:%s",
		client, peerInfo.BroadcastAddress, peerInfo.TCPPort, peerInfo.HTTPPort, peerInfo.Version)

	// 在客户端对象中保存生产者信息
	client.peerInfo = &peerInfo

	// 在数据库中注册生产者
	if p.nsqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
		p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}

	// 构建响应数据
	data := make(map[string]interface{})
	data["tcp_port"] = p.nsqlookupd.RealTCPAddr().Port
	data["http_port"] = p.nsqlookupd.RealHTTPAddr().Port
	data["version"] = version.Binary
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err)
	}
	data["broadcast_address"] = p.nsqlookupd.opts.BroadcastAddress
	data["hostname"] = hostname

	// 将响应数据序列化为 JSON 格式
	response, err := json.Marshal(data)
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

// PING 响应 PING 命令请求。此方法主要负责更新客户端连接的状态信息，并返回一个简单的 "OK" 确认。
//
// 参数:
//   - client: 指向 ClientV1 结构体的指针，表示发送命令的客户端。
//   - params: 一个字符串切片，表示命令的具体参数，在此方法中未使用。
//
// 返回值:
//   - []byte: 一个包含 "OK" 的字节切片，表示 PING 命令的成功响应。
//   - error: 如果有错误发生，会返回 nil，表示此次操作没有错误。
func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	// 检查客户端的 peerInfo 是否存在，这可能是由于 PING 命令是此连接上的首个命令。
	if client.peerInfo != nil {
		// 获取上次更新的时间，并记录当前时间，以计算时间差。
		cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
		now := time.Now()
		// 记录此次 PING 操作的信息，包括客户端 ID 和自上次 PING 以来的时间差。
		p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): pinged (last ping %s)", client.peerInfo.id,
			now.Sub(cur))
		// 更新客户端的最后更新时间戳。
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
	}
	// 返回 "OK" 表示 PING 命令成功处理。
	return []byte("OK"), nil
}
