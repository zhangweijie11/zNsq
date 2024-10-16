package nsqd

import (
	"bufio"
	"compress/flate"
	"crypto/tls"
	"fmt"
	"github.com/golang/snappy"
	"github.com/zhangweijie11/zNsq/internal/auth"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultBufferSize = 16 * 1024

const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

// identifyDataV2 结构体定义了 V2 版本客户端识别数据的格式。
// 这些数据在客户端与服务器建立连接时用于标识和配置客户端。
type identifyDataV2 struct {
	// ClientID 是客户端的唯一标识符。
	ClientID string `json:"client_id"`
	// Hostname 是客户端的主机名。
	Hostname string `json:"hostname"`
	// HeartbeatInterval 定义了心跳间隔的时间（单位：秒）。
	HeartbeatInterval int `json:"heartbeat_interval"`
	// OutputBufferSize 定义了输出缓冲区的大小。
	OutputBufferSize int `json:"output_buffer_size"`
	// OutputBufferTimeout 定义了输出缓冲区的超时时间（单位：秒）。
	OutputBufferTimeout int `json:"output_buffer_timeout"`
	// FeatureNegotiation 表示是否支持特性协商。
	FeatureNegotiation bool `json:"feature_negotiation"`
	// TLSv1 表示是否支持 TLSv1 加密协议。
	TLSv1 bool `json:"tls_v1"`
	// Deflate 表示是否支持 Deflate 压缩。
	Deflate bool `json:"deflate"`
	// DeflateLevel 定义了 Deflate 压缩的级别。
	DeflateLevel int `json:"deflate_level"`
	// Snappy 表示是否支持 Snappy 压缩。
	Snappy bool `json:"snappy"`
	// SampleRate 定义了采样率。
	SampleRate int32 `json:"sample_rate"`
	// UserAgent 是客户端的用户代理字符串。
	UserAgent string `json:"user_agent"`
	// MsgTimeout 是消息的超时时间（单位：秒）。
	MsgTimeout int `json:"msg_timeout"`
}

// 客户端结构体V2版本
// 该结构体包含客户端的各种状态和配置信息
type clientV2 struct {
	// Conn是底层的网络连接
	net.Conn

	// ReadyCount表示客户端准备接收的消息数量
	ReadyCount int64
	// InFlightCount表示正在处理中的消息数量
	InFlightCount int64
	// MessageCount表示已接收的消息数量
	MessageCount uint64
	// FinishCount表示已成功处理的消息数量
	FinishCount uint64
	// RequeueCount表示已重新排队的消息数量
	RequeueCount uint64
	// pubCounts存储了每个主题的已发布消息数量
	pubCounts map[string]uint64

	// writeLock用于同步写操作
	writeLock sync.RWMutex
	// metaLock用于同步元数据操作
	metaLock sync.RWMutex

	// ID是客户端的唯一标识符
	ID int64
	// nsqd是当前客户端连接的NSQd实例
	nsqd *NSQD

	// UserAgent表示客户端使用的用户代理
	UserAgent string
	// tlsConn是TLS安全连接
	tlsConn *tls.Conn

	// flateWriter是用于压缩消息的flate.Writer实例
	flateWriter *flate.Writer
	// Reader是从连接读取数据的bufio.Reader实例
	Reader *bufio.Reader
	// Writer是向连接写入数据的bufio.Writer实例
	Writer *bufio.Writer

	// OutputBufferSize是输出缓冲区的大小
	OutputBufferSize int
	// OutputBufferTimeout是输出缓冲区的超时时间
	OutputBufferTimeout time.Duration
	// HeartbeatInterval是心跳间隔
	HeartbeatInterval time.Duration
	// MsgTimeout是消息处理的超时时间
	MsgTimeout time.Duration

	// State表示客户端的状态
	State int32
	// ConnectTime是客户端的连接时间
	ConnectTime time.Time

	// Channel是客户端订阅的频道
	Channel *Channel
	// ReadyStateChan是用于通知客户端就绪状态的通道
	ReadyStateChan chan int
	// ExitChan是用于通知客户端退出的通道
	ExitChan chan int

	// ClientID是客户端ID，用于标识客户端
	ClientID string
	// Hostname是客户端的主机名
	Hostname string
	// SampleRate是抽样率，用于统计
	SampleRate int32

	// IdentifyEventChan是用于处理身份验证事件的通道
	IdentifyEventChan chan identifyEvent
	// SubEventChan是用于处理订阅事件的通道
	SubEventChan chan *Channel

	// TLS表示TLS是否启用
	TLS int32
	// Snappy表示Snappy压缩是否启用
	Snappy int32
	// Deflate表示Deflate压缩是否启用
	Deflate int32

	// lenBuf是用于消息长度编码的缓冲区
	lenBuf [4]byte
	// lenSlice是lenBuf的切片表示
	lenSlice []byte

	// AuthSecret是用于身份验证的秘密
	AuthSecret string
	// AuthState是当前的身份验证状态
	AuthState *auth.State
}

// identifyEvent 用于存储客户端识别事件的相关信息
type identifyEvent struct {
	OutputBufferTimeout time.Duration // 输出缓冲区超时时间
	HeartbeatInterval   time.Duration // 心跳间隔
	SampleRate          int32         // 采样率
	MsgTimeout          time.Duration // 消息超时时间
}

// PubCount 定义了一个用于存储主题及其发布数量的结构体。
// 该结构体包含两个字段：Topic 和 Count。
// Topic 字段用于存储主题的名称，对应 JSON 数据中的 "topic" 键。
// Count 字段用于存储该主题的发布数量，对应 JSON 数据中的 "count" 键。
type PubCount struct {
	Topic string `json:"topic"` // Topic 存储主题的名称。
	Count uint64 `json:"count"` // Count 存储主题的发布数量。
}

type ClientV2Stats struct {
	ClientID                      string     `json:"client_id"`                         // 客户端ID，用于标识客户端
	Hostname                      string     `json:"hostname"`                          // 主机名
	Version                       string     `json:"version"`                           // 客户端版本
	RemoteAddress                 string     `json:"remote_address"`                    // 远程地址
	State                         int32      `json:"state"`                             // 客户端状态
	ReadyCount                    int64      `json:"ready_count"`                       // 准备就绪计数
	InFlightCount                 int64      `json:"in_flight_count"`                   // 飞行中计数
	MessageCount                  uint64     `json:"message_count"`                     // 消息计数
	FinishCount                   uint64     `json:"finish_count"`                      // 完成计数
	RequeueCount                  uint64     `json:"requeue_count"`                     // 重新排队计数
	ConnectTime                   int64      `json:"connect_ts"`                        // 连接时间戳
	SampleRate                    int32      `json:"sample_rate"`                       // 采样率
	Deflate                       bool       `json:"deflate"`                           // 是否使用Deflate压缩
	Snappy                        bool       `json:"snappy"`                            // 是否使用Snappy压缩
	UserAgent                     string     `json:"user_agent"`                        // 用户代理
	Authed                        bool       `json:"authed,omitempty"`                  // 是否已认证
	AuthIdentity                  string     `json:"auth_identity,omitempty"`           // 认证身份
	AuthIdentityURL               string     `json:"auth_identity_url,omitempty"`       // 认证身份URL
	PubCounts                     []PubCount `json:"pub_counts,omitempty"`              // 发布计数
	TLS                           bool       `json:"tls"`                               // 是否使用TLS
	CipherSuite                   string     `json:"tls_cipher_suite"`                  // 密码套件
	TLSVersion                    string     `json:"tls_version"`                       // TLS版本
	TLSNegotiatedProtocol         string     `json:"tls_negotiated_protocol"`           // TLS协商协议
	TLSNegotiatedProtocolIsMutual bool       `json:"tls_negotiated_protocol_is_mutual"` // TLS协商协议是否相互
}

// prettyConnectionState 结构体封装了 tls.ConnectionState，用于提供更丰富的连接状态信息。
// 它主要用于在需要了解 TLS 连接详细状态时，提供一个增强的表示形式。
type prettyConnectionState struct {
	tls.ConnectionState
}

// GetCipherSuite 返回当前连接使用的密码套件的字符串表示。
// 如果密码套件是已知的，则返回相应的TLS标准字符串。
// 如果密码套件未知，则返回格式化的错误信息。
func (p *prettyConnectionState) GetCipherSuite() string {
	// 根据不同的密码套件返回相应的字符串表示
	switch p.CipherSuite {
	case tls.TLS_RSA_WITH_RC4_128_SHA:
		return "TLS_RSA_WITH_RC4_128_SHA"
	case tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_RSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
	}
	// 如果密码套件未知，返回格式化的错误信息
	return fmt.Sprintf("Unknown %d", p.CipherSuite)
}

func (p *prettyConnectionState) GetVersion() string {
	switch p.Version {
	case tls.VersionTLS10:
		return "TLS1.0"
	case tls.VersionTLS11:
		return "TLS1.1"
	case tls.VersionTLS12:
		return "TLS1.2"
	case tls.VersionTLS13:
		return "TLS1.3"
	default:
		return fmt.Sprintf("Unknown %d", p.Version)
	}
}

// newClientV2 创建并初始化一个新的clientV2实例
// 参数:
//
//	id: 客户端ID
//	conn: 网络连接
//	nsqd: NSQd实例的指针
//
// 返回值:
//
//	*clientV2: 新创建的clientV2实例的指针
func newClientV2(id int64, conn net.Conn, nsqd *NSQD) *clientV2 {
	// 根据连接获取远程地址的标识符
	var identifier string
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}

	// 根据NSQD的配置和连接信息初始化clientV2实例
	c := &clientV2{
		ID:   id,
		nsqd: nsqd,

		Conn: conn,

		Reader: bufio.NewReaderSize(conn, defaultBufferSize),
		Writer: bufio.NewWriterSize(conn, defaultBufferSize),

		OutputBufferSize:    defaultBufferSize,
		OutputBufferTimeout: nsqd.getOpts().OutputBufferTimeout,

		MsgTimeout: nsqd.getOpts().MsgTimeout,

		ReadyStateChan: make(chan int, 1),
		ExitChan:       make(chan int),
		ConnectTime:    time.Now(),
		State:          stateInit,

		ClientID: identifier,
		Hostname: identifier,

		SubEventChan:      make(chan *Channel, 1),
		IdentifyEventChan: make(chan identifyEvent, 1),

		HeartbeatInterval: nsqd.getOpts().ClientTimeout / 2,

		pubCounts: make(map[string]uint64),
	}
	c.lenSlice = c.lenBuf[:]
	return c
}

// IsReadyForMessages 检查客户端是否准备好接收新消息。
// 该方法通过判断当前通道是否已暂停以及客户端的状态（通过比较准备中的消息数和正在处理中的消息数）来决定。
// 如果通道被暂停，或者正在处理中的消息数大于等于准备中的消息数，或者准备中的消息数小于等于0，则返回false。
// 否则，返回true表示客户端可以接收更多消息。
func (c *clientV2) IsReadyForMessages() bool {
	// 检查通道是否已暂停，如果暂停，则客户端不能接收新消息
	if c.Channel.IsPaused() {
		return false
	}

	// 加载准备中的消息数和正在处理中的消息数
	readyCount := atomic.LoadInt64(&c.ReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	// 记录当前的状态，用于调试目的
	c.nsqd.logf(LOG_DEBUG, "[%s] state rdy: %4d inflt: %4d", c, readyCount, inFlightCount)

	// 判断是否满足接收新消息的条件
	if inFlightCount >= readyCount || readyCount <= 0 {
		return false
	}

	return true
}

// Flush 清空当前客户端的写入缓冲区。
// 该方法首先根据心跳间隔设置写入截止时间，然后尝试刷新缓冲区。
// 如果心跳间隔大于0，则设置写入截止时间为当前时间加上心跳间隔；
// 否则，设置写入截止时间为零值。
// 调用Writer的Flush方法来清空缓冲区，如果出错则返回错误。
// 如果flateWriter不为空，调用flateWriter的Flush方法并返回其结果。
func (c *clientV2) Flush() error {
	// 定义一个零时间变量，用于设置无心跳间隔时的写入截止时间
	var zeroTime time.Time

	// 根据心跳间隔设置写入截止时间
	if c.HeartbeatInterval > 0 {
		c.SetWriteDeadline(time.Now().Add(c.HeartbeatInterval))
	} else {
		c.SetWriteDeadline(zeroTime)
	}

	// 尝试清空Writer的缓冲区
	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	// 如果flateWriter不为空，尝试清空flateWriter的缓冲区
	if c.flateWriter != nil {
		return c.flateWriter.Flush()
	}

	// 如果一切顺利，返回nil
	return nil
}

// SendingMessage 增加了在处理中的消息数量和总消息数量。
// 该函数通过原子操作更新客户端状态，确保在并发环境下消息计数的准确性。
// 没有输入参数。
// 返回值: 无
func (c *clientV2) SendingMessage() {
	// 增加在处理中的消息数量
	atomic.AddInt64(&c.InFlightCount, 1)
	// 增加总消息数量
	atomic.AddUint64(&c.MessageCount, 1)
}

// Identify 向nsqd发送客户端的识别信息，以配置和标识客户端。
// 这包括设置客户端的ID、主机名、用户代理字符串，以及调整心跳间隔、输出缓冲区大小、超时设置等。
// 参数data包含客户端在与nsqd建立连接时需要提供的所有识别信息。
// 返回可能的错误，如果有的话。
func (c *clientV2) Identify(data identifyDataV2) error {
	// 记录IDENTIFY请求，帮助调试和日志记录。
	c.nsqd.logf(LOG_INFO, "[%s] IDENTIFY: %+v", c, data)

	// 更新客户端的元数据，包括客户端ID、主机名和用户代理。
	c.metaLock.Lock()
	c.ClientID = data.ClientID
	c.Hostname = data.Hostname
	c.UserAgent = data.UserAgent
	c.metaLock.Unlock()

	// 设置心跳间隔，确保客户端和nsqd之间的连接保持活跃。
	err := c.SetHeartbeatInterval(data.HeartbeatInterval)
	if err != nil {
		return err
	}

	// 配置输出缓冲区，控制消息发送前的最大等待时间和最大消息数。
	err = c.SetOutputBuffer(data.OutputBufferSize, data.OutputBufferTimeout)
	if err != nil {
		return err
	}

	// 设置样本率，用于控制消息采样，可能用于调试或减少消息流量。
	err = c.SetSampleRate(data.SampleRate)
	if err != nil {
		return err
	}

	// 设置消息超时，控制消息在未被确认前的最大存活时间。
	err = c.SetMsgTimeout(data.MsgTimeout)
	if err != nil {
		return err
	}

	// 构建识别事件，包含配置的连接参数。
	ie := identifyEvent{
		OutputBufferTimeout: c.OutputBufferTimeout,
		HeartbeatInterval:   c.HeartbeatInterval,
		SampleRate:          c.SampleRate,
		MsgTimeout:          c.MsgTimeout,
	}

	// 尝试更新客户端的消息泵，发送识别事件。
	// 使用select的默认分支来避免阻塞。
	select {
	case c.IdentifyEventChan <- ie:
	default:
	}

	// 如果所有操作都成功，返回nil。
	return nil
}

// SetHeartbeatInterval 设置客户端的心跳间隔。
// desiredInterval 参数表示期望的心跳间隔，单位为毫秒。
// 返回错误如果期望的心跳间隔无效。
func (c *clientV2) SetHeartbeatInterval(desiredInterval int) error {
	// 获取写锁以确保并发安全
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	// 根据desiredInterval的值进行不同的操作
	switch {
	case desiredInterval == -1:
		// 如果desiredInterval为-1，重置心跳间隔为0
		c.HeartbeatInterval = 0
	case desiredInterval == 0:
		// 如果desiredInterval为0，不进行任何操作，使用默认心跳间隔
		// do nothing (use default)
	case desiredInterval >= 1000 &&
		desiredInterval <= int(c.nsqd.getOpts().MaxHeartbeatInterval/time.Millisecond):
		// 如果desiredInterval在有效范围内，更新心跳间隔
		c.HeartbeatInterval = time.Duration(desiredInterval) * time.Millisecond
	default:
		// 如果desiredInterval无效，返回错误
		return fmt.Errorf("heartbeat interval (%d) is invalid", desiredInterval)
	}

	// 如果没有无效情况发生，返回nil（没有错误）
	return nil
}

// SetOutputBuffer 设置客户端的输出缓冲区大小和超时时间。
// desiredSize 表示期望的缓冲区大小。
// desiredTimeout 表示期望的超时时间。
// 该函数通过锁定确保线程安全，在修改输出缓冲区和超时设置前会获取写锁。
func (c *clientV2) SetOutputBuffer(desiredSize int, desiredTimeout int) error {
	// 获取写锁
	c.writeLock.Lock()
	// 确保解锁，即使函数因错误提前返回
	defer c.writeLock.Unlock()

	// 设置输出缓冲区超时时间
	switch {
	case desiredTimeout == -1:
		// 如果请求的超时时间为-1，则设置为无超时（0）
		c.OutputBufferTimeout = 0
	case desiredTimeout == 0:
		// 如果请求的超时时间为0，则不进行任何操作（使用默认值）
		// do nothing (use default)
	case true &&
		desiredTimeout >= int(c.nsqd.getOpts().MinOutputBufferTimeout/time.Millisecond) &&
		desiredTimeout <= int(c.nsqd.getOpts().MaxOutputBufferTimeout/time.Millisecond):

		// 如果请求的超时时间在最小和最大值之间，则设置超时时间
		c.OutputBufferTimeout = time.Duration(desiredTimeout) * time.Millisecond
	default:
		// 如果请求的超时时间不合法，则返回错误
		return fmt.Errorf("output buffer timeout (%d) is invalid", desiredTimeout)
	}

	// 设置输出缓冲区大小
	switch {
	case desiredSize == -1:
		// 如果请求的缓冲区大小为-1，表示实际上不使用缓冲区（每次写入都会直接到网络连接）
		c.OutputBufferSize = 1
		c.OutputBufferTimeout = 0
	case desiredSize == 0:
		// 如果请求的缓冲区大小为0，则不进行任何操作（使用默认值）
		// do nothing (use default)
	case desiredSize >= 64 && desiredSize <= int(c.nsqd.getOpts().MaxOutputBufferSize):
		// 如果请求的缓冲区大小在允许的范围内，则设置缓冲区大小
		c.OutputBufferSize = desiredSize
	default:
		// 如果请求的缓冲区大小不合法，则返回错误
		return fmt.Errorf("output buffer size (%d) is invalid", desiredSize)
	}

	// 如果请求的缓冲区大小不为0，则重新初始化缓冲区写入器
	if desiredSize != 0 {
		// 尝试刷新当前的缓冲区写入器
		err := c.Writer.Flush()
		if err != nil {
			// 如果刷新失败，则返回错误
			return err
		}
		// 重新初始化缓冲区写入器
		c.Writer = bufio.NewWriterSize(c.Conn, c.OutputBufferSize)
	}

	// 操作成功，返回nil
	return nil
}

// SetSampleRate 设置采样率。
//
// sampleRate: 要设置的采样率，范围为0到99。
//
// 错误返回：如果给定的采样率超出了有效范围。
func (c *clientV2) SetSampleRate(sampleRate int32) error {
	// 检查采样率是否在有效范围内，不在则返回错误。
	if sampleRate < 0 || sampleRate > 99 {
		return fmt.Errorf("sample rate (%d) is invalid", sampleRate)
	}
	// 使用原子操作更新采样率，确保线程安全。
	atomic.StoreInt32(&c.SampleRate, sampleRate)
	return nil
}

// SetMsgTimeout 设置消息超时时间，以毫秒为单位。
// 该函数接收一个整数参数msgTimeout，用于指定消息在队列中等待的最大时间。
// 如果msgTimeout为0，则不进行任何操作，继续使用默认超时时间。
// msgTimeout的有效范围是从1000到当前配置的MaxMsgTimeout之间的毫秒数。
// 如果msgTimeout不在有效范围内，函数将返回一个错误。
// 参数:
//
//	msgTimeout - 消息超时时间（毫秒）
//
// 返回值:
//
//	error - 如果msgTimeout无效，返回错误信息
func (c *clientV2) SetMsgTimeout(msgTimeout int) error {
	// 获取写锁以确保并发安全
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	// 根据msgTimeout的值进行条件判断
	switch {
	// 如果msgTimeout为0，不改变当前MsgTimeout值，相当于使用默认值
	case msgTimeout == 0:
		// do nothing (use default)
	// 如果msgTimeout在有效范围内，更新c.MsgTimeout值
	case msgTimeout >= 1000 &&
		msgTimeout <= int(c.nsqd.getOpts().MaxMsgTimeout/time.Millisecond):
		c.MsgTimeout = time.Duration(msgTimeout) * time.Millisecond
	// 如果msgTimeout不在有效范围内，返回错误
	default:
		return fmt.Errorf("msg timeout (%d) is invalid", msgTimeout)
	}

	// 操作成功，返回nil
	return nil
}

// UpgradeTLS 升级客户端连接到TLS（传输层安全性协议）。
// 这个方法首先获取写锁，以确保在升级过程中不会有其他写操作正在进行。
// 然后，它将现有的连接包装成一个TLS连接，并进行TLS握手以初始化安全连接。
// 如果握手成功，它会更新客户端的读取器和写入器为使用新的TLS连接，并设置TLS状态标志。
func (c *clientV2) UpgradeTLS() error {
	// 获取写锁以保护连接的升级过程。
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	// 使用给定的TLS配置将现有连接包装为TLS服务器连接。
	tlsConn := tls.Server(c.Conn, c.nsqd.tlsConfig)
	// 为TLS连接设置超时，以确保握手不会无限期挂起。
	tlsConn.SetDeadline(time.Now().Add(5 * time.Second))
	// 执行TLS握手以初始化安全连接。
	err := tlsConn.Handshake()
	if err != nil {
		// 如果握手失败，返回错误。
		return err
	}
	// 将TLS连接存储在客户端实例中以供后续使用。
	c.tlsConn = tlsConn

	// 更新客户端的读取器和写入器以使用新的TLS连接。
	c.Reader = bufio.NewReaderSize(c.tlsConn, defaultBufferSize)
	c.Writer = bufio.NewWriterSize(c.tlsConn, c.OutputBufferSize)

	// 将TLS状态标志设置为1（表示启用），以指示TLS已在客户端上启用。
	atomic.StoreInt32(&c.TLS, 1)

	// 升级过程完成，没有错误。
	return nil
}

// UpgradeSnappy UpgradeSnappy将客户端连接升级为使用Snappy压缩
// 此方法获取写锁以确保在执行升级过程中的排他访问
// 它首先检查是否已建立TLS连接，如果是，则使用TLS连接而不是普通连接
// 然后，它设置读取器为带缓冲的Snappy解压缩器，并设置写入器为带缓冲的Snappy压缩器
// 最后，它更新Snappy状态标志以指示Snappy压缩现在是活动的
func (c *clientV2) UpgradeSnappy() error {
	// 获取写锁以保护连接设置
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	// 根据情况选择适当的连接
	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	// 配置读取器为带缓冲的Snappy解压缩器
	c.Reader = bufio.NewReaderSize(snappy.NewReader(conn), defaultBufferSize)

	// 配置写入器为带缓冲的Snappy压缩器
	// 注意：此处使用NewWriter是因为我们自行处理缓冲，而不是使用NewBufferedWriter
	c.Writer = bufio.NewWriterSize(snappy.NewWriter(conn), c.OutputBufferSize)

	// 更新Snappy状态标志
	atomic.StoreInt32(&c.Snappy, 1)

	return nil
}

// UpgradeDeflate 升级当前连接以使用压缩数据传输。
// 该方法通过flate包实现了数据的压缩，提高了数据传输的效率。
// 参数level控制压缩的级别，较高的级别会减小传输的数据量，但可能会增加CPU的使用率。
// 如果升级过程中发生错误，方法会返回该错误。
func (c *clientV2) UpgradeDeflate(level int) error {
	// 获取写锁，以确保在升级过程中不会有其他写操作发生，避免数据的不一致性。
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	// 选择连接对象。如果存在TLS连接，则使用TLS连接，否则使用普通连接。
	// 这样做是为了确保使用的是最安全的连接方式。
	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	// 为读取操作创建一个带有缓冲的flate.Reader，以读取压缩后的数据。
	// 使用了bufio.NewReaderSize来设置缓冲区的大小，以提高读取效率。
	c.Reader = bufio.NewReaderSize(flate.NewReader(conn), defaultBufferSize)

	// 创建一个flate.Writer用于写入压缩数据，并设置压缩级别。
	// 虽然NewWriter可能返回错误，但此处忽略了错误处理，因为无法恢复且应避免阻塞。
	fw, _ := flate.NewWriter(conn, level)
	c.flateWriter = fw
	// 为写入操作创建一个带有缓冲的bufio.Writer，以提高写入效率。
	c.Writer = bufio.NewWriterSize(fw, c.OutputBufferSize)

	// 使用原子操作更新c.Deflate，表示现在使用了压缩。
	// 这是为了确保在多线程环境下，对共享变量的访问是安全的。
	atomic.StoreInt32(&c.Deflate, 1)

	// 如果一切顺利，返回nil，表示升级成功。
	return nil
}

// FinishedMessage 标记消息处理完成。
// 该函数通过原子操作更新完成计数和飞行中计数，并尝试更新就绪状态。
func (c *clientV2) FinishedMessage() {
	// 原子性增加完成计数
	atomic.AddUint64(&c.FinishCount, 1)
	// 原子性减少飞行中计数
	atomic.AddInt64(&c.InFlightCount, -1)
	// 尝试更新客户端的就绪状态
	c.tryUpdateReadyState()
}

// RequeuedMessage 重新排队消息。
// 该函数用于处理消息的重新排队逻辑，包括更新重排队计数和在处理中消息计数。
func (c *clientV2) RequeuedMessage() {
	// 将重排队计数加1。
	atomic.AddUint64(&c.RequeueCount, 1)
	// 将在处理中消息计数减1。
	atomic.AddInt64(&c.InFlightCount, -1)
	// 尝试更新消息准备状态。
	c.tryUpdateReadyState()
}

// tryUpdateReadyState 尝试更新客户端的就绪状态。
// 该函数通过向就绪状态通道发送信号来通知状态变更，但仅在通道可写时发送。
func (c *clientV2) tryUpdateReadyState() {
	// 尝试向就绪状态通道发送信号，表示状态可能已经更新
	select {
	case c.ReadyStateChan <- 1:
		// 通道可写，成功发送信号
	default:
		// 通道不可写，放弃发送信号
	}
}

// StartClose 开始关闭客户端
//
// 该函数将客户端状态标记为“正在关闭”，以启动关闭流程。
// 它主要通过设置状态标志和更改就绪计数来实现。
func (c *clientV2) StartClose() {
	// 将客户端的就绪状态强制设置为0，以确保不再处理新的请求
	c.SetReadyCount(0)

	// 标记客户端状态为“正在关闭”，通知其他操作该客户端正在关闭过程中
	atomic.StoreInt32(&c.State, stateClosing)
}

// SetReadyCount 设置客户端的ReadyCount计数。
// 该函数通过原子操作设置ReadyCount，确保在并发环境下安全更新。
// 参数:
//
//	count - 要设置的新ReadyCount值。
//
// 通过原子操作设置ReadyCount，若新旧值不相同，则尝试更新客户端的就绪状态。
func (c *clientV2) SetReadyCount(count int64) {
	oldCount := atomic.SwapInt64(&c.ReadyCount, count)

	if oldCount != count {
		c.tryUpdateReadyState()
	}
}

// IsAuthorized 检查客户端是否对特定主题和频道有访问权限。
// 参数:
//
//	topic - 要访问的主题。
//	channel - 要访问的频道。
//
// 返回值:
//
//	bool - 客户端是否有访问权限。
//	error - 如果发生错误，则返回错误信息。
func (c *clientV2) IsAuthorized(topic, channel string) (bool, error) {
	// 检查AuthState是否为空，如果为空，则返回无权限。
	if c.AuthState == nil {
		return false, nil
	}
	// 如果权限状态已过期，则尝试重新查询权限。
	if c.AuthState.IsExpired() {
		err := c.QueryAuthd()
		if err != nil {
			return false, err
		}
	}
	// 如果客户端被允许访问指定的主题和频道，则返回有权限。
	if c.AuthState.IsAllowed(topic, channel) {
		return true, nil
	}
	// 默认情况下返回无权限。
	return false, nil
}

// HasAuthorizations 检查客户端是否拥有任何授权权限。
// 返回值:
//
//	bool - 如果客户端拥有至少一个授权权限，则返回true；否则返回false。
func (c *clientV2) HasAuthorizations() bool {
	// 检查AuthState是否为空，如果为空，则返回无任何授权权限。
	if c.AuthState != nil {
		// 返回客户端是否拥有授权权限。
		return len(c.AuthState.Authorizations) != 0
	}
	// 默认情况下返回无任何授权权限。
	return false
}

// QueryAuthd 验证客户端的身份认证状态。
// 该方法通过查询权威服务器来确定当前连接是否已认证。
// 它考虑了TLS（SSL）连接和远程IP在身份认证过程中的作用。
func (c *clientV2) QueryAuthd() error {
	// 初始化远程IP变量
	remoteIP := ""
	// 如果客户端连接的网络类型为TCP，尝试获取远程IP地址
	if c.RemoteAddr().Network() == "tcp" {
		ip, _, err := net.SplitHostPort(c.String())
		if err != nil {
			return err
		}
		remoteIP = ip
	}

	// 检查TLS（SSL）是否启用
	tlsEnabled := atomic.LoadInt32(&c.TLS) == 1
	// 初始化证书通用名称变量
	commonName := ""
	// 如果TLS启用，尝试获取客户端证书的通用名称
	if tlsEnabled {
		tlsConnState := c.tlsConn.ConnectionState()
		if len(tlsConnState.PeerCertificates) > 0 {
			commonName = tlsConnState.PeerCertificates[0].Subject.CommonName
		}
	}

	// 使用查询到的远程IP、TLS状态和通用名称等信息，向认证服务器查询身份认证状态
	authState, err := auth.QueryAnyAuthd(c.nsqd.getOpts().AuthHTTPAddresses,
		remoteIP, tlsEnabled, commonName, c.AuthSecret,
		c.nsqd.clientTLSConfig,
		c.nsqd.getOpts().HTTPClientConnectTimeout,
		c.nsqd.getOpts().HTTPClientRequestTimeout,
		c.nsqd.getOpts().AuthHTTPRequestMethod,
	)
	if err != nil {
		return err
	}
	// 更新客户端的身份认证状态
	c.AuthState = authState
	return nil
}

// String 返回客户端远程地址的字符串表示。
// 该方法主要用途是在需要显示客户端地址时，提供一个方便的途径获取地址信息。
func (c *clientV2) String() string {
	return c.RemoteAddr().String()
}

// PublishedMessage 增加指定主题的已发布消息计数。
//
// 该方法在 clientV2 结构体中定义，用于在消息发布时更新内部状态。
// 它使用一个互斥锁来确保线程安全，以防止并发访问引发的问题。
//
// 参数:
//
//	topic - 消息所属的主题字符串。
//	count - 要增加的消息计数，类型为 uint64。
func (c *clientV2) PublishedMessage(topic string, count uint64) {
	// 加锁以确保线程安全地访问 pubCounts。
	c.metaLock.Lock()
	// 增加指定主题的消息计数。
	c.pubCounts[topic] += count
	// 解锁以释放对其他线程的阻塞。
	c.metaLock.Unlock()
}

// Pause 暂停客户端状态更新。
// 该方法在客户端需要暂停状态更新时调用，通过更新内部状态来准备暂停操作。
func (c *clientV2) Pause() {
	// 尝试更新客户端的就绪状态，以确保客户端处于正确的状态进行暂停。
	c.tryUpdateReadyState()
}

// UnPause 用于取消暂停客户端状态。
// 该方法内部调用了tryUpdateReadyState函数来尝试更新客户端的状态，
// 以达到从暂停状态恢复到准备就绪状态的目的。
func (c *clientV2) UnPause() {
	// 尝试更新客户端的状态，这是实现取消暂停的核心操作。
	c.tryUpdateReadyState()
}

// TimedOutMessage 表示处理消息超时的情况。
// 该函数将处理中的消息计数减一，并尝试更新客户端的就绪状态。
func (c *clientV2) TimedOutMessage() {
	// 将处理中的消息计数减一，表示一个消息因超时而不再处理。
	atomic.AddInt64(&c.InFlightCount, -1)

	// 尝试更新客户端的就绪状态，以反映消息超时可能带来的变化。
	c.tryUpdateReadyState()
}

// Stats 收集并返回客户端的统计信息。
// 如果提供了topicName，则仅返回与该主题相关的发布计数。
func (c *clientV2) Stats(topicName string) ClientStats {
	// 加读锁以保护客户端元数据
	c.metaLock.RLock()
	// 初始化统计信息的基本字段
	clientID := c.ClientID
	hostname := c.Hostname
	userAgent := c.UserAgent
	var identity string
	var identityURL string
	// 如果鉴权状态不为空，记录身份信息
	if c.AuthState != nil {
		identity = c.AuthState.Identity
		identityURL = c.AuthState.IdentityURL
	}
	// 初始化pubCounts切片并根据topicName过滤发布计数
	pubCounts := make([]PubCount, 0, len(c.pubCounts))
	for topic, count := range c.pubCounts {
		// 如果提供了特定的主题名且当前主题不符合，则跳过
		if len(topicName) > 0 && topic != topicName {
			continue
		}
		// 添加当前主题的发布计数到结果切片
		pubCounts = append(pubCounts, PubCount{
			Topic: topic,
			Count: count,
		})
		// 由于我们可能只对特定主题的计数感兴趣，一旦找到就退出循环
		break
	}
	// 解锁以允许其他读写操作
	c.metaLock.RUnlock()

	// 构建并返回客户端统计信息
	stats := ClientV2Stats{
		Version:         "V2",
		RemoteAddress:   c.RemoteAddr().String(),
		ClientID:        clientID,
		Hostname:        hostname,
		UserAgent:       userAgent,
		State:           atomic.LoadInt32(&c.State),
		ReadyCount:      atomic.LoadInt64(&c.ReadyCount),
		InFlightCount:   atomic.LoadInt64(&c.InFlightCount),
		MessageCount:    atomic.LoadUint64(&c.MessageCount),
		FinishCount:     atomic.LoadUint64(&c.FinishCount),
		RequeueCount:    atomic.LoadUint64(&c.RequeueCount),
		ConnectTime:     c.ConnectTime.Unix(),
		SampleRate:      atomic.LoadInt32(&c.SampleRate),
		TLS:             atomic.LoadInt32(&c.TLS) == 1,
		Deflate:         atomic.LoadInt32(&c.Deflate) == 1,
		Snappy:          atomic.LoadInt32(&c.Snappy) == 1,
		Authed:          c.HasAuthorizations(),
		AuthIdentity:    identity,
		AuthIdentityURL: identityURL,
		PubCounts:       pubCounts,
	}

	// 如果客户端使用TLS连接，则收集TLS相关信息
	if stats.TLS {
		p := prettyConnectionState{c.tlsConn.ConnectionState()}
		stats.CipherSuite = p.GetCipherSuite()
		stats.TLSVersion = p.GetVersion()
		stats.TLSNegotiatedProtocol = p.NegotiatedProtocol
		stats.TLSNegotiatedProtocolIsMutual = p.NegotiatedProtocolIsMutual
	}

	// 返回填充好的客户端统计信息
	return stats
}

// String 方法提供了一个关于 ClientV2Stats 实例的详细字符串表示。
// 它通过格式化输出展示了客户端的连接时间、持续时间、远程地址、代理信息，
// 以及该客户端作为生产者或消费者时的不同统计信息。
func (s ClientV2Stats) String() string {
	// 将连接时间戳转换为时间对象
	connectTime := time.Unix(s.ConnectTime, 0)
	// 计算连接的持续时间，并精确到秒
	duration := time.Since(connectTime).Truncate(time.Second)

	// 解析远程地址以获取端口号
	_, port, _ := net.SplitHostPort(s.RemoteAddress)
	// 构造客户端标识字符串，包含主机名、端口号和用户代理信息
	id := fmt.Sprintf("%s:%s %s", s.Hostname, port, s.UserAgent)

	// 当存在发布计数信息时，格式化生产者信息
	if len(s.PubCounts) > 0 {
		var total uint64
		var topicOut []string
		for _, v := range s.PubCounts {
			total += v.Count
			topicOut = append(topicOut, fmt.Sprintf("%s=%d", v.Topic, v.Count))
		}
		return fmt.Sprintf("[%s %-21s] msgs: %-8d topics: %s connected: %s",
			s.Version,
			id,
			total,
			strings.Join(topicOut, ","),
			duration,
		)
	}

	// 当不存在发布计数信息时，格式化消费者信息
	return fmt.Sprintf("[%s %-21s] state: %d inflt: %-4d rdy: %-4d fin: %-8d re-q: %-8d msgs: %-8d connected: %s",
		s.Version,
		id,
		s.State,
		s.InFlightCount,
		s.ReadyCount,
		s.FinishCount,
		s.RequeueCount,
		s.MessageCount,
		duration,
	)
}

// Empty 清空客户端状态。
// 该方法主要用于客户端异常或需要重置状态的场景。
// 它通过将飞行中的请求计数器 InFlightCount 设置为 0 来实现。
// 同时，它调用 tryUpdateReadyState 方法来尝试更新客户端的就绪状态。
func (c *clientV2) Empty() {
	// 使用原子操作将 InFlightCount 设置为 0，确保线程安全。
	atomic.StoreInt64(&c.InFlightCount, 0)
	// 调用 tryUpdateReadyState 方法尝试更新客户端的就绪状态。
	c.tryUpdateReadyState()
}

// Auth 使用给定的密钥对客户端进行认证。
//
// secret 是服务器用于认证的密钥。
//
// 返回错误如果认证失败。
func (c *clientV2) Auth(secret string) error {
	c.AuthSecret = secret // 将认证密钥设置给客户端。
	return c.QueryAuthd() // 调用QueryAuthd方法进行认证查询。
}
