package clusterinfo

import (
	"encoding/json"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/blang/semver"
	"github.com/zhangweijie11/zNsq/internal/quantile"
)

type ProducerTopic struct {
	Topic      string `json:"topic"`
	Tombstoned bool   `json:"tombstoned"`
}

type ProducerTopics []ProducerTopic

func (pt ProducerTopics) Len() int           { return len(pt) }
func (pt ProducerTopics) Swap(i, j int)      { pt[i], pt[j] = pt[j], pt[i] }
func (pt ProducerTopics) Less(i, j int) bool { return pt[i].Topic < pt[j].Topic }

// Producer 生产者
// 该结构体用于定义生产者的配置和状态信息
type Producer struct {
	// RemoteAddresses 是生产者的远程地址列表
	RemoteAddresses []string `json:"remote_addresses"`
	// RemoteAddress 是生产者的单个远程地址
	RemoteAddress string `json:"remote_address"`
	// Hostname 是生产者的主机名
	Hostname string `json:"hostname"`
	// BroadcastAddress 是生产者用于广播的地址
	BroadcastAddress string `json:"broadcast_address"`
	// TCPPort 是生产者使用的TCP端口
	TCPPort int `json:"tcp_port"`
	// HTTPPort 是生产者使用的HTTP端口
	HTTPPort int `json:"http_port"`
	// Version 是生产者的版本信息
	Version string `json:"version"`
	// VersionObj 是生产者的版本对象，用于内部版本比较
	VersionObj semver.Version `json:"-"`
	// Topics 是生产者处理的主题列表
	Topics ProducerTopics `json:"topics"`
	// OutOfDate 表示生产者的版本是否过时
	OutOfDate bool `json:"out_of_date"`
}

// UnmarshalJSON UnmarshalJSON将JSON字节切片解码为Producer类型实例。
// 该方法用于从JSON格式的数据中提取有关生产者的信息，
// 包括远程地址、主机名、广播地址、TCP端口、HTTP端口、版本和主题。
// 解码过程中，它会创建一个新的Producer实例，并将解码的信息复制到该实例中。
// 对于主题信息，它会与tombstone标志一起存储在p.Topics中。
// 如果解码的版本信息无效，它会将版本设置为"0.0.0"。
func (p *Producer) UnmarshalJSON(b []byte) error {
	// 定义一个匿名结构体用于存储解码的JSON数据
	var r struct {
		RemoteAddress    string   `json:"remote_address"`
		Hostname         string   `json:"hostname"`
		BroadcastAddress string   `json:"broadcast_address"`
		TCPPort          int      `json:"tcp_port"`
		HTTPPort         int      `json:"http_port"`
		Version          string   `json:"version"`
		Topics           []string `json:"topics"`
		Tombstoned       []bool   `json:"tombstones"`
	}

	// 解码JSON数据到上面定义的匿名结构体
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}

	// 将解码的数据复制到Producer实例p中
	*p = Producer{
		RemoteAddress:    r.RemoteAddress,
		Hostname:         r.Hostname,
		BroadcastAddress: r.BroadcastAddress,
		TCPPort:          r.TCPPort,
		HTTPPort:         r.HTTPPort,
		Version:          r.Version,
	}

	// 处理主题和tombstone标志的配对
	for i, t := range r.Topics {
		// 将主题和对应的tombstone标志添加到p.Topics中
		p.Topics = append(p.Topics, ProducerTopic{Topic: t, Tombstoned: r.Tombstoned[i]})
	}

	// 解析版本信息
	version, err := semver.Parse(p.Version)
	if err != nil {
		// 如果版本信息无效，默认设置为"0.0.0"
		version, _ = semver.Parse("0.0.0")
	}
	p.VersionObj = version

	return nil
}

// Address 返回生产者的远程地址。
// 如果远程地址未设置，则返回"N/A"。
// 此方法提供了一种安全的方式来获取远程地址信息，
// 而不必在调用代码中进行空值检查。
func (p *Producer) Address() string {
	if p.RemoteAddress == "" {
		return "N/A"
	}
	return p.RemoteAddress
}

// HTTPAddress HTTPAddress返回生产者HTTP服务的地址。
// 通过组合广播地址和HTTP端口，返回一个完整的HTTP服务地址。
// 返回值是一个字符串，表示生产者的HTTP服务地址，格式为"主机:端口"。
func (p *Producer) HTTPAddress() string {
	return net.JoinHostPort(p.BroadcastAddress, strconv.Itoa(p.HTTPPort))
}

// TCPAddress TCPAddress返回生产者TCP服务的地址。
// 通过组合广播地址和TCP端口，返回一个完整的TCP服务地址。
// 返回值是一个字符串，表示生产者的TCP服务地址，格式为"主机:端口"。
func (p *Producer) TCPAddress() string {
	return net.JoinHostPort(p.BroadcastAddress, strconv.Itoa(p.TCPPort))
}

// IsInconsistent 检查生产者是否状态不一致。
//
// 通过比较生产者实例中保存的远程地址数量与期望的lookupd服务数量，
// 来判断生产者是否处于不一致状态。如果不一致，返回true；否则，返回false。
//
// 参数:
//
//	numLookupd - 期望的lookupd服务数量，用于比较。
//
// 返回值:
//
//	如果生产者的远程地址数量与期望的lookupd服务数量不相等，则返回true，表示状态不一致；
//	否则返回false，表示状态一致。
func (p *Producer) IsInconsistent(numLookupd int) bool {
	return len(p.RemoteAddresses) != numLookupd
}

// TopicStats 主题统计
// 该结构体用于统计消息队列中主题的相关信息，包括主题的深度、消息数量等。
type TopicStats struct {
	Node                 string                                  `json:"node"`                   // 节点名称
	Hostname             string                                  `json:"hostname"`               // 主机名称
	TopicName            string                                  `json:"topic_name"`             // 主题名称
	Depth                int64                                   `json:"depth"`                  // 主题深度，即等待处理的消息数量
	MemoryDepth          int64                                   `json:"memory_depth"`           // 内存中消息深度
	BackendDepth         int64                                   `json:"backend_depth"`          // 后端存储深度
	MessageCount         int64                                   `json:"message_count"`          // 发送的消息数量
	NodeStats            []*TopicStats                           `json:"nodes"`                  // 节点统计信息数组，用于集群环境
	Channels             []*ChannelStats                         `json:"channels"`               // 通道统计信息数组
	Paused               bool                                    `json:"paused"`                 // 主题是否暂停
	E2eProcessingLatency *quantile.E2eProcessingLatencyAggregate `json:"e2e_processing_latency"` // 端到端处理延迟
}

// Add 合并另一个 TopicStats 实例的数据到当前实例中。
// 它更新当前主题统计信息的度量，包括深度、消息计数等，
// 并合并通道统计信息和节点统计信息。
// 参数:
//
//	a *TopicStats: 要合并的 TopicStats 实例。
func (t *TopicStats) Add(a *TopicStats) {
	// 标记节点为通配符，表示汇总统计信息不特定于某个节点。
	t.Node = "*"
	// 累加深度、内存深度、后端深度和消息计数。
	t.Depth += a.Depth
	t.MemoryDepth += a.MemoryDepth
	t.BackendDepth += a.BackendDepth
	t.MessageCount += a.MessageCount
	// 如果另一个实例的暂停状态为 true，则设置当前实例的暂停状态。
	if a.Paused {
		t.Paused = a.Paused
	}
	// 合并通道统计信息。
	for _, aChannelStats := range a.Channels {
		found := false
		for _, channelStats := range t.Channels {
			// 如果找到匹配的通道名称，则累加统计信息。
			if aChannelStats.ChannelName == channelStats.ChannelName {
				found = true
				channelStats.Add(aChannelStats)
			}
		}
		// 如果没有找到匹配的通道名称，则将其添加到列表中。
		if !found {
			t.Channels = append(t.Channels, aChannelStats)
		}
	}
	// 添加源实例到节点统计信息列表，并按主机排序。
	t.NodeStats = append(t.NodeStats, a)
	sort.Sort(TopicStatsByHost{t.NodeStats})
	// 初始化端到端处理延迟聚合，如果尚未初始化。
	if t.E2eProcessingLatency == nil {
		t.E2eProcessingLatency = &quantile.E2eProcessingLatencyAggregate{
			Addr:  t.Node,
			Topic: t.TopicName,
		}
	}
	// 累加端到端处理延迟。
	t.E2eProcessingLatency.Add(a.E2eProcessingLatency)
}

// ChannelStats 通道统计
// 该结构体用于记录和展示消息通道的相关统计信息，包括但不限于节点信息、通道深度、消息统计及客户端信息。
type ChannelStats struct {
	// Node节点名称
	Node string `json:"node"`
	// Hostname主机名
	Hostname string `json:"hostname"`
	// TopicName主题名称
	TopicName string `json:"topic_name"`
	// ChannelName通道名称
	ChannelName string `json:"channel_name"`
	// Depth总深度
	Depth int64 `json:"depth"`
	// MemoryDepth内存深度
	MemoryDepth int64 `json:"memory_depth"`
	// BackendDepth后端深度
	BackendDepth int64 `json:"backend_depth"`
	// InFlightCount飞行中消息数量
	InFlightCount int64 `json:"in_flight_count"`
	// DeferredCount延迟消息数量
	DeferredCount int64 `json:"deferred_count"`
	// RequeueCount重新排队消息数量
	RequeueCount int64 `json:"requeue_count"`
	// TimeoutCount超时消息数量
	TimeoutCount int64 `json:"timeout_count"`
	// MessageCount消息数量
	MessageCount int64 `json:"message_count"`
	// ClientCount客户端数量
	ClientCount int `json:"client_count"`
	// Selected选择状态，用于内部处理，不包含在JSON序列化中
	Selected bool `json:"-"`
	// NodeStats节点统计信息数组
	NodeStats []*ChannelStats `json:"nodes"`
	// Clients客户端统计信息数组
	Clients []*ClientStats `json:"clients"`
	// Paused暂停状态
	Paused bool `json:"paused"`
	// E2eProcessingLatency端到端处理延迟聚合信息
	E2eProcessingLatency *quantile.E2eProcessingLatencyAggregate `json:"e2e_processing_latency"`
}

// Add 方法用于将另一个 ChannelStats 对象的统计信息添加到当前 ChannelStats 对象中。
// 它通过累加各种统计指标来汇总通道的统计信息，并处理暂停状态及客户端和节点统计信息的列表。
// 此方法支持统计信息的聚合，以便进行更全面的分析或报告。
func (c *ChannelStats) Add(a *ChannelStats) {
	// 标记节点为通配符，表示统计信息不分特定节点
	c.Node = "*"
	// 累加深度相关的统计信息
	c.Depth += a.Depth
	c.MemoryDepth += a.MemoryDepth
	c.BackendDepth += a.BackendDepth
	// 累加消息处理状态的统计信息
	c.InFlightCount += a.InFlightCount
	c.DeferredCount += a.DeferredCount
	c.RequeueCount += a.RequeueCount
	c.TimeoutCount += a.TimeoutCount
	// 累加计数类型的统计信息
	c.MessageCount += a.MessageCount
	c.ClientCount += a.ClientCount
	// 如果另一个 ChannelStats 对象处于暂停状态，则将当前对象标记为暂停
	if a.Paused {
		c.Paused = a.Paused
	}
	// 合并节点统计信息，并按一定规则排序
	c.NodeStats = append(c.NodeStats, a)
	sort.Sort(ChannelStatsByHost{c.NodeStats})
	// 初始化端到端处理延迟的聚合信息，如果尚未初始化
	if c.E2eProcessingLatency == nil {
		c.E2eProcessingLatency = &quantile.E2eProcessingLatencyAggregate{
			Addr:    c.Node,
			Topic:   c.TopicName,
			Channel: c.ChannelName,
		}
	}
	// 合并端到端处理延迟的统计信息
	c.E2eProcessingLatency.Add(a.E2eProcessingLatency)
	// 合并客户端统计信息，并按一定规则排序
	c.Clients = append(c.Clients, a.Clients...)
	sort.Sort(ClientsByHost{c.Clients})
}

// ClientStats 客户端统计
// 该结构体用于记录客户端的各种统计信息，包括连接信息、性能数据及安全特性使用情况。
type ClientStats struct {
	// Node 节点名称，标识客户端连接的服务器节点。
	Node string `json:"node"`
	// RemoteAddress 客户端的远程地址。
	RemoteAddress string `json:"remote_address"`
	// Version 客户端版本。
	Version string `json:"version"`
	// ClientID 客户端ID，用于唯一标识一个客户端。
	ClientID string `json:"client_id"`
	// Hostname 主机名，通常指客户端的主机名称。
	Hostname string `json:"hostname"`
	// UserAgent 用户代理字符串，包含客户端软件的信息。
	UserAgent string `json:"user_agent"`
	// ConnectTs 连接时间戳，表示客户端建立连接的时间。
	ConnectTs int64 `json:"connect_ts"`
	// ConnectedDuration 连接时长，表示客户端已连接的总时间。
	ConnectedDuration time.Duration `json:"connected"`
	// InFlightCount 当前未完成的请求数量。
	InFlightCount int `json:"in_flight_count"`
	// ReadyCount 准备就绪的请求数量，即准备发送或接收数据的请求。
	ReadyCount int `json:"ready_count"`
	// FinishCount 已完成的请求数量。
	FinishCount int64 `json:"finish_count"`
	// RequeueCount 重新排队的请求数量，通常指因某些原因未能处理而重新排队的请求。
	RequeueCount int64 `json:"requeue_count"`
	// MessageCount 收发的消息数量，统计客户端发送和接收的所有消息。
	MessageCount int64 `json:"message_count"`
	// SampleRate 采样率，用于统计采样数据的比例。
	SampleRate int32 `json:"sample_rate"`
	// Deflate 表示是否支持Deflate压缩算法。
	Deflate bool `json:"deflate"`
	// Snappy 表示是否支持Snappy压缩算法。
	Snappy bool `json:"snappy"`
	// Authed 表示客户端是否已通过身份验证。
	Authed bool `json:"authed"`
	// AuthIdentity 身份验证标识，记录用于身份验证的用户名或其他标识符。
	AuthIdentity string `json:"auth_identity"`
	// AuthIdentityURL 身份验证标识的URL，提供更多的身份验证信息。
	AuthIdentityURL string `json:"auth_identity_url"`
	// TLS 表示连接是否使用TLS（传输层安全性协议）。
	TLS bool `json:"tls"`
	// CipherSuite TLS使用的密码套件，提供加密和解密算法的详细信息。
	CipherSuite string `json:"tls_cipher_suite"`
	// TLSVersion TLS版本，表示使用的TLS协议版本。
	TLSVersion string `json:"tls_version"`
	// TLSNegotiatedProtocol TLS协商的协议，表示TLS协议协商的结果。
	TLSNegotiatedProtocol string `json:"tls_negotiated_protocol"`
	// TLSNegotiatedProtocolIsMutual 表示TLS协商的协议是否是双方都认可的。
	TLSNegotiatedProtocolIsMutual bool `json:"tls_negotiated_protocol_is_mutual"`
}

// UnmarshalJSON 实现了json.Unmarshaler接口，用于从JSON数据反序列化ClientStats结构体。
// 通过此方法，可以将JSON格式的字节切片转换为ClientStats实例。
// 参数:
//
//	b []byte: 待反序列化的JSON格式的字节切片。
//
// 返回值:
//
//	error: 如果反序列化过程中发生错误，返回该错误，否则返回nil。
func (s *ClientStats) UnmarshalJSON(b []byte) error {
	// 使用一个局部类型别名locaClientStats来防止json.Unmarshal递归调用。
	// 这是因为直接使用ClientStats类型进行反序列化可能会导致无限递归。
	type locaClientStats ClientStats // 重新定义类型，避免递归问题

	// 定义一个locaClientStats类型的变量ss，用于接收反序列化后的数据。
	var ss locaClientStats

	// 调用json.Unmarshal方法将字节切片b反序列化为ss。
	// 如果发生错误，则直接返回该错误。
	if err := json.Unmarshal(b, &ss); err != nil {
		return err
	}

	// 将ss转换为ClientStats类型，并赋值给接收者s。
	// 这样就完成了从JSON到ClientStats结构的转换。
	*s = ClientStats(ss)

	// 计算并设置ConnectedDuration字段。
	// 使用当前时间减去连接时间（ConnectTs）来获取连接时长，确保时间精度为秒。
	s.ConnectedDuration = time.Now().Truncate(time.Second).Sub(time.Unix(s.ConnectTs, 0))

	// 反序列化成功，返回nil。
	return nil
}

func (s *ClientStats) HasUserAgent() bool {
	return s.UserAgent != ""
}

func (s *ClientStats) HasSampleRate() bool {
	return s.SampleRate > 0
}

type ChannelStatsList []*ChannelStats

func (c ChannelStatsList) Len() int      { return len(c) }
func (c ChannelStatsList) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

type ChannelStatsByHost struct {
	ChannelStatsList
}

func (c ChannelStatsByHost) Less(i, j int) bool {
	return c.ChannelStatsList[i].Hostname < c.ChannelStatsList[j].Hostname
}

type ClientStatsList []*ClientStats

func (c ClientStatsList) Len() int      { return len(c) }
func (c ClientStatsList) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

type ClientsByHost struct {
	ClientStatsList
}

func (c ClientsByHost) Less(i, j int) bool {
	return c.ClientStatsList[i].Hostname < c.ClientStatsList[j].Hostname
}

type TopicStatsList []*TopicStats

func (t TopicStatsList) Len() int      { return len(t) }
func (t TopicStatsList) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

type TopicStatsByHost struct {
	TopicStatsList
}

func (c TopicStatsByHost) Less(i, j int) bool {
	return c.TopicStatsList[i].Hostname < c.TopicStatsList[j].Hostname
}

type Producers []*Producer

func (t Producers) Len() int      { return len(t) }
func (t Producers) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

func (t Producers) HTTPAddrs() []string {
	var addrs []string
	for _, p := range t {
		addrs = append(addrs, p.HTTPAddress())
	}
	return addrs
}

func (t Producers) Search(needle string) *Producer {
	for _, producer := range t {
		if needle == producer.HTTPAddress() {
			return producer
		}
	}
	return nil
}

type ProducersByHost struct {
	Producers
}

func (c ProducersByHost) Less(i, j int) bool {
	return c.Producers[i].Hostname < c.Producers[j].Hostname
}
