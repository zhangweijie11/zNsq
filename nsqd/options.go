package nsqd

import (
	"crypto/md5"
	"crypto/tls"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"
)

type Options struct {
	Logger    Logger      // 日志引擎
	ID        int64       `flag:"node-id" cfg:"id"`            // 节点 ID
	LogLevel  lg.LogLevel `flag:"log-level" cfg:"log-level"`   // 日志级别
	LogPrefix string      `flag:"log-prefix" cfg:"log-prefix"` // 日志信息前缀
	// 客户端配置
	TCPAddress               string        `flag:"tcp-address"`                                                   // TCP 监听地址
	HTTPAddress              string        `flag:"http-address"`                                                  // HTTP 监听地址
	HTTPSAddress             string        `flag:"https-address"`                                                 // HTTPS 监听地址
	BroadcastAddress         string        `flag:"broadcast-address"`                                             // 节点广播地址
	BroadcastTCPPort         int           `flag:"broadcast-tcp-port"`                                            // 节点广播端口
	BroadcastHTTPPort        int           `flag:"broadcast-http-port"`                                           // 节点广播 HTTP 端口
	NSQLookupdTCPAddresses   []string      `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"`            // lookupd 服务地址
	AuthHTTPAddresses        []string      `flag:"auth-http-address" cfg:"auth_http_addresses"`                   // 认证服务地址
	AuthHTTPRequestMethod    string        `flag:"auth-http-request-method" cfg:"auth_http_request_method"`       // 认证请求方法
	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout" cfg:"http_client_connect_timeout"` // HTTP 客户端连接超时时间
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout" cfg:"http_client_request_timeout"` // HTTP 客户端请求超时时间
	// 数据持久化配置
	DataPath        string        `flag:"data-path" cfg:"data-path"` // 数据路径
	MemQueueSize    int64         `flag:"mem-queue-size"`            // 内存消息队列长度
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"`        // 文件最大字节数
	SyncEvery       int64         `flag:"sync-every"`                // 同步间隔
	SyncTimeout     time.Duration `flag:"sync-timeout"`              // 同步超时时间
	// 队列扫描配置
	QueueScanInterval        time.Duration // 队列扫描间隔
	QueueScanRefreshInterval time.Duration // 队列刷新间隔
	QueueScanSelectionCount  int           `flag:"queue-scan-selection-count"` // 队列选择数量
	QueueScanWorkerPoolMax   int           `flag:"queue-scan-worker-pool-max"` // 队列扫描工作池最大值
	QueueScanDirtyPercent    float64       // 队列脏百分比
	// 消息配置
	MsgTimeout    time.Duration `flag:"msg-timeout"`     // 消息超时时间
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"` // 消息最大超时时间
	MaxMsgSize    int64         `flag:"max-msg-size"`    // 消息最大字节数
	MaxBodySize   int64         `flag:"max-body-size"`   // 最大消息体大小
	MaxReqTimeout time.Duration `flag:"max-req-timeout"` // 客户端最大请求超时时间
	ClientTimeout time.Duration // 客户端超时时间
	// 心跳相关配置
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"`    // 客户端最大心跳间隔
	MaxRdyCount            int64         `flag:"max-rdy-count"`             // 最大 RDY 值
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`    // 最大输出缓冲大小
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"` // 最大输出缓冲超时时间
	MinOutputBufferTimeout time.Duration `flag:"min-output-buffer-timeout"` // 最小输出缓冲超时时间
	OutputBufferTimeout    time.Duration `flag:"output-buffer-timeout"`     // 输出缓冲超时时间
	MaxChannelConsumers    int           `flag:"max-channel-consumers"`     // 限制每个 topic 的消费者数量
	// StatsD 配置
	StatsdAddress          string        `flag:"statsd-address"`           // StatsD 服务地址
	StatsdPrefix           string        `flag:"statsd-prefix"`            // StatsD 前缀
	StatsdInterval         time.Duration `flag:"statsd-interval"`          // StatsD 发送间隔
	StatsdMemStats         bool          `flag:"statsd-mem-stats"`         // 是否发送内存统计信息
	StatsdUDPPacketSize    int           `flag:"statsd-udp-packet-size"`   // StatsD UDP 包大小
	StatsdExcludeEphemeral bool          `flag:"statsd-exclude-ephemeral"` // 是否排除临时队列
	// 端到端处理延迟
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`                                         // 端到端处理延迟窗口时间
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"` // 端到端处理延迟百分比
	// TLS 配置
	TLSCert             string `flag:"tls-cert"`               // TLS 证书
	TLSKey              string `flag:"tls-key"`                // TLS 私钥
	TLSRequired         int    `flag:"tls-required"`           // TLS 认证模式
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy"` // 客户端 TLS 认证策略
	TLSRootCAFile       string `flag:"tls-root-ca-file"`       // 客户端 TLS 根 CA 文件
	TLSMinVersion       uint16 `flag:"tls-min-version"`        // TLS 最小版本
	// 压缩
	DeflateEnabled  bool `flag:"deflate"`           // 是否启用 deflate 压缩
	SnappyEnabled   bool `flag:"snappy"`            // 是否启用 snappy 压缩
	MaxDeflateLevel int  `flag:"max-deflate-level"` // deflate 压缩级别
}

// NewOptions 初始化Options结构体实例，用于配置nsqd的各种参数。
// 该函数通过获取主机名并计算其哈希值来生成默认的ID，
// 并设置了一组默认的配置值供nsqd使用。
func NewOptions() *Options {
	// 获取当前主机名
	hostname, err := os.Hostname()
	if err != nil {
		// 如果获取主机名失败，记录错误并退出
		log.Fatal(err)
	}

	// 创建一个MD5哈希对象
	h := md5.New()
	// 将主机名写入哈希对象并计算哈希值
	io.WriteString(h, hostname)
	// 使用CRC32算法计算哈希值，并取模1024作为默认ID
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	// 返回一个新的Options实例，使用默认配置和计算出的默认ID
	return &Options{
		ID:        defaultID,
		LogPrefix: "[nsqd] ",
		LogLevel:  lg.INFO,

		TCPAddress:        "0.0.0.0:4150",
		HTTPAddress:       "0.0.0.0:4151",
		HTTPSAddress:      "0.0.0.0:4152",
		BroadcastAddress:  "0.0.0.0",
		BroadcastTCPPort:  0,
		BroadcastHTTPPort: 0,

		NSQLookupdTCPAddresses: make([]string, 0),
		AuthHTTPAddresses:      make([]string, 0),
		AuthHTTPRequestMethod:  "get",

		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,

		QueueScanInterval:        100 * time.Millisecond,
		QueueScanRefreshInterval: 5 * time.Second,
		QueueScanSelectionCount:  20,
		QueueScanWorkerPoolMax:   4,
		QueueScanDirtyPercent:    0.25,

		MsgTimeout:    60 * time.Second,
		MaxMsgTimeout: 15 * time.Minute,
		MaxMsgSize:    1024 * 1024,
		MaxBodySize:   5 * 1024 * 1024,
		MaxReqTimeout: 1 * time.Hour,
		ClientTimeout: 60 * time.Second,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxRdyCount:            2500,
		MaxOutputBufferSize:    64 * 1024,
		MaxOutputBufferTimeout: 30 * time.Second,
		MinOutputBufferTimeout: 25 * time.Millisecond,
		OutputBufferTimeout:    250 * time.Millisecond,
		MaxChannelConsumers:    0,

		StatsdPrefix:        "nsq.%s",
		StatsdInterval:      60 * time.Second,
		StatsdMemStats:      true,
		StatsdUDPPacketSize: 508,

		E2EProcessingLatencyWindowTime: time.Duration(10 * time.Minute),

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,

		TLSMinVersion: tls.VersionTLS10,
	}
}
