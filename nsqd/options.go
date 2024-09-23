package nsqd

import (
	"crypto/md5"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"
)

type Options struct {
	Logger                          Logger        // 日志引擎
	ID                              int64         `flag:"node-id" cfg:"id"`                                                           // 节点 ID
	LogLevel                        lg.LogLevel   `flag:"log-level" cfg:"log-level"`                                                  // 日志级别
	LogPrefix                       string        `flag:"log-prefix" cfg:"log-prefix"`                                                // 日志信息前缀
	DataPath                        string        `flag:"data-path" cfg:"data-path"`                                                  // 数据路径
	MemQueueSize                    int64         `flag:"mem-queue-size"`                                                             // 内存消息队列长度
	MsgTimeout                      time.Duration `flag:"msg-timeout"`                                                                // 消息超时时间
	MaxBytesPerFile                 int64         `flag:"max-bytes-per-file"`                                                         // 文件最大字节数
	MaxMsgSize                      int64         `flag:"max-msg-size"`                                                               // 消息最大字节数
	MaxMsgTimeout                   time.Duration `flag:"max-msg-timeout"`                                                            // 消息最大超时时间
	SyncEvery                       int64         `flag:"sync-every"`                                                                 // 同步间隔
	SyncTimeout                     time.Duration `flag:"sync-timeout"`                                                               // 同步超时时间
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`                                         // 端到端处理延迟窗口时间
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"` // 端到端处理延迟百分比
	NSQLookupdTCPAddresses          []string      `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"`                         // lookupd 服务地址
	OutputBufferTimeout             time.Duration `flag:"output-buffer-timeout"`                                                      // 输出缓冲超时时间
	ClientTimeout                   time.Duration `flag:"client-timeout"`                                                             // 客户端超时时间
	MaxBodySize                     int64         `flag:"max-body-size"`                                                              // 最大消息体大小
	MaxHeartbeatInterval            time.Duration `flag:"max-heartbeat-interval"`                                                     // 客户端最大心跳间隔
	MaxOutputBufferTimeout          time.Duration `flag:"max-output-buffer-timeout"`                                                  // 最大输出缓冲超时时间
	MinOutputBufferTimeout          time.Duration `flag:"min-output-buffer-timeout"`                                                  // 最小输出缓冲超时时间
	MaxOutputBufferSize             int64         `flag:"max-output-buffer-size"`                                                     // 最大输出缓冲大小
	MaxRdyCount                     int64         `flag:"max-rdy-count"`                                                              // 最大 RDY 值
	MaxReqTimeout                   time.Duration `flag:"max-req-timeout"`                                                            // 客户端最大请求超时时间
	DeflateEnabled                  bool          `flag:"deflate"`                                                                    // 是否启用 deflate 压缩
	MaxDeflateLevel                 int           `flag:"max-deflate-level"`                                                          // deflate 压缩级别
	SnappyEnabled                   bool          `flag:"snappy"`                                                                     // 是否启用 snappy 压缩
	AuthHTTPAddresses               []string      `flag:"auth-http-address" cfg:"auth_http_addresses"`                                // 认证服务地址
	AuthHTTPRequestMethod           string        `flag:"auth-http-request-method" cfg:"auth_http_request_method"`                    // 认证请求方法
	HTTPClientConnectTimeout        time.Duration `flag:"http-client-connect-timeout" cfg:"http_client_connect_timeout"`              // HTTP 客户端连接超时时间
	HTTPClientRequestTimeout        time.Duration `flag:"http-client-request-timeout" cfg:"http_client_request_timeout"`              // HTTP 客户端请求超时时间
	TLSRequired                     int           `flag:"tls-required"`                                                               // TLS 认证模式
	MaxChannelConsumers             int           `flag:"max-channel-consumers"`                                                      // 限制每个 topic 的消费者数量
	BroadcastAddress                string        `flag:"broadcast-address"`                                                          // 节点广播地址
	BroadcastTCPPort                int           `flag:"broadcast-tcp-port"`                                                         // 节点广播端口
	BroadcastHTTPPort               int           `flag:"broadcast-http-port"`                                                        // 节点广播 HTTP 端口
	QueueScanInterval               time.Duration `flag:"queue-scan-interval"`                                                        // 队列扫描间隔
	QueueScanRefreshInterval        time.Duration `flag:"queue-scan-refresh-interval"`                                                // 队列刷新间隔
	QueueScanSelectionCount         int           `flag:"queue-scan-selection-count"`                                                 // 队列选择数量
	QueueScanWorkerPoolMax          int           `flag:"queue-scan-worker-pool-max"`                                                 // 队列扫描工作池最大值
	QueueScanDirtyPercent           float64       `flag:"queue-scan-dirty-percent"`                                                   // 队列脏百分比
	StatsdAddress                   string        `flag:"statsd-address"`                                                             // StatsD 服务地址
	StatsdPrefix                    string        `flag:"statsd-prefix"`                                                              // StatsD 前缀
	StatsdInterval                  time.Duration `flag:"statsd-interval"`                                                            // StatsD 发送间隔
	StatsdMemStats                  bool          `flag:"statsd-mem-stats"`                                                           // 是否发送内存统计信息
	StatsdUDPPacketSize             int           `flag:"statsd-udp-packet-size"`                                                     // StatsD UDP 包大小
	StatsdExcludeEphemeral          bool          `flag:"statsd-exclude-ephemeral"`                                                   // 是否排除临时队列
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	// 生成一个基于输入数据哈希的 ID，且范围在 0 到 1023 之间
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:        defaultID,
		LogPrefix: "[nsqd] ",
		LogLevel:  lg.INFO,
	}
}
