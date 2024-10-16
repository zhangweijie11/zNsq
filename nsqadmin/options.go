package nsqadmin

import (
	"time"

	"github.com/zhangweijie11/zNsq/internal/lg"
)

// Options 定义了nsqadmin的配置参数
type Options struct {
	// LogLevel 日志级别
	LogLevel lg.LogLevel `flag:"log-level"`
	// LogPrefix 日志前缀
	LogPrefix string `flag:"log-prefix"`
	// Logger 自定义日志记录器
	Logger Logger
	// HTTPAddress HTTP服务监听的地址
	HTTPAddress string `flag:"http-address"`
	// BasePath API的基础路径
	BasePath string `flag:"base-path"`
	// DevStaticDir 开发环境下的静态文件目录
	DevStaticDir string `flag:"dev-static-dir"`
	// GraphiteURL Graphite监控系统的URL
	GraphiteURL string `flag:"graphite-url"`
	// ProxyGraphite 是否代理Graphite请求
	ProxyGraphite bool `flag:"proxy-graphite"`
	// StatsdPrefix Statsd监控数据的前缀
	StatsdPrefix string `flag:"statsd-prefix"`
	// StatsdCounterFormat Statsd计数器数据格式
	StatsdCounterFormat string `flag:"statsd-counter-format"`
	// StatsdGaugeFormat Statsd计量器数据格式
	StatsdGaugeFormat string `flag:"statsd-gauge-format"`
	// StatsdInterval Statsd数据发送间隔
	StatsdInterval time.Duration `flag:"statsd-interval"`
	// NSQLookupdHTTPAddresses NSQ Lookupd的HTTP地址列表
	NSQLookupdHTTPAddresses []string `flag:"lookupd-http-address" cfg:"nsqlookupd_http_addresses"`
	// NSQDHTTPAddresses NSQD的HTTP地址列表
	NSQDHTTPAddresses []string `flag:"nsqd-http-address" cfg:"nsqd_http_addresses"`
	// HTTPClientConnectTimeout HTTP客户端连接超时时间
	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout"`
	// HTTPClientRequestTimeout HTTP客户端请求超时时间
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout"`
	// HTTPClientTLSInsecureSkipVerify 是否跳过TLS证书验证
	HTTPClientTLSInsecureSkipVerify bool `flag:"http-client-tls-insecure-skip-verify"`
	// HTTPClientTLSRootCAFile TLS根证书文件
	HTTPClientTLSRootCAFile string `flag:"http-client-tls-root-ca-file"`
	// HTTPClientTLSCert TLS客户端证书文件
	HTTPClientTLSCert string `flag:"http-client-tls-cert"`
	// HTTPClientTLSKey TLS客户端密钥文件
	HTTPClientTLSKey string `flag:"http-client-tls-key"`
	// AllowConfigFromCIDR 允许访问的CIDR列表
	AllowConfigFromCIDR string `flag:"allow-config-from-cidr"`
	// NotificationHTTPEndpoint 通知服务的HTTP端点
	NotificationHTTPEndpoint string `flag:"notification-http-endpoint"`
	// ACLHTTPHeader ACL验证的HTTP头字段
	ACLHTTPHeader string `flag:"acl-http-header"`
	// AdminUsers 管理员用户列表
	AdminUsers []string `flag:"admin-user" cfg:"admin_users"`
}

// NewOptions 创建并返回一个配置了默认参数的Options指针。
// 这个函数通过设定一系列默认值，初始化一个Options对象，用于后续的配置和使用。
func NewOptions() *Options {
	return &Options{
		// 日志前缀，默认设置为"[nsqadmin] "，用于标识日志来源
		LogPrefix: "[nsqadmin] ",
		// 日志级别，默认为INFO，用于控制日志的详细程度
		LogLevel: lg.INFO,
		// HTTP监听地址，默认监听所有IP的4171端口
		HTTPAddress: "0.0.0.0:4171",
		// HTTP服务的根路径，默认为"/"
		BasePath: "/",
		// Statsd监控数据的前缀，默认使用"nsq."作为前缀
		StatsdPrefix: "nsq.%s",
		// Statsd计数器数据的格式，默认格式为"stats.counters.%s.count"
		StatsdCounterFormat: "stats.counters.%s.count",
		// Statsd Gauge数据的格式，默认格式为"stats.gauges.%s"
		StatsdGaugeFormat: "stats.gauges.%s",
		// Statsd数据推送间隔，默认为60秒一次
		StatsdInterval: 60 * time.Second,
		// HTTP客户端连接超时时间，默认为2秒
		HTTPClientConnectTimeout: 2 * time.Second,
		// HTTP客户端请求超时时间，默认为5秒
		HTTPClientRequestTimeout: 5 * time.Second,
		// 允许访问的CIDR块，默认为本地回环地址的CIDR块
		AllowConfigFromCIDR: "127.0.0.1/8",
		// 用于识别用户身份的HTTP头，默认使用"X-Forwarded-User"头
		ACLHTTPHeader: "X-Forwarded-User",
		// 管理员用户列表，默认为空列表，表示没有管理员用户
		AdminUsers: []string{},
	}
}
