package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc"
	"github.com/mreiferson/go-options"
	"github.com/zhangweijie11/zNsq/internal/app"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"github.com/zhangweijie11/zNsq/internal/version"
	"github.com/zhangweijie11/zNsq/nsqadmin"
)

// nsqadminFlagSet 构建并返回一个用于解析nsqadmin命令行参数的flag.FlagSet对象。
// 它初始化了nsqadmin的各种配置选项，使得这些配置可以通过命令行参数进行调整。
// 参数opts是指向nsqadmin.Options结构体的指针，用于存储命令行参数的值。
// 该函数返回一个*flag.FlagSet类型的指针。
func nsqadminFlagSet(opts *nsqadmin.Options) *flag.FlagSet {
	// 创建一个新的flag.FlagSet对象，命名为"nsqadmin"，错误处理方式为ExitOnError。
	flagSet := flag.NewFlagSet("nsqadmin", flag.ExitOnError)

	// 配置文件路径
	flagSet.String("config", "", "path to config file")
	// 版本打印开关
	flagSet.Bool("version", false, "print version string")

	// 日志级别变量初始化
	logLevel := opts.LogLevel
	// 设置日志级别选项
	flagSet.Var(&logLevel, "log-level", "set log verbosity: debug, info, warn, error, or fatal")
	// 日志前缀
	flagSet.String("log-prefix", "[nsqadmin] ", "log message prefix")
	// verbose选项（已弃用）
	flagSet.Bool("verbose", false, "[deprecated] has no effect, use --log-level")

	// HTTP监听地址
	flagSet.String("http-address", opts.HTTPAddress, "<addr>:<port> to listen on for HTTP clients")
	// URL基础路径
	flagSet.String("base-path", opts.BasePath, "URL base path")
	// 开发静态文件目录
	flagSet.String("dev-static-dir", opts.DevStaticDir, "(development use only)")

	// Graphite HTTP地址
	flagSet.String("graphite-url", opts.GraphiteURL, "graphite HTTP address")
	// 代理Graphite HTTP请求开关
	flagSet.Bool("proxy-graphite", false, "proxy HTTP requests to graphite")

	// statsd计数器统计键格式
	flagSet.String("statsd-counter-format", opts.StatsdCounterFormat, "The counter stats key formatting applied by the implementation of statsd. If no formatting is desired, set this to an empty string.")
	// statsd仪表盘统计键格式
	flagSet.String("statsd-gauge-format", opts.StatsdGaugeFormat, "The gauge stats key formatting applied by the implementation of statsd. If no formatting is desired, set this to an empty string.")
	// statsd键前缀
	flagSet.String("statsd-prefix", opts.StatsdPrefix, "prefix used for keys sent to statsd (%s for host replacement, must match nsqd)")
	// statsd推送时间间隔
	flagSet.Duration("statsd-interval", opts.StatsdInterval, "time interval nsqd is configured to push to statsd (must match nsqd)")

	// 管理动作通知HTTP终端
	flagSet.String("notification-http-endpoint", "", "HTTP endpoint (fully qualified) to which POST notifications of admin actions will be sent")

	// HTTP客户端连接超时时间
	flagSet.Duration("http-client-connect-timeout", opts.HTTPClientConnectTimeout, "timeout for HTTP connect")
	// HTTP客户端请求超时时间
	flagSet.Duration("http-client-request-timeout", opts.HTTPClientRequestTimeout, "timeout for HTTP request")

	// HTTP客户端TLS验证跳过开关
	flagSet.Bool("http-client-tls-insecure-skip-verify", false, "configure the HTTP client to skip verification of TLS certificates")
	// HTTP客户端TLS根CA文件路径
	flagSet.String("http-client-tls-root-ca-file", "", "path to CA file for the HTTP client")
	// HTTP客户端TLS证书文件路径
	flagSet.String("http-client-tls-cert", "", "path to certificate file for the HTTP client")
	// HTTP客户端TLS密钥文件路径
	flagSet.String("http-client-tls-key", "", "path to key file for the HTTP client")

	// 允许配置的CIDR
	flagSet.String("allow-config-from-cidr", opts.AllowConfigFromCIDR, "A CIDR from which to allow HTTP requests to the /config endpoint")
	// ACL HTTP头
	flagSet.String("acl-http-header", opts.ACLHTTPHeader, "HTTP header to check for authenticated admin users")

	// nsqlookupd HTTP地址数组
	nsqlookupdHTTPAddresses := app.StringArray{}
	flagSet.Var(&nsqlookupdHTTPAddresses, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	// nsqd HTTP地址数组
	nsqdHTTPAddresses := app.StringArray{}
	flagSet.Var(&nsqdHTTPAddresses, "nsqd-http-address", "nsqd HTTP address (may be given multiple times)")
	// 管理用户数组
	adminUsers := app.StringArray{}
	flagSet.Var(&adminUsers, "admin-user", "admin user (may be given multiple times; if specified, only these users will be able to perform privileged actions; acl-http-header is used to determine the authenticated user)")

	// 返回构建好的flag.FlagSet对象
	return flagSet
}

type program struct {
	once     sync.Once
	nsqadmin *nsqadmin.NSQAdmin
}

// main函数是程序的入口点。
// 它创建一个program实例，然后在svc.Run中运行该实例，等待程序终止。
func main() {
	prg := &program{}
	// 使用svc.Run运行程序，处理SIGINT和SIGTERM信号
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		// 如果有错误，记录错误信息并退出
		logFatal("%s", err)
	}
}

// Init Init函数在程序初始化时调用。
// 对于Windows服务，它会更改当前工作目录为可执行文件所在的目录。
// 对于非服务环境，直接返回nil。
func (p *program) Init(env svc.Environment) error {
	// 如果是Windows服务
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		// 尝试更改工作目录
		return os.Chdir(dir)
	}
	// 对于非服务环境，直接返回nil
	return nil
}

// Start Start函数在程序启动时调用。
// 它初始化配置，解析命令行参数，并启动nsqadmin服务。
func (p *program) Start() error {
	// 初始化nsqadmin选项
	opts := nsqadmin.NewOptions()

	// 解析命令行标志
	flagSet := nsqadminFlagSet(opts)
	flagSet.Parse(os.Args[1:])

	// 如果版本标志被设置，打印版本信息并退出
	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqadmin"))
		os.Exit(0)
	}

	// 初始化配置
	var cfg config
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		// 解析配置文件
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			// 如果配置文件解析失败，记录错误并退出
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	// 验证配置
	cfg.Validate()

	// 解析配置选项
	options.Resolve(opts, flagSet, cfg)
	// 创建nsqadmin实例
	nsqadmin, err := nsqadmin.New(opts)
	if err != nil {
		// 如果实例化nsqadmin失败，记录错误并退出
		logFatal("failed to instantiate nsqadmin - %s", err)
	}
	p.nsqadmin = nsqadmin

	// 在后台goroutine中运行nsqadmin.Main
	go func() {
		err := p.nsqadmin.Main()
		if err != nil {
			// 如果nsqadmin运行出错，停止程序并退出
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

// Stop Stop函数在程序停止时调用。
// 它调用nsqadmin的Exit方法来停止服务。
func (p *program) Stop() error {
	// 确保nsqadmin只停止一次
	p.once.Do(func() {
		p.nsqadmin.Exit()
	})
	return nil
}

// logFatal是一个辅助函数，用于记录错误并退出程序。
// 它接收一个格式字符串和可变数量的参数，将它们组合成错误信息并记录。
func logFatal(f string, args ...interface{}) {
	// 记录致命错误
	lg.LogFatal("[nsqadmin] ", f, args...)
}
