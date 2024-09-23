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
	"github.com/zhangweijie11/zNsq/internal/lg"
	"github.com/zhangweijie11/zNsq/internal/version"
	"github.com/zhangweijie11/zNsq/nsqlookupd"
)

type program struct {
	once       sync.Once
	nsqlookupd *nsqlookupd.NSQLookupd
}

// main函数是程序的入口点
func main() {
	// 初始化program类型的指针
	prg := &program{}
	// 启动程序，并监听中断信号SIGINT和终止信号SIGTERM
	// 这里解释了为什么需要监听这两个信号：为了能够优雅地退出程序，处理任何未完成的事务
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		// 如果运行中出现错误，则记录错误信息并退出程序
		// 这里解释了为什么直接退出而不是返回错误：因为这是main函数，程序的入口，错误发生时应立即终止
		logFatal("%s", err)
	}
}

// Init 初始化
func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

// Start 启动程序
func (p *program) Start() error {
	// 初始化NSQ Lookupd选项
	opts := nsqlookupd.NewOptions()

	// 创建并解析命令行标志集
	flagSet := nsqlookupdFlagSet(opts)
	flagSet.Parse(os.Args[1:])

	// 检查是否显示版本信息
	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqlookupd"))
		os.Exit(0)
	}

	// 初始化配置结构体
	var cfg config
	// 获取配置文件路径
	configFile := flagSet.Lookup("config").Value.String()
	// 如果提供了配置文件，则解析配置文件
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	// 校验配置
	cfg.Validate()

	// 解析并应用命令行标志和配置文件选项
	options.Resolve(opts, flagSet, cfg)
	// 创建新的NSQ Lookupd实例
	nsqlookupd, err := nsqlookupd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqlookupd", err)
	}
	// 保存NSQ Lookupd实例到程序结构体
	p.nsqlookupd = nsqlookupd

	// 在新的goroutine中运行NSQ Lookupd主循环
	go func() {
		err := p.nsqlookupd.Main()
		if err != nil {
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.nsqlookupd.Exit()
	})
	return nil
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqlookupd] ", f, args...)
}

// nsqlookupdFlagSet 构建并返回一个针对nsqlookupd的命令行参数解析器。
// 它根据nsqlookupd的配置选项opts来设置一系列可配置的命令行标志。
// 返回的FlagSet对象可以用于解析命令行参数，以便初始化nsqlookupd的服务配置。
func nsqlookupdFlagSet(opts *nsqlookupd.Options) *flag.FlagSet {
	// 创建一个新的命令行参数解析器，命名为"nsqlookupd"，并在遇到错误时默认退出。
	flagSet := flag.NewFlagSet("nsqlookupd", flag.ExitOnError)

	// 配置文件路径标志
	flagSet.String("config", "", "path to config file")
	// 版本打印标志
	flagSet.Bool("version", false, "print version string")

	// 日志级别标志，使用opts中的LogLevel字段作为默认值
	logLevel := opts.LogLevel
	flagSet.Var(&logLevel, "log-level", "set log verbosity: debug, info, warn, error, or fatal")
	// 日志前缀标志
	flagSet.String("log-prefix", "[nsqlookupd] ", "log message prefix")
	// 过时的日志详细度标志，建议使用--log-level
	flagSet.Bool("verbose", false, "[deprecated] has no effect, use --log-level")

	// TCP监听地址标志
	flagSet.String("tcp-address", opts.TCPAddress, "<addr>:<port> to listen on for TCP clients")
	// HTTP监听地址标志
	flagSet.String("http-address", opts.HTTPAddress, "<addr>:<port> to listen on for HTTP clients")
	// 本节点广播地址标志，默认为操作系统主机名
	flagSet.String("broadcast-address", opts.BroadcastAddress, "address of this lookupd node, (default to the OS hostname)")

	// 生产者非活跃超时时间标志
	flagSet.Duration("inactive-producer-timeout", opts.InactiveProducerTimeout, "duration of time a producer will remain in the active list since its last ping")
	// 生产者墓碑状态持续时间标志
	flagSet.Duration("tombstone-lifetime", opts.TombstoneLifetime, "duration of time a producer will remain tombstoned if registration remains")

	// 返回构建好的命令行参数解析器
	return flagSet
}
