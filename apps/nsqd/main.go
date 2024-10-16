package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc"
	"github.com/mreiferson/go-options"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"github.com/zhangweijie11/zNsq/internal/version"
	"github.com/zhangweijie11/zNsq/nsqd"
	"log"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"
)

type program struct {
	once sync.Once
	nsqd *nsqd.NSQD
}

// main函数是程序的入口点
func main() {
	// 初始化一个program类型的指针，用于后续的程序执行
	prg := &program{}
	// 使用svc.Run运行程序，监听SIGINT和SIGTERM信号，确保程序可以优雅地退出
	// 这里解释了为什么需要监听信号：为了确保程序能够在接收到中断或终止信号时，有机会执行清理资源等操作
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		// 如果程序运行中出现错误，则记录错误信息并退出程序
		// 这里解释了为什么需要处理错误：为了及时反馈程序运行状态，确保出现问题时能够快速定位
		log.Fatal("%s", err)
	}
}

// Init 初始化程序
// 它通过解析命令行参数、配置文件，初始化 nsqd 实例等步骤来完成程序的初始化。
// 参数:
//
//	env: 服务环境，目前未使用但可能保留用于未来功能。
//
// 返回值:
//
//	error: 初始化过程中如果发生错误，会返回该错误，否则返回nil。
func (p *program) Init(env svc.Environment) error {
	// 创建 nsqd 的选项实例
	opts := nsqd.NewOptions()
	// 构建并解析命令行参数
	flagSet := nsqdFlagSet(opts)
	flagSet.Parse(os.Args[1:])
	// 初始化随机数种子
	rand.Seed(time.Now().UTC().UnixNano())

	// 检查命令行中是否设置了 --version 标志。如果设置了，则打印软件版本信息并成功退出程序。
	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}

	// 初始化配置结构体
	var cfg config
	// 获取命令行中的配置文件路径
	configFile := flagSet.Lookup("config").Value.String()
	// 如果配置文件路径不为空，则尝试解析配置文件
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			// 如果解析失败，记录错误并退出
			logFatal("无法加载配置文件 %s - %s", configFile, err)
		}
	}
	// 校验配置
	cfg.Validate()

	// 将命令行参数、配置文件参数都解析绑定到 opts 上
	options.Resolve(opts, flagSet, cfg)

	// 实例化 nsqd
	nsqd, err := nsqd.New(opts)
	if err != nil {
		// 如果实例化失败，记录错误并退出
		logFatal("无法实例化 nsqd - %s", err)
	}

	// 将 nsqd 实例绑定到程序结构体
	p.nsqd = nsqd

	// 初始化成功，返回nil
	return nil
}

// Start 启动 nsqd 服务
// 该函数负责加载元数据、持久化元数据并启动 nsqd 主循环
// 参数: 无
// 返回值: error, 在此示例中不会返回错误，实际使用可能根据实际情况进行错误处理
func (p *program) Start() error {
	// 加载元数据，如果失败则记录错误并退出
	err := p.nsqd.LoadMetadata()
	if err != nil {
		logFatal("failed to load metadata - %s", err)
	}

	// 持久化元数据，如果失败则记录错误并退出
	err = p.nsqd.PersistMetadata()
	if err != nil {
		logFatal("failed to persist metadata - %s", err)
	}

	// 启动 nsqd 主循环
	// 使用 goroutine 以异步方式运行 nsqd.Main，以便 Start 函数可以非阻塞地返回
	go func() {
		err := p.nsqd.Main()
		if err != nil {
			// 如果 Main 函数出现错误，停止 nsqd 并退出程序
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

// 错误日志输出
func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqd] ", f, args...)
}

// Stop 优雅地停止程序
//
// 通过调用 nsqd 实例的 Exit 方法来确保所有资源被正确释放
// 没有参数需要说明，因为函数使用了 .Once 设计模式，保证 nsqd 只被正确关闭一次
// 返回值：在关闭过程中不会返回错误，因为 nsqd 的 Exit 方法设计为没有返回值
func (p *program) Stop() error {
	p.once.Do(func() {
		p.nsqd.Exit()
	})
	return nil
}

// Handle 用于处理操作系统的信号
// 该方法主要目的是为了优雅地停止或处理对外服务
// 当接收到指定的信号时，该方法将返回一个错误，指示需要停止服务
// 参数:
//
//	s os.Signal: 操作系统发送的信号
//
// 返回值:
//
//	error: 如果信号处理导致服务需要停止，则返回svc.ErrStop错误
func (p *program) Handle(s os.Signal) error {
	return svc.ErrStop
}

// Context 返回nsqd的上下文对象
// 通过此方法可以获取program实例所在nsqd实例的上下文信息
func (p *program) Context() context.Context {
	return p.nsqd.Context()
}
