package nsqlookupd

import (
	"fmt"
	"github.com/zhangweijie11/zNsq/internal/http_api"
	"github.com/zhangweijie11/zNsq/internal/protocol"
	"github.com/zhangweijie11/zNsq/internal/util"
	"github.com/zhangweijie11/zNsq/internal/version"
	"log"
	"net"
	"os"
	"sync"
)

// NSQLookupd 结构体是 NSQ Lookup 服务的服务器端实例的核心表示。
// 它包含同步机制、选项配置、TCP 和 HTTP 监听器、TCP 服务器、等待组和注册表数据库。
type NSQLookupd struct {
	sync.RWMutex                       // 提供读写锁机制，用于并发控制。
	opts         *Options              // Lookupd 的配置选项。
	tcpListener  net.Listener          // TCP 监听器，用于监听 TCP 连接请求。
	httpListener net.Listener          // HTTP 监听器，用于监听 HTTP 连接请求。
	tcpServer    *tcpServer            // TCP 服务器实例，处理 TCP 连接上的数据传输。
	waitGroup    util.WaitGroupWrapper // 等待组包装器，用于管理并发任务的完成。
	DB           *RegistrationDB       // 注册表数据库，存储 NSQ 话题和生产者的信息。
}

// New 创建一个新的NSQLookupd实例。
// opts 是用于配置NSQLookupd的选项。
// 返回*NSQLookupd实例和错误，如果创建过程中出现错误，则返回nil和错误信息。
func New(opts *Options) (*NSQLookupd, error) {
	var err error

	// 如果未提供日志记录器，则使用默认配置创建一个新的日志记录器
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	// 初始化NSQLookupd实例
	l := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}

	// 记录nsqlookupd的版本信息
	l.logf(LOG_INFO, version.String("nsqlookupd"))

	// 初始化TCP服务器和监听器
	l.tcpServer = &tcpServer{nsqlookupd: l}
	l.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}

	// 初始化HTTP服务器和监听器
	l.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
	}

	// 成功创建NSQLookupd实例并返回
	return l, nil
}

// Main 是 NSQLookupd 结构体的成员函数，用于启动并管理 NSQLookupd 的主要服务流程。
// 它通过监听 TCP 和 HTTP 请求，并在接收到错误时优雅地退出应用程序。
func (l *NSQLookupd) Main() error {
	// 初始化一个错误退出通道，用于向主协程报告错误。
	exitCh := make(chan error)

	// sync.Once 确保错误只被处理一次。
	var once sync.Once

	// exitFunc 用于处理错误并关闭程序。
	// 它确保无论何时何地发生错误，都可以被正确处理且仅处理一次。
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				// 记录致命错误
				l.logf(LOG_FATAL, "%s", err)
			}
			// 向退出通道发送错误
			exitCh <- err
		})
	}

	// 启动 TCP 服务器，并将其错误处理委托给 exitFunc。
	l.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(l.tcpListener, l.tcpServer, l.logf))
	})

	// 创建并启动 HTTP 服务器实例，并将其错误处理委托给 exitFunc。
	httpServer := newHTTPServer(l)
	l.waitGroup.Wrap(func() {
		exitFunc(http_api.Serve(l.httpListener, httpServer, "HTTP", l.logf))
	})

	// 接收并返回第一个报告的错误
	err := <-exitCh
	return err
}

func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	return l.httpListener.Addr().(*net.TCPAddr)
}

// Exit 优雅地关闭NSQLookupd服务
// 该函数先关闭TCP监听器和服务器，然后等待所有正在进行的操作完成
func (l *NSQLookupd) Exit() {
	// 关闭TCP监听器，阻止新的连接
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	// 关闭TCP服务器，主动关闭已接受的连接
	if l.tcpServer != nil {
		l.tcpServer.Close()
	}

	// 关闭HTTP监听器，阻止新的HTTP请求
	if l.httpListener != nil {
		l.httpListener.Close()
	}

	// 等待所有正在进行的操作完成
	l.waitGroup.Wait()
}
