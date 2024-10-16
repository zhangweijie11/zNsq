package nsqadmin

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/zhangweijie11/zNsq/internal/http_api"
	"github.com/zhangweijie11/zNsq/internal/util"
	"github.com/zhangweijie11/zNsq/internal/version"
)

// NSQAdmin 结构体封装了 NSQAdmin 的状态和行为。
// 它使用 sync.RWMutex 来处理并发访问，确保数据的一致性。
// 该结构体还管理了 HTTP 监听器、工作等待组、管理操作通知、Graphite 计量URL和HTTP客户端的TLS配置。
type NSQAdmin struct {
	sync.RWMutex                              // 用于并发控制，保护结构体内的变量
	opts                atomic.Value          // 原子性值，用于存储配置选项，支持并发访问
	httpListener        net.Listener          // HTTP 监听器，用于处理 incoming HTTP 请求
	waitGroup           util.WaitGroupWrapper // 工作等待组，用于等待所有工作完成后再继续执行
	notifications       chan *AdminAction     // 通知通道，用于传递管理操作
	graphiteURL         *url.URL              // Graphite 计量URL，用于记录和监控
	httpClientTLSConfig *tls.Config           // HTTP 客户端的 TLS 配置，用于安全通信
}

// New 创建一个新的NSQAdmin实例。
// 参数:
//
//	opts (*Options): 配置选项的指针。
//
// 返回值:
//
//	(*NSQAdmin): NSQAdmin实例的指针。
//	(error): 错误，如果创建失败。
func New(opts *Options) (*NSQAdmin, error) {
	// 如果没有提供日志记录器，则使用默认配置创建一个。
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	// 初始化NSQAdmin实例。
	n := &NSQAdmin{
		notifications: make(chan *AdminAction),
	}
	n.swapOpts(opts)

	// 至少需要一个nsqd或nsqlookupd的地址。
	if len(opts.NSQDHTTPAddresses) == 0 && len(opts.NSQLookupdHTTPAddresses) == 0 {
		return nil, errors.New("--nsqd-http-address or --lookupd-http-address required")
	}

	// 不能同时指定nsqd和nsqlookupd的地址。
	if len(opts.NSQDHTTPAddresses) != 0 && len(opts.NSQLookupdHTTPAddresses) != 0 {
		return nil, errors.New("use --nsqd-http-address or --lookupd-http-address not both")
	}

	// 如果指定了TLS证书但没有指定密钥，或者反之，返回错误。
	if opts.HTTPClientTLSCert != "" && opts.HTTPClientTLSKey == "" {
		return nil, errors.New("--http-client-tls-key must be specified with --http-client-tls-cert")
	}

	if opts.HTTPClientTLSKey != "" && opts.HTTPClientTLSCert == "" {
		return nil, errors.New("--http-client-tls-cert must be specified with --http-client-tls-key")
	}

	// 配置TLS连接。
	n.httpClientTLSConfig = &tls.Config{
		InsecureSkipVerify: opts.HTTPClientTLSInsecureSkipVerify,
	}
	if opts.HTTPClientTLSCert != "" && opts.HTTPClientTLSKey != "" {
		cert, err := tls.LoadX509KeyPair(opts.HTTPClientTLSCert, opts.HTTPClientTLSKey)
		if err != nil {
			return nil, fmt.Errorf("failed to LoadX509KeyPair %s, %s - %s",
				opts.HTTPClientTLSCert, opts.HTTPClientTLSKey, err)
		}
		n.httpClientTLSConfig.Certificates = []tls.Certificate{cert}
	}
	if opts.HTTPClientTLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := os.ReadFile(opts.HTTPClientTLSRootCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read TLS root CA file %s - %s",
				opts.HTTPClientTLSRootCAFile, err)
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, fmt.Errorf("failed to AppendCertsFromPEM %s", opts.HTTPClientTLSRootCAFile)
		}
		n.httpClientTLSConfig.RootCAs = tlsCertPool
	}

	// 验证nsqlookupd地址的有效性。
	for _, address := range opts.NSQLookupdHTTPAddresses {
		_, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve --lookupd-http-address (%s) - %s", address, err)
		}
	}

	// 验证nsqd地址的有效性。
	for _, address := range opts.NSQDHTTPAddresses {
		_, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve --nsqd-http-address (%s) - %s", address, err)
		}
	}

	// 配置Graphite代理。
	if opts.ProxyGraphite {
		url, err := url.Parse(opts.GraphiteURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse --graphite-url (%s) - %s", opts.GraphiteURL, err)
		}
		n.graphiteURL = url
	}

	// 验证允许配置的CIDR块。
	if opts.AllowConfigFromCIDR != "" {
		_, _, err := net.ParseCIDR(opts.AllowConfigFromCIDR)
		if err != nil {
			return nil, fmt.Errorf("failed to parse --allow-config-from-cidr (%s) - %s", opts.AllowConfigFromCIDR, err)
		}
	}

	// 标准化基础路径。
	opts.BasePath = normalizeBasePath(opts.BasePath)

	// 记录NSQAdmin启动信息。
	n.logf(LOG_INFO, version.String("nsqadmin"))

	// 创建HTTP监听器。
	var err error
	n.httpListener, err = net.Listen("tcp", n.getOpts().HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", n.getOpts().HTTPAddress, err)
	}

	// 返回初始化的NSQAdmin实例。
	return n, nil
}

// normalizeBasePath 标准化路径字符串。
// 该函数确保路径字符串以斜杠 (/) 开头，并以斜杠结尾。
// 如果输入的路径字符串为空，函数将返回 "/"。
// 参数:
//
//	p - 代表一个路径字符串。
//
// 返回值:
//
//	标准化后的路径字符串。
func normalizeBasePath(p string) string {
	// 当路径字符串为空时，返回 "/"
	if len(p) == 0 {
		return "/"
	}
	// 如果路径字符串的第一个字符不是斜杠，则添加斜杠到路径字符串的开头
	if p[0] != '/' {
		p = "/" + p
	}
	// 使用 path.Clean 函数简化路径字符串，移除重复的斜杠和 "."、".."
	// 这样可以确保路径字符串的规范性和唯一性
	return path.Clean(p)
}

// getOpts 返回当前生效的Options实例
func (n *NSQAdmin) getOpts() *Options {
	return n.opts.Load().(*Options)
}

// swapOpts 交换NSQAdmin的Options配置
func (n *NSQAdmin) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

// RealHTTPAddr 返回NSQAdmin HTTP监听器的TCP地址信息
func (n *NSQAdmin) RealHTTPAddr() *net.TCPAddr {
	return n.httpListener.Addr().(*net.TCPAddr)
}

// handleAdminActions 处理管理操作，比如发送通知到指定的HTTP端点
func (n *NSQAdmin) handleAdminActions() {
	for action := range n.notifications {
		// 将操作序列化为JSON格式
		content, err := json.Marshal(action)
		if err != nil {
			n.logf(LOG_ERROR, "failed to serialize admin action - %s", err)
			continue
		}

		// 创建一个HTTP客户端，配置超时时间
		httpclient := &http.Client{
			Transport: http_api.NewDeadlineTransport(n.getOpts().HTTPClientConnectTimeout, n.getOpts().HTTPClientRequestTimeout),
		}

		// 日志记录：准备发送通知到指定的HTTP端点
		n.logf(LOG_INFO, "POSTing notification to %s", n.getOpts().NotificationHTTPEndpoint)

		// 发送HTTP POST请求
		resp, err := httpclient.Post(n.getOpts().NotificationHTTPEndpoint,
			"application/json", bytes.NewBuffer(content))
		if err != nil {
			n.logf(LOG_ERROR, "failed to POST notification - %s", err)
			continue
		}

		// 关闭响应体
		resp.Body.Close()
	}
}

// Main NSQAdmin的Main方法定义了NSQAdmin实例的主运行逻辑。
// 它通过监听HTTP请求和处理管理操作来执行NSQAdmin的职责。
// Main方法确保了程序的正确启动和优雅退出。
func (n *NSQAdmin) Main() error {
	// 创建一个通道，用于在程序的任何部分报告错误，从而触发退出。
	exitCh := make(chan error)

	// sync.Once确保每个错误只被处理一次，避免重复退出。
	var once sync.Once

	// exitFunc是一个回调函数，用于在程序的不同部分中统一错误处理和退出机制。
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				// 记录致命错误，以便于问题追踪和诊断。
				n.logf(LOG_FATAL, "%s", err)
			}
			// 通过exitCh通道发送错误，触发程序退出。
			exitCh <- err
		})
	}

	// 创建一个新的HTTPServer实例，该实例配置了NSQAdmin实例n的状态和设置。
	httpServer := NewHTTPServer(n)

	// 使用waitGroup包装启动HTTP服务，以便在Main函数退出前确保所有启动的goroutine都已完成。
	n.waitGroup.Wrap(func() {
		// 启动HTTP服务，并将错误处理委托给exitFunc。
		exitFunc(http_api.Serve(n.httpListener, http_api.CompressHandler(httpServer), "HTTP", n.logf))
	})

	// 使用waitGroup包装处理管理操作，保证管理操作的启动和执行。
	n.waitGroup.Wrap(n.handleAdminActions)

	// 接收第一个报告的错误，这将决定程序的退出原因。
	err := <-exitCh
	return err
}

// Exit NSQAdmin的Exit函数用于优雅地关闭NSQAdmin实例。
// 它按照以下步骤清理资源：
// 1. 关闭HTTP监听器，如果已启动的话。
// 2. 关闭通知通道，阻止新通知的接收。
// 3. 等待所有的worker完成当前的任务，确保所有正在进行的操作都已完成。
func (n *NSQAdmin) Exit() {
	// 如果HTTP监听器已经启动，则关闭它。
	// 这可以释放监听的端口资源，并允许其他依赖该端口的服务启动。
	if n.httpListener != nil {
		n.httpListener.Close()
	}

	// 关闭通知通道，阻止接收新的通知。
	// 这是清理资源的重要步骤，确保没有新的数据处理请求会被发送到这个实例。
	close(n.notifications)

	// 等待所有正在运行的worker完成它们的任务。
	// 这确保了所有正在进行的操作都有机会完成，避免数据丢失或不一致。
	n.waitGroup.Wait()
}
