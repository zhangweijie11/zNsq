package nsqd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zhangweijie11/zNsq/internal/clusterinfo"
	"github.com/zhangweijie11/zNsq/internal/dirlock"
	"github.com/zhangweijie11/zNsq/internal/http_api"
	"github.com/zhangweijie11/zNsq/internal/protocol"
	"github.com/zhangweijie11/zNsq/internal/statsd"
	"github.com/zhangweijie11/zNsq/internal/util"
	"github.com/zhangweijie11/zNsq/internal/version"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

type NSQD struct {
	ctx       context.Context    // 默认上下文
	ctxCancel context.CancelFunc // 取消方法
	sync.RWMutex
	opts                 atomic.Value             // 参数
	startTime            time.Time                // 启动时间
	topicMap             map[string]*Topic        // topic map
	exitChan             chan int                 // 退出通道
	notifyChan           chan interface{}         // 通知通道
	optsNotificationChan chan struct{}            // 参数通知通道
	dl                   *dirlock.DirLock         // 目录排它锁
	waitGroup            util.WaitGroupWrapper    // 等待组
	errValue             atomic.Value             // 错误
	lookupPeers          atomic.Value             // lookupd 客户端
	ci                   *clusterinfo.ClusterInfo // 集群
	tcpServer            *tcpServer               // tcp server
	tcpListener          net.Listener             // tcp listener
	httpListener         net.Listener             // http listener
	httpsListener        net.Listener             // https listener
	clientIDSequence     int64                    // 客户端ID
	tlsConfig            *tls.Config              // TLS配置
	clientTLSConfig      *tls.Config              // 客户端TLS配置
	poolSize             int                      // 连接池大小
	isLoading            int32                    // 是否正在加载元数据
	isExiting            int32                    // 是否正在退出
}

func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

// New 创建一个新的NSQD实例。
// 参数opts是用于配置NSQD的选项。
// 返回一个指向NSQD实例的指针和一个错误（如果有）。
func New(opts *Options) (*NSQD, error) {
	var err error

	// 确定数据路径，默认为当前工作目录。
	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
	}
	// 初始化日志记录器，如果未提供的话。
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	// 初始化NSQD实例。
	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]*Topic),
		exitChan:             make(chan int),
		notifyChan:           make(chan interface{}),
		optsNotificationChan: make(chan struct{}, 1),
		dl:                   dirlock.New(dataPath),
	}
	n.ctx, n.ctxCancel = context.WithCancel(context.Background())
	httpcli := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	n.ci = clusterinfo.New(n.logf, httpcli)

	// 初始化lookupPeers存储。
	n.lookupPeers.Store([]*lookupPeer{})

	// 应用传入的配置选项。
	n.swapOpts(opts)
	// 初始化错误存储。
	n.errValue.Store(errStore{})

	// 尝试锁定数据路径。
	err = n.dl.Lock()
	if err != nil {
		return nil, fmt.Errorf("failed to lock data-path: %v", err)
	}

	// 验证配置参数的有效性。
	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		return nil, errors.New("--max-deflate-level must be [1,9]")
	}
	if opts.ID < 0 || opts.ID >= 1024 {
		return nil, errors.New("--node-id must be [0,1024)")
	}

	// 处理TLS配置。
	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config - %s", err)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		return nil, errors.New("cannot require TLS client connections without TLS key and cert")
	}
	n.tlsConfig = tlsConfig

	// 处理客户端TLS配置。
	clientTLSConfig, err := buildClientTLSConfig(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build client TLS config - %s", err)
	}
	n.clientTLSConfig = clientTLSConfig

	// 验证认证HTTP请求方法。
	if opts.AuthHTTPRequestMethod != "post" && opts.AuthHTTPRequestMethod != "get" {
		return nil, errors.New("--auth-http-request-method must be post or get")
	}

	// 验证端到端处理延迟百分位数。
	for _, v := range opts.E2EProcessingLatencyPercentiles {
		if v <= 0 || v > 1 {
			return nil, fmt.Errorf("invalid E2E processing latency percentile: %v", v)
		}
	}

	// 日志记录NSQD启动信息。
	n.logf(LOG_INFO, version.String("nsqd"))
	n.logf(LOG_INFO, "ID: %d", opts.ID)

	// 初始化TCP服务器。
	n.tcpServer = &tcpServer{nsqd: n}
	n.tcpListener, err = net.Listen(util.TypeOfAddr(opts.TCPAddress), opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	// 初始化HTTP服务器（如果配置了地址）。
	if opts.HTTPAddress != "" {
		n.httpListener, err = net.Listen(util.TypeOfAddr(opts.HTTPAddress), opts.HTTPAddress)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
		}
	}
	// 初始化HTTPS服务器（如果配置了TLS配置和地址）。
	if n.tlsConfig != nil && opts.HTTPSAddress != "" {
		n.httpsListener, err = tls.Listen("tcp", opts.HTTPSAddress, n.tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPSAddress, err)
		}
	}
	// 设置广播端口。
	if opts.BroadcastHTTPPort == 0 {
		tcpAddr, ok := n.RealHTTPAddr().(*net.TCPAddr)
		if ok {
			opts.BroadcastHTTPPort = tcpAddr.Port
		}
	}
	if opts.BroadcastTCPPort == 0 {
		tcpAddr, ok := n.RealTCPAddr().(*net.TCPAddr)
		if ok {
			opts.BroadcastTCPPort = tcpAddr.Port
		}
	}
	// 配置Statsd前缀。
	if opts.StatsdPrefix != "" {
		var port string = fmt.Sprint(opts.BroadcastHTTPPort)
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	// 返回NSQD实例。
	return n, nil
}

// 组装 nsqd 元数据文件路径
func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "nsqd.dat")
}

// 读取指定文件的内容，如果文件不存在则返回空的字节切片
func readOrEmpty(fn string) ([]byte, error) {
	data, err := os.ReadFile(fn)
	if err != nil {
		// 检查错误是否是文件不存在
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("无法从文件中读取元数据 %s - %s", fn, err)
		}
	}

	return data, nil
}

// Metadata 当前 NSQD 的持久性元数据信息的集合
type Metadata struct {
	Topics  []TopicMetadata `json:"topics"`
	Version string          `json:"version"`
}

// TopicMetadata 主题的持久性信息
type TopicMetadata struct {
	Name     string            `json:"name"`     // 主题名称
	Paused   bool              `json:"paused"`   // 是否停用
	Channels []ChannelMetadata `json:"channels"` // 主题通道
}

// ChannelMetadata 通道的持久性信息
type ChannelMetadata struct {
	Name   string `json:"name"`
	Paused bool   `json:"paused"`
}

// LoadMetadata 加载元数据
// 该方法从元数据文件中读取配置，并根据配置初始化或更新NSQD中的主题和频道状态。
// 包括暂停和恢复主题及其频道。
func (n *NSQD) LoadMetadata() error {
	// 标记NSQD为正在加载元数据状态
	atomic.StoreInt32(&n.isLoading, 1)
	defer atomic.StoreInt32(&n.isLoading, 0)

	// 创建一个新的元数据文件路径
	fn := newMetadataFile(n.getOpts())

	// 尝试读取元数据文件，如果文件不存在则返回空数据
	data, err := readOrEmpty(fn)
	if err != nil {
		return err
	}
	if data == nil {
		return nil
	}

	// 定义一个Metadata类型变量，用于存储解析后的元数据
	var m Metadata
	// 尝试将读取的数据解析为JSON格式的元数据
	err = json.Unmarshal(data, &m)
	if err != nil {
		// 如果解析失败，返回错误信息
		return fmt.Errorf("无法解析元数据 %s - %s", fn, err)
	}

	// 遍历元数据中的所有主题
	for _, t := range m.Topics {
		// 检查主题名称是否有效
		if !protocol.IsValidTopicName(t.Name) {
			// 如果主题名称无效，记录警告日志并跳过该主题
			n.logf(LOG_WARN, "忽略创建无效的主题 %s", t.Name)
			continue
		}

		// 获取或创建NSQD中的主题实例
		topic := n.GetTopic(t.Name)
		// 如果元数据中标记主题为暂停状态，则暂停该主题
		if t.Paused {
			topic.Pause()
		}
		// 遍历主题下的所有频道
		for _, c := range t.Channels {
			// 检查频道名称是否有效
			if !protocol.IsValidChannelName(c.Name) {
				// 如果频道名称无效，记录警告日志并跳过该频道
				n.logf(LOG_WARN, "skipping creation of invalid channel %s", c.Name)
				continue
			}
			// 获取或创建主题下的频道实例
			channel := topic.GetChannel(c.Name)
			// 如果元数据中标记频道为暂停状态，则暂停该频道
			if c.Paused {
				channel.Pause()
			}
		}
		// 启动主题，以确保主题及其频道的状态与元数据一致
		topic.Start()
	}
	// 元数据加载完成，返回nil表示没有遇到错误
	return nil
}

// GetTopic 根据给定的主题名获取对应的Topic对象。
// 如果主题不存在，则创建一个新的主题。
func (n *NSQD) GetTopic(topicName string) *Topic {
	// 首先尝试读锁，假设主题已经存在
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {
		// 如果主题存在，直接返回
		return t
	}

	// 如果读锁失败，尝试写锁，准备创建新主题
	n.Lock()

	// 再次检查主题是否已经存在，避免在等待锁时其他线程创建了主题
	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}

	// 如果主题不存在，创建新主题
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}
	t = NewTopic(topicName, n, deleteCallback)
	n.topicMap[topicName] = t

	n.Unlock()

	// 记录主题创建日志
	n.logf(LOG_INFO, "TOPIC(%s): created", t.name)

	// 如果是在加载元数据时创建的主题，不进行进一步的初始化
	if atomic.LoadInt32(&n.isLoading) == 1 {
		return t
	}

	// 如果使用lookupd，调用lookupd获取channel列表并立即创建channel
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(LOG_WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}
		for _, channelName := range channelNames {
			// 不创建ephemeral类型的channel，除非有consumer client
			if strings.HasSuffix(channelName, "#ephemeral") {
				continue
			}
			t.GetChannel(channelName)
		}
	} else if len(n.getOpts().NSQLookupdTCPAddresses) > 0 {
		// 如果有配置lookupd地址但无法获取到，则记录错误日志
		n.logf(LOG_ERROR, "no available nsqlookupd to query for channels to pre-create for topic %s", t.name)
	}

	// 所有channel添加完毕后，启动主题的消息泵
	t.Start()
	return t
}

// Notify 处理通知逻辑，它根据当前nsqd的状态决定是否需要持久化元数据。
// 参数:
//
//	v: 要发送的通知数据。
//	persist: 标志位，指示是否在发送通知后持久化元数据。
func (n *NSQD) Notify(v interface{}, persist bool) {
	// 判断nsqd是否正在加载中。如果元数据在加载过程中，不应该进行持久化操作。
	loading := atomic.LoadInt32(&n.isLoading) == 1

	// 使用waitGroup.Wrap确保goroutine的生命周期管理。
	n.waitGroup.Wrap(func() {
		// 尝试退出时，直接返回。
		select {
		case <-n.exitChan:
			return
		// 向通知通道发送数据。
		case n.notifyChan <- v:
			// 如果正在加载或不需要持久化，则发送通知后直接返回。
			if loading || !persist {
				return
			}

			// 锁定nsqd实例，准备持久化元数据。
			n.Lock()
			defer n.Unlock() // 确保在函数退出前释放锁。

			// 尝试持久化元数据。
			err := n.PersistMetadata()
			if err != nil {
				// 如果持久化失败，记录错误日志。
				n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
			}
		}
	})
}

// PersistMetadata 用于持久化NSQD的元数据，以便在重启后保留主题和通道的信息。
// 该方法通过序列化当前的元数据并将其写入到特定的文件中来实现。
// 如果写入过程中发生错误，则返回相应的错误。
func (n *NSQD) PersistMetadata() error {
	// 生成新的元数据文件名
	fileName := newMetadataFile(n.getOpts())
	// 记录持久化元数据的操作日志
	n.logf(LOG_INFO, "NSQ: 持久化主题和通道到文件 %s", fileName)

	// 将元数据转换为JSON格式的数据
	data, err := json.Marshal(n.GetMetadata(false))
	if err != nil {
		return err
	}
	// 生成临时文件名
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// 写入数据到临时文件，并同步到磁盘
	err = writeSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}
	// 将临时文件重命名为最终的文件名，确保原子操作
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}
	// 此处应该进行fsync以确保数据同步到磁盘，但实际操作中可能需要权衡性能

	// 操作成功，返回nil
	return nil
}

// GetMetadata 检索 NSQ 守护程序的当前主题和通道集。如果设置了 ephemeral 标志，则即使临时主题未保存到磁盘，也会返回这些主题
func (n *NSQD) GetMetadata(ephemeral bool) *Metadata {
	meta := &Metadata{
		Version: version.Binary,
	}
	for _, topic := range n.topicMap {
		if topic.ephemeral && !ephemeral {
			continue
		}
		topicData := TopicMetadata{
			Name:   topic.name,
			Paused: topic.IsPaused(),
		}
		topic.Lock()
		for _, channel := range topic.channelMap {
			if channel.ephemeral {
				continue
			}
			topicData.Channels = append(topicData.Channels, ChannelMetadata{
				Name:   channel.name,
				Paused: channel.IsPaused(),
			})
		}
		topic.Unlock()
		meta.Topics = append(meta.Topics, topicData)
	}
	return meta
}

// 同步数据，将数据同步到磁盘中
func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

// SetHealth 设置NSQD实例的健康状态
//
// 该方法通过存储错误信息来标记当前的健康状态。如果错误非空，
// 表示NSQD实例处于不健康状态；如果错误为空，则表示实例是健康的。
//
// 参数:
//
//	err - 错误信息，用于表示健康状态。非空表示不健康，空表示健康。
func (n *NSQD) SetHealth(err error) {
	// 使用atomic.Store指针存储errStore结构体，其中包含错误信息
	// atomic.Store指针确保了并发安全
	n.errValue.Store(errStore{err: err})
}

// Main NSQD的主运行函数
// 初始化NSQD实例并设置必要的组件，然后启动所有服务器（TCP，HTTP，HTTPS）和循环任务
func (n *NSQD) Main() error {
	// 用于接收退出信号的通道
	exitCh := make(chan error)

	// 确保退出信号只被处理一次
	var once sync.Once

	// 退出函数，用于处理错误并发送退出信号
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				n.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	// 启动TCP服务器
	n.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(n.tcpListener, n.tcpServer, n.logf))
	})

	// 如果配置了HTTP监听器，则启动HTTP服务器
	if n.httpListener != nil {
		httpServer := newHTTPServer(n, false, n.getOpts().TLSRequired == TLSRequired)
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf))
		})
	}

	// 如果配置了HTTPS监听器，则启动HTTPS服务器
	if n.httpsListener != nil {
		httpsServer := newHTTPServer(n, true, true)
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf))
		})
	}

	// 启动队列扫描循环任务
	n.waitGroup.Wrap(n.queueScanLoop)
	// 启动查找表循环任务
	n.waitGroup.Wrap(n.lookupLoop)
	// 如果配置了Statsd地址，则启动Statsd循环任务
	if n.getOpts().StatsdAddress != "" {
		n.waitGroup.Wrap(n.statsdLoop)
	}

	// 等待退出信号
	err := <-exitCh
	return err
}

// IsAuthEnabled 检查是否启用了认证。
// 通过判断NSQD选项中AuthHTTPAddresses的长度来确定。
// 如果长度不为0，则表示启用了认证。
func (n *NSQD) IsAuthEnabled() bool {
	return len(n.getOpts().AuthHTTPAddresses) != 0
}

// GetHealth 获取NSQD的健康状态。
// 如果NSQD发生错误，返回错误信息；
// 否则，返回"OK"表示健康。
func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		// 当存在错误时，返回"NOK"状态及错误详情。
		return fmt.Sprintf("NOK - %s", err)
	}
	// 无错误时，返回"OK"状态。
	return "OK"
}

// GetError 获取当前NSQD实例的错误信息（如果有）。
// 通过atomic.LoadValue获取errValue中存储的错误。
func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

// IsHealthy 检查NSQD实例是否健康。
// 该方法通过检查NSQD实例的错误状态来判断其健康状况。
// 如果GetError方法返回nil，表示没有检测到错误，实例被认为是健康的。
// 返回值：
//
//	bool：如果实例健康，返回true；否则返回false。
func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

// RealTCPAddr 返回NSQD实例的TCP地址。
// 如果TCP监听器尚未初始化，则返回一个空的TCP地址。
func (n *NSQD) RealTCPAddr() net.Addr {
	if n.tcpListener == nil {
		return &net.TCPAddr{}
	}
	return n.tcpListener.Addr()
}

// RealHTTPAddr 返回NSQD实例的HTTP地址。
// 如果HTTP监听器尚未初始化，则返回一个空的TCP地址。
func (n *NSQD) RealHTTPAddr() net.Addr {
	if n.httpListener == nil {
		return &net.TCPAddr{}
	}
	return n.httpListener.Addr()
}

// RealHTTPSAddr 返回NSQD实例的HTTPS地址。
// 如果HTTPS监听器尚未初始化，则返回一个空的TCP地址。
// 返回值是一个指向net.TCPAddr的指针，以确保类型安全性。
func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	if n.httpsListener == nil {
		return &net.TCPAddr{}
	}
	return n.httpsListener.Addr().(*net.TCPAddr)
}

// GetStartTime 返回NSQD实例的启动时间。
// 该函数主要用于获取NSQD实例的运行时长等相关信息。
func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

// GetExistingTopic 通过主题名称获取已存在的 Topic 实例。
//
// 该函数尝试从 NSQD 实例的 topicMap 中查找指定名称的主题。
// 如果找到，返回该主题实例；否则，返回一个错误。
//
// 参数:
//
//	topicName - 要查找的主题名称。
//
// 返回值:
//
//	*Topic - 如果找到指定名称的主题，则返回该主题的指针。
//	error - 如果未找到指定名称的主题，则返回一个错误。
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	// 加读锁，确保并发安全地访问 topicMap。
	n.RLock()
	defer n.RUnlock()

	// 从 topicMap 中查找指定名称的主题。
	topic, ok := n.topicMap[topicName]
	if !ok {
		// 如果未找到指定名称的主题，返回错误。
		return nil, errors.New("topic does not exist")
	}

	// 找到了指定名称的主题，返回该主题实例。
	return topic, nil
}

// DeleteExistingTopic 删除已存在的主题
//
// 该函数尝试从NSQD实例的内部主题映射中删除指定的主题。如果主题不存在，
// 则返回一个错误。函数首先以只读模式锁定NSQD实例，检查主题是否存在，
// 如果存在，则删除主题并从映射中移除。
//
// 参数:
//   - topicName: 待删除的主题的名称
//
// 返回值:
//   - error: 如果主题不存在，则返回一个错误，否则没有错误。
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	// 以只读模式锁定NSQD，用于安全地访问主题映射
	n.RLock()
	// 从主题映射中查找指定的主题
	topic, ok := n.topicMap[topicName]
	// 如果主题不存在
	if !ok {
		// 解锁并返回错误
		n.RUnlock()
		return errors.New("主题不存在")
	}
	// 解锁，结束只读锁定
	n.RUnlock()

	// 删除主题的内容和状态
	topic.Delete()
	// 再次锁定NSQD，用于修改主题映射
	n.Lock()
	// 从主题映射中移除主题
	delete(n.topicMap, topicName)
	// 解锁，结束写操作
	n.Unlock()

	// 操作成功，返回nil
	return nil
}

// swapOpts 将给定的选项替换为NSQD实例的当前选项。
// 参数:
//   - opts: 指向新选项的指针。这些选项将替换NSQD当前使用的选项。
//
// 该方法不返回任何值。
func (n *NSQD) swapOpts(opts *Options) {
	// 使用传入的选项替换当前选项。
	n.opts.Store(opts)
}

// triggerOptsNotification 触发选项通知
// 该函数通过向optsNotificationChan发送一个空的struct{}{}来通知监听者有新的配置选项需要处理。
// 如果通道满，则不发送，以避免阻塞。
func (n *NSQD) triggerOptsNotification() {
	// 尝试向optsNotificationChan发送一个空的struct{}{}，如果通道空闲，发送成功；
	// 如果通道满，则默认处理（即不发送），避免阻塞当前协程。
	select {
	case n.optsNotificationChan <- struct{}{}:
	default:
	}
}

// queueScanLoop 扫描队列
// 该函数负责定期扫描NSQD实例中的所有频道，以查找可能需要重新平衡的深度消费者。
func (n *NSQD) queueScanLoop() {
	// 创建工作通道、响应通道和关闭通道
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount)
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	// 根据配置初始化工作间隔和刷新间隔的定时器
	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval)

	// 获取当前所有的频道
	channels := n.channels()
	// 调整工作池大小以匹配频道数量
	n.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C:
			// 检查工作间隔，决定是否进行下一轮扫描
			if len(channels) == 0 {
				// 如果没有频道，则跳过本次循环
				continue
			}
		case <-refreshTicker.C:
			// 按照刷新间隔更新频道列表
			channels = n.channels()
			// 调整工作池大小以匹配新的频道数量
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			// 刷新完成后继续下一轮循环
			continue
		case <-n.exitChan:
			// 如果收到退出信号，则跳出循环
			goto exit
		}

		// 确定本次扫描的频道数量
		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		// 随机选择频道进行扫描
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		// 统计扫描结果中的“脏”频道数量
		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		// 如果“脏”频道比例超过阈值，则重新进行扫描
		if float64(numDirty)/float64(num) > n.getOpts().QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	// 退出前的清理工作
	n.logf(LOG_INFO, "QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

// channels 返回所有频道的列表
// 该函数通过遍历所有主题和它们的频道来创建一个包含所有频道的切片
// 使用读锁来保证并发安全性，因为该操作只读取数据并不进行写操作
// 返回值是所有频道的指针切片，用于进一步的操作比如查询或修改频道状态
func (n *NSQD) channels() []*Channel {
	// 初始化一个空的频道切片，用于存储所有频道的指针
	var channels []*Channel

	// 加读锁，准备遍历topicMap
	n.RLock()
	// 遍历topicMap中的所有主题
	for _, t := range n.topicMap {
		// 加读锁，准备遍历channelMap
		t.RLock()
		// 遍历channelMap中的所有频道，并将它们添加到channels切片中
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		// 解锁，完成对当前主题的频道遍历
		t.RUnlock()
	}
	// 解锁，完成对所有主题的遍历
	n.RUnlock()
	// 返回包含所有频道的切片
	return channels
}

// resizePool 调整NSQD实例的队列扫描工作池大小。
// 根据当前队列的数量动态调整工作池的大小，以保持高效的处理能力。
// 参数:
// - num: 用于计算理想工作池大小的队列数量。
// - workCh: 工作池中的通道，用于分发工作。
// - responseCh: 用于接收工作完成信号的通道。
// - closeCh: 用于通知停止工作的通道。
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	// 计算理想的工作池大小，但最少为1，最多为配置允许的最大值。
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax
	}

	// 根据理想工作池大小调整实际工作池大小。
	for {
		if idealPoolSize == n.poolSize {
			break
		} else if idealPoolSize < n.poolSize {
			// 如果理想大小小于当前大小，缩小工作池。
			closeCh <- 1
			n.poolSize--
		} else {
			// 如果理想大小大于当前大小，扩大工作池。
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker 是 NSQD 的一个内部工作函数，负责处理队列扫描任务。
// 它从 workCh 中接收需要处理的 Channel，并根据处理结果通过 responseCh 发送反馈。
// 如果收到 closeCh 的关闭信号，则函数退出。
//
// 参数:
// - workCh: 一个用于接收待处理 Channel 的通道。
// - responseCh: 一个用于发送 Channel 处理结果的通道。
// - closeCh: 一个用于监听关闭信号的通道。
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		// 接收并处理 Channel
		case c := <-workCh:
			// 获取当前时间的纳秒级戳，用于队列消息的时间比较。
			now := time.Now().UnixNano()
			// 标记 Channel 是否有变动，默认为 false。
			dirty := false

			// 如果处理飞行中的队列成功，标记 Channel 有变动。
			if c.processInFlightQueue(now) {
				dirty = true
			}

			// 如果处理延迟队列成功，标记 Channel 有变动。
			if c.processDeferredQueue(now) {
				dirty = true
			}

			// 向 responseCh 发送 Channel 是否有变动的结果。
			responseCh <- dirty

		// 监听到关闭信号，退出函数。
		case <-closeCh:
			return
		}
	}
}

// Exit NSQD的Exit方法用于优雅地关闭NSQD实例
// 它确保所有的子系统和监听器都被正确关闭
func (n *NSQD) Exit() {
	// 防止多次调用Exit方法
	if !atomic.CompareAndSwapInt32(&n.isExiting, 0, 1) {
		return
	}

	// 关闭TCP监听器
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	// 关闭TCP服务器
	if n.tcpServer != nil {
		n.tcpServer.Close()
	}

	// 关闭HTTP监听器
	if n.httpListener != nil {
		n.httpListener.Close()
	}

	// 关闭HTTPS监听器
	if n.httpsListener != nil {
		n.httpsListener.Close()
	}

	// 锁定NSQD实例以保证并发安全性
	n.Lock()
	// 尝试持久化元数据
	err := n.PersistMetadata()
	if err != nil {
		// 记录持久化元数据失败的错误信息
		n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
	}
	// 通知关闭所有主题
	n.logf(LOG_INFO, "NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	// 解锁NSQD实例
	n.Unlock()

	// 通知停止所有子系统
	n.logf(LOG_INFO, "NSQ: stopping subsystems")
	close(n.exitChan)
	// 等待所有子系统完成关闭
	n.waitGroup.Wait()
	// 解锁数据泄露检测机制
	n.dl.Unlock()
	// 最后一次日志记录，表示NSQD实例已经准备好退出
	n.logf(LOG_INFO, "NSQ: bye")
	// 取消上下文
	n.ctxCancel()
}

func (n *NSQD) Context() context.Context {
	return n.ctx
}

// buildTLSConfig 构建TLS配置对象。
// 该函数根据Options中的配置信息来设置TLS连接的相关参数。
// 如果不需要TLS配置，则返回nil。
// 参数:
//
//	opts - 配置选项，包含TLS证书、密钥、客户端认证策略等信息。
//
// 返回值:
//
//	*tls.Config - TLS配置对象，如果不需要TLS则为nil。
//	error - 错误对象，如果出现错误则为非nil。
func buildTLSConfig(opts *Options) (*tls.Config, error) {
	// 如果TLS证书和密钥都未提供，则不启用TLS。
	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	// 默认TLS客户端认证策略为：如果提供了客户端证书，则验证客户端证书。
	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	// 加载服务器的X.509证书和私钥。
	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	// 根据配置信息，设置TLS客户端认证策略。
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	// 初始化TLS配置，并设置基本的配置信息。
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
	}

	// 如果指定了根CA文件，则加载并设置到TLS配置中。
	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := os.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		// 将根CA证书添加到证书池中。
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	return tlsConfig, nil
}

// buildClientTLSConfig 构建客户端的TLS配置。
// 该函数根据提供的Options配置TLS连接的最小版本，并可选地配置根证书。
// 如果指定了TLSRootCAFile选项，则会从该文件中加载并添加根证书到证书池。
// 参数:
//
//	opts (*Options): 配置选项的指针，用于设置TLS最小版本和根证书文件路径。
//
// 返回值:
//
//	(*tls.Config): 构建好的TLS配置的指针。
//	(error): 如果在读取根证书文件时发生错误，或者未能将证书添加到证书池时返回错误。
func buildClientTLSConfig(opts *Options) (*tls.Config, error) {
	// 初始化TLS配置，设置最小TLS版本。
	tlsConfig := &tls.Config{
		MinVersion: opts.TLSMinVersion,
	}

	// 如果指定了根证书文件路径，则加载根证书并配置证书池。
	if opts.TLSRootCAFile != "" {
		// 创建一个新的证书池。
		tlsCertPool := x509.NewCertPool()
		// 读取根证书文件内容。
		caCertFile, err := os.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		// 将读取的证书添加到证书池中。
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		// 将配置的证书池赋值给TLS配置。
		tlsConfig.RootCAs = tlsCertPool
	}

	// 返回构建好的TLS配置。
	return tlsConfig, nil
}
