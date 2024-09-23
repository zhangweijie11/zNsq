package nsqd

import (
	"github.com/zhangweijie11/zNsq/internal/quantile"
	"runtime"
	"sort"
	"sync/atomic"
)

type ClientStats interface {
	String() string
}

type Stats struct {
	Topics    []TopicStats
	Producers []ClientStats
}

type TopicStats struct {
	TopicName            string           `json:"topic_name"`
	Channels             []ChannelStats   `json:"channels"`
	Depth                int64            `json:"depth"`
	BackendDepth         int64            `json:"backend_depth"`
	MessageCount         uint64           `json:"message_count"`
	MessageBytes         uint64           `json:"message_bytes"`
	Paused               bool             `json:"paused"`
	E2eProcessingLatency *quantile.Result `json:"e2e_processing_latency"`
}

type ChannelStats struct {
	ChannelName          string           `json:"channel_name"`
	Depth                int64            `json:"depth"`
	BackendDepth         int64            `json:"backend_depth"`
	InFlightCount        int              `json:"in_flight_count"`
	DeferredCount        int              `json:"deferred_count"`
	MessageCount         uint64           `json:"message_count"`
	RequeueCount         uint64           `json:"requeue_count"`
	TimeoutCount         uint64           `json:"timeout_count"`
	ClientCount          int              `json:"client_count"`
	Clients              []ClientStats    `json:"clients"`
	Paused               bool             `json:"paused"`
	E2eProcessingLatency *quantile.Result `json:"e2e_processing_latency"`
}

type TopicsByName struct {
	Topics
}

type Topics []*Topic

// Less 比较索引 i 和 j 处的主题名称大小。
// 这个方法用于排序算法中，确保按照名称顺序进行排序。
// 参数:
//
//	i, j - 待比较的两个索引位置。
//
// 返回值:
//
//	如果索引 i 处的主题名称小于索引 j 处的主题名称，则返回 true；否则返回 false。
func (t TopicsByName) Less(i, j int) bool {
	return t.Topics[i].name < t.Topics[j].name
}

// Len 返回Topics实例中元素的数量
// 该方法主要用于获取Topics实例的长度，以便在迭代或操作Topics实例时知道其中包含多少元素
func (t Topics) Len() int { return len(t) }

// Swap 交换索引 i 和 j 处的主题。
// 此方法允许对 Topics 结构中的元素进行位置交换，
// 用于诸如排序或反转顺序的操作。
// 参数:
//
//	i - 要交换的第一个主题的索引。
//	j - 要交换的第二个主题的索引。
func (t Topics) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

type ChannelsByName struct {
	Channels
}

type Channels []*Channel

// Less ChannelsByName.Less 比较Channels切片中的两个元素的name字段大小
// 该方法用于排序算法中，确定是否需要交换两个元素的位置
// 参数:
//
//	i, j: 待比较的两个元素的索引
//
// 返回值:
//
//	如果第i个元素的name字段小于第j个元素的name字段，则返回true；否则返回false
func (c ChannelsByName) Less(i, j int) bool {
	return c.Channels[i].name < c.Channels[j].name
}

func (c Channels) Len() int      { return len(c) }
func (c Channels) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

// NewChannelStats 创建并返回一个ChannelStats实例，该实例包含了关于通道的当前统计信息。
// 参数c是指向Channel的指针，用于收集统计信息的通道。
// 参数clients是一个ClientStats的切片，包含通道中每个客户端的统计信息。
// 参数clientCount是当前连接到通道的客户端数量。
func NewChannelStats(c *Channel, clients []ClientStats, clientCount int) ChannelStats {
	// 加锁以确保对inFlightMessages的访问是线程安全的
	c.inFlightMutex.Lock()
	inflight := len(c.inFlightMessages) // 获取并记录正在处理中的消息数量
	c.inFlightMutex.Unlock()

	// 加锁以确保对deferredMessages的访问是线程安全的
	c.deferredMutex.Lock()
	deferred := len(c.deferredMessages) // 获取并记录延迟处理的消息数量
	c.deferredMutex.Unlock()

	// 返回一个填充好的ChannelStats结构体
	// 包含通道的基本信息、消息深度、后台消息深度、正在处理中的消息数量、
	// 延迟消息数量、已发送消息总数、已重新排队消息数量、超时消息数量、
	// 当前客户端数量、每个客户端的详细统计信息以及通道是否暂停状态。
	// 同时，还包含了端到端的处理延迟信息。
	return ChannelStats{
		ChannelName:   c.name,
		Depth:         c.Depth(),
		BackendDepth:  c.backend.Depth(),
		InFlightCount: inflight,
		DeferredCount: deferred,
		MessageCount:  atomic.LoadUint64(&c.messageCount),
		RequeueCount:  atomic.LoadUint64(&c.requeueCount),
		TimeoutCount:  atomic.LoadUint64(&c.timeoutCount),
		ClientCount:   clientCount,
		Clients:       clients,
		Paused:        c.IsPaused(),

		E2eProcessingLatency: c.e2eProcessingLatencyStream.Result(),
	}
}

// GetStats 提供NSQD的统计信息，包括主题和频道的统计，可选地包括客户端统计。
// 参数:
//
//	topic: 指定获取统计信息的主题名。如果为空，则返回所有主题的统计信息。
//	channel: 指定获取统计信息的频道名。如果为空，则返回所有频道的统计信息。
//	includeClients: 控制是否包括客户端统计信息。
//
// 返回值:
//
//	Stats: 包含请求的统计信息的对象。
func (n *NSQD) GetStats(topic string, channel string, includeClients bool) Stats {
	var stats Stats

	// 加读锁，确保并发安全
	n.RLock()
	var realTopics []*Topic
	// 根据输入参数决定需要统计的主题
	if topic == "" {
		realTopics = make([]*Topic, 0, len(n.topicMap))
		for _, t := range n.topicMap {
			realTopics = append(realTopics, t)
		}
	} else if val, exists := n.topicMap[topic]; exists {
		realTopics = []*Topic{val}
	} else {
		n.RUnlock()
		return stats
	}
	n.RUnlock()
	// 对主题列表按名称排序
	sort.Sort(TopicsByName{realTopics})

	topics := make([]TopicStats, 0, len(realTopics))

	for _, t := range realTopics {
		t.RLock()
		var realChannels []*Channel
		// 根据输入参数决定需要统计的频道
		if channel == "" {
			realChannels = make([]*Channel, 0, len(t.channelMap))
			for _, c := range t.channelMap {
				realChannels = append(realChannels, c)
			}
		} else if val, exists := t.channelMap[channel]; exists {
			realChannels = []*Channel{val}
		} else {
			t.RUnlock()
			continue
		}
		t.RUnlock()
		// 对频道列表按名称排序
		sort.Sort(ChannelsByName{realChannels})
		channels := make([]ChannelStats, 0, len(realChannels))
		for _, c := range realChannels {
			var clients []ClientStats
			var clientCount int
			c.RLock()
			if includeClients {
				clients = make([]ClientStats, 0, len(c.clients))
				for _, client := range c.clients {
					clients = append(clients, client.Stats(topic))
				}
			}
			clientCount = len(c.clients)
			c.RUnlock()
			channels = append(channels, NewChannelStats(c, clients, clientCount))
		}
		topics = append(topics, NewTopicStats(t, channels))
	}
	stats.Topics = topics

	if includeClients {
		var producerStats []ClientStats
		// 遍历TCP服务器的连接，收集生产者统计信息
		n.tcpServer.conns.Range(func(k, v interface{}) bool {
			c := v.(Client)
			if c.Type() == typeProducer {
				producerStats = append(producerStats, c.Stats(topic))
			}
			return true
		})
		stats.Producers = producerStats
	}

	return stats
}

// NewTopicStats 根据给定的主题和频道统计信息，创建并返回一个新的主题统计对象。
// 该函数将当前主题的名称、各频道的统计信息、主题的深度、后端深度、消息计数、消息字节总量
// 以及是否暂停等信息封装到TopicStats结构中，并返回该结构。
// 参数:
// - t: 指向主题的指针，用于获取主题相关的统计信息。
// - channels: 一个ChannelStats数组，包含了各个频道的统计信息。
// 返回值:
//   - TopicStats: 包含了主题名称、频道统计信息、深度、后端深度、消息计数、消息字节总量、是否暂停
//     以及端到端处理延迟的主题统计信息结构体。
func NewTopicStats(t *Topic, channels []ChannelStats) TopicStats {
	return TopicStats{
		TopicName:    t.name,                             // 设置主题名称
		Channels:     channels,                           // 设置频道统计信息数组
		Depth:        t.Depth(),                          // 设置主题的深度
		BackendDepth: t.backend.Depth(),                  // 设置后端深度
		MessageCount: atomic.LoadUint64(&t.messageCount), // 原子操作加载消息计数
		MessageBytes: atomic.LoadUint64(&t.messageBytes), // 原子操作加载消息字节总量
		Paused:       t.IsPaused(),                       // 设置主题的暂停状态

		// 计算并设置端到端处理延迟
		E2eProcessingLatency: t.AggregateChannelE2eProcessingLatency().Result(),
	}
}

// memStats 定义了内存统计信息的结构体。
// 该结构体用于存储和表示Go运行时的内存管理统计数据，主要包括堆内存的使用情况、垃圾回收暂停时间和垃圾回收运行次数。
type memStats struct {
	// HeapObjects 表示当前堆上的对象数量。
	HeapObjects uint64 `json:"heap_objects"`
	// HeapIdleBytes 表示堆上空闲的字节数。
	HeapIdleBytes uint64 `json:"heap_idle_bytes"`
	// HeapInUseBytes 表示堆上正在使用的字节数。
	HeapInUseBytes uint64 `json:"heap_in_use_bytes"`
	// HeapReleasedBytes 表示堆上已释放的字节数。
	HeapReleasedBytes uint64 `json:"heap_released_bytes"`
	// GCPauseUsec100 表示垃圾回收暂停时间的百分位数（100%）。
	GCPauseUsec100 uint64 `json:"gc_pause_usec_100"`
	// GCPauseUsec99 表示垃圾回收暂停时间的百分位数（99%）。
	GCPauseUsec99 uint64 `json:"gc_pause_usec_99"`
	// GCPauseUsec95 表示垃圾回收暂停时间的百分位数（95%）。
	GCPauseUsec95 uint64 `json:"gc_pause_usec_95"`
	// NextGCBytes 表示下一次垃圾回收预计触发时堆的占用字节数。
	NextGCBytes uint64 `json:"next_gc_bytes"`
	// GCTotalRuns 表示垃圾回收总共运行的次数。
	GCTotalRuns uint32 `json:"gc_total_runs"`
}

// getMemStats 获取并计算内存统计数据
// 该函数通过调用runtime包的ReadMemStats函数获取当前的内存统计信息，
// 并计算垃圾回收暂停时间的百分位数。
// 返回值包括堆对象数量、空闲堆内存大小、使用中堆内存大小、已释放堆内存大小、
// 垃圾回收暂停时间的百分位数、下一次垃圾回收的阈值、以及已执行的垃圾回收次数。
func getMemStats() memStats {
	// 初始化内存统计变量
	var ms runtime.MemStats
	// 读取当前的内存统计信息
	runtime.ReadMemStats(&ms)

	// sort the GC pause array
	// 需要注意的是，GC暂停时间数组PauseNs可能比实际GC次数长，
	// 因此只复制实际GC次数的暂停时间进行排序。
	length := len(ms.PauseNs)
	if int(ms.NumGC) < length {
		length = int(ms.NumGC)
	}
	// 创建一个切片来存储GC暂停时间，并进行排序
	gcPauses := make(Uint64Slice, length)
	copy(gcPauses, ms.PauseNs[:length])
	sort.Sort(gcPauses)

	// 计算并返回内存统计数据
	// 这里返回的GC暂停时间的百分位数被转换为秒（除以1000）
	return memStats{
		ms.HeapObjects,
		ms.HeapIdle,
		ms.HeapInuse,
		ms.HeapReleased,
		percentile(100.0, gcPauses, len(gcPauses)) / 1000,
		percentile(99.0, gcPauses, len(gcPauses)) / 1000,
		percentile(95.0, gcPauses, len(gcPauses)) / 1000,
		ms.NextGC,
		ms.NumGC,
	}
}
