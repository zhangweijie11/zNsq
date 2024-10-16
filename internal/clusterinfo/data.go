package clusterinfo

import (
	"fmt"
	"github.com/blang/semver"
	"github.com/zhangweijie11/zNsq/internal/http_api"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"github.com/zhangweijie11/zNsq/internal/stringy"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type PartialErr interface {
	error
	Errors() []error
}

// ErrList 是一个自定义的错误类型，它包含一个错误列表。
// 这使得ErrList能够持有多个错误，并在需要时将它们作为一个整体处理。
type ErrList []error

// Error 实现了error接口，它通过将ErrList中的所有错误信息连接成一个字符串来生成错误信息。
// 这个方法遍历ErrList中的每一个错误，调用每个错误的Error方法获取错误信息，
// 然后将所有的错误信息用换行符连接起来。这样可以在显示错误时一次性展示所有错误信息，
// 使得错误处理过程更加集中和方便。
func (l ErrList) Error() string {
	var es []string
	for _, e := range l {
		es = append(es, e.Error())
	}
	return strings.Join(es, "\n")
}

// Errors 返回ErrList本身，本质上就是返回列表中所有的错误。
// 这个方法可以用于获取ErrList中的所有错误，以便于进一步处理或者访问。
// 它提供了一种访问ErrList内部错误列表的途径，而不需要直接操作内部结构。
func (l ErrList) Errors() []error {
	return l
}

// ClusterInfo 结构体封装了集群信息的相关数据。
// 它包含了一个日志记录函数和一个http客户端，用于与集群进行通信。
type ClusterInfo struct {
	log    lg.AppLogFunc    // log 字段用于记录日志信息。
	client *http_api.Client // client 字段持有一个http客户端实例，用于发送和接收http请求。
}

// New 创建并返回一个新的 ClusterInfo 实例。
// 该函数接收一个日志记录函数和一个 http_api 的客户端实例作为参数，
// 并将它们用于初始化 ClusterInfo 实例的相应字段。
// 参数:
// - log: 用于记录日志的函数，可以帮助在集群信息处理过程中记录重要信息或错误。
// - client: 一个 http_api 的客户端实例，用于与集群进行通信，获取和操作集群信息。
// 返回值:
// - *ClusterInfo: 返回一个指向 ClusterInfo 实例的指针，该实例包含了传入的日志记录函数和 http_api 客户端。
func New(log lg.AppLogFunc, client *http_api.Client) *ClusterInfo {
	// 使用传入的日志记录函数和 http_api 客户端初始化 ClusterInfo 实例并返回。
	return &ClusterInfo{
		log:    log,
		client: client,
	}
}

// logf 是ClusterInfo的一个方法，用于记录日志。
// 它首先检查ClusterInfo实例中是否初始化了日志记录器。
// 如果初始化了，它将使用日志记录器记录一条信息级别的日志。
// 日志格式由参数f指定，args...参数将被插入到f中的相应位置。
//
// 参数:
//
//	f: 日志信息的格式字符串，类似于printf风格的格式化字符串。
//	args...: 可变长度的参数列表，这些参数将被格式化并插入到f中。
//
// 该方法没有返回值。
func (c *ClusterInfo) logf(f string, args ...interface{}) {
	if c.log != nil {
		c.log(lg.INFO, f, args...)
	}
}

// GetVersion 通过指定的集群地址获取集群版本信息。
// 参数:
//
//	addr - 集群的地址，用于构造请求的URL。
//
// 返回值:
//
//	semver.Version - 解析后的集群版本。
//	error - 错误信息，如果请求失败或版本信息为空，则返回错误。
func (c *ClusterInfo) GetVersion(addr string) (semver.Version, error) {
	// 构造请求集群信息的URL。
	endpoint := fmt.Sprintf("http://%s/info", addr)

	// 定义响应结构体，用于存储从集群获取的信息。
	var resp struct {
		Version string `json:"version"`
	}

	// 向集群发送GET请求，获取版本信息。
	err := c.client.GETV1(endpoint, &resp)
	if err != nil {
		// 如果请求失败，则返回解析后的空版本和错误信息。
		return semver.Version{}, err
	}

	// 如果响应的版本信息为空，则默认设置为"unknown"。
	if resp.Version == "" {
		resp.Version = "unknown"
	}

	// 解析版本字符串并返回。
	return semver.Parse(resp.Version)
}

// GetLookupdTopics 查询给定 nsqlookupd 服务上的所有主题
// lookupdHTTPAddrs 参数是 nsqlookupd 服务的 HTTP 地址列表
// 返回值是查询到的主题列表和可能的错误
func (c *ClusterInfo) GetLookupdTopics(lookupdHTTPAddrs []string) ([]string, error) {
	// topics 用于存储所有主题
	var topics []string
	// lock 用于确保线程安全地访问 topics 和 errs
	var lock sync.Mutex
	// wg 用于等待所有并发查询完成
	var wg sync.WaitGroup
	// errs 用于收集查询过程中遇到的错误
	var errs []error

	// respType 用于解析 nsqlookupd 返回的 JSON 响应
	type respType struct {
		Topics []string `json:"topics"`
	}

	// 遍历每个 nsqlookupd 地址发起查询
	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			// 构建查询 endpoint
			endpoint := fmt.Sprintf("http://%s/topics", addr)
			// 记录查询日志
			c.logf("CI: querying nsqlookupd %s", endpoint)

			// 用于存储查询响应
			var resp respType
			// 发起 GET 请求
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				// 如果请求失败，记录错误
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			// 线程安全地更新 topics
			lock.Lock()
			defer lock.Unlock()
			topics = append(topics, resp.Topics...)
		}(addr)
	}
	// 等待所有查询完成
	wg.Wait()

	// 如果所有查询都失败了，返回错误
	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqlookupd: %s", ErrList(errs))
	}

	// 去重并排序 topics
	topics = stringy.Uniq(topics)
	sort.Strings(topics)

	// 如果有查询失败，返回主题列表和错误列表
	if len(errs) > 0 {
		return topics, ErrList(errs)
	}
	// 返回主题列表和 nil 表示没有错误
	return topics, nil
}

// GetLookupdTopicChannels 查询给定主题在指定的 nsqlookupd 服务上的所有频道
// 该方法并行查询所有提供的 nsqlookupd 地址，并等待所有查询完成
// 它会收集每个 nsqlookupd 返回的频道，并确保返回的频道列表是唯一的且排序的
// 如果所有 nsqlookupd 都查询失败，则返回错误
// 参数:
//
//	topic - 要查询的主题名称
//	lookupdHTTPAddrs - nsqlookupd 服务的 HTTP 地址列表
//
// 返回值:
//
//	一个字符串切片，包含所有唯一的频道名称，按字母顺序排序
//	一个错误，如果至少有一个 nsqlookupd 查询失败；否则为 nil
func (c *ClusterInfo) GetLookupdTopicChannels(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	// 初始化频道切片
	var channels []string
	// 用于线程安全地更新 channels 切片的锁
	var lock sync.Mutex
	// 用于等待所有并发查询完成的等待组
	var wg sync.WaitGroup
	// 存储查询过程中遇到的错误
	var errs []error

	// 定义一个结构体来解析 nsqlookupd 的响应
	type respType struct {
		Channels []string `json:"channels"`
	}

	// 对每个 nsqlookupd 地址发起并行查询
	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			// 构造查询 nsqlookupd 的 endpoint
			endpoint := fmt.Sprintf("http://%s/channels?topic=%s", addr, url.QueryEscape(topic))
			c.logf("CI: querying nsqlookupd %s", endpoint)

			// 用于存储 nsqlookupd 响应的变量
			var resp respType
			// 执行 HTTP GET 请求
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				// 如果发生错误，记录错误
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			// 线程安全地更新 channels 切片
			lock.Lock()
			defer lock.Unlock()
			channels = append(channels, resp.Channels...)
		}(addr)
	}
	// 等待所有查询完成
	wg.Wait()

	// 如果所有 nsqlookupd 都查询失败，返回错误
	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqlookupd: %s", ErrList(errs))
	}

	// 确保 channels 列表中的元素唯一，并进行排序
	channels = stringy.Uniq(channels)
	sort.Strings(channels)

	// 如果有查询失败，返回频道列表和错误列表
	if len(errs) > 0 {
		return channels, ErrList(errs)
	}
	// 所有查询成功，返回频道列表和 nil 错误
	return channels, nil
}

// GetLookupdProducers 从给定的nsqlookupd HTTP地址列表中获取生产者信息。
// 参数 lookupdHTTPAddrs 是nsqlookupd的HTTP地址数组。
// 返回值 Producers 是生产者的信息列表，error 是可能的错误。
// 如果能够从至少一个nsqlookupd获取信息，则返回该lookupd的生产者信息列表和可能的错误列表。
// 如果无法从任何nsqlookupd获取信息，则返回错误。
func (c *ClusterInfo) GetLookupdProducers(lookupdHTTPAddrs []string) (Producers, error) {
	// producers 用于存储所有生产者信息。
	var producers []*Producer
	// lock 用于在并发环境下安全地操作producers切片。
	var lock sync.Mutex
	// wg 用于等待所有并发查询完成。
	var wg sync.WaitGroup
	// errs 用于收集从nsqlookupd查询时可能发生的错误。
	var errs []error

	// producersByAddr 用于根据生产者的TCP地址唯一标识生产者。
	producersByAddr := make(map[string]*Producer)
	// maxVersion 用于存储查询到的最大版本号。
	maxVersion, _ := semver.Parse("0.0.0")

	// respType 用于解析nsqlookupd返回的JSON数据。
	type respType struct {
		Producers []*Producer `json:"producers"`
	}

	// 遍历nsqlookupd HTTP地址列表，对每个地址发起查询请求。
	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			// 构建nsqlookupd的查询地址。
			endpoint := fmt.Sprintf("http://%s/nodes", addr)
			// 记录查询请求的日志。
			c.logf("CI: querying nsqlookupd %s", endpoint)

			// 定义一个临时变量用于存储响应数据。
			var resp respType
			// 发起GET请求，获取nsqlookupd的生产者信息。
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				// 如果发生错误，记录错误信息。
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			// 同步访问producersByAddr。
			lock.Lock()
			defer lock.Unlock()
			// 处理响应中的生产者信息。
			for _, producer := range resp.Producers {
				// 使用TCP地址唯一标识生产者。
				key := producer.TCPAddress()
				p, ok := producersByAddr[key]
				if !ok {
					// 如果是新发现的生产者，将其添加到producersByAddr和producers中。
					producersByAddr[key] = producer
					producers = append(producers, producer)
					// 更新最大版本号。
					if maxVersion.LT(producer.VersionObj) {
						maxVersion = producer.VersionObj
					}
					// 对生产者支持的主题进行排序。
					sort.Sort(producer.Topics)
					p = producer
				}
				// 记录该nsqlookupd的地址。
				p.RemoteAddresses = append(p.RemoteAddresses,
					fmt.Sprintf("%s/%s", addr, producer.Address()))
			}
		}(addr)
	}
	// 等待所有查询完成。
	wg.Wait()

	// 如果所有查询都失败了，则返回错误。
	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqlookupd: %s", ErrList(errs))
	}

	// 标记版本低于最大版本号的生产者为过时。
	for _, producer := range producersByAddr {
		if producer.VersionObj.LT(maxVersion) {
			producer.OutOfDate = true
		}
	}
	// 对生产者列表按主机名排序。
	sort.Sort(ProducersByHost{producers})

	// 如果有查询失败，则返回生产者列表和错误列表。
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	// 如果所有查询都成功，则只返回生产者列表。
	return producers, nil
}

// GetLookupdTopicProducers returns Producers of all the nsqd for a given topic by
// unioning the nodes returned from the given lookupd
func (c *ClusterInfo) GetLookupdTopicProducers(topic string, lookupdHTTPAddrs []string) (Producers, error) {
	var producers Producers
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Producers Producers `json:"producers"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, p := range resp.Producers {
				for _, pp := range producers {
					if p.HTTPAddress() == pp.HTTPAddress() {
						goto skip
					}
				}
				producers = append(producers, p)
			skip:
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqlookupd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetNSQDTopics returns a []string containing all the topics produced by the given nsqd
func (c *ClusterInfo) GetNSQDTopics(nsqdHTTPAddrs []string) ([]string, error) {
	var topics []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, topic := range resp.Topics {
				topics = stringy.Add(topics, topic.Name)
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqd: %s", ErrList(errs))
	}

	sort.Strings(topics)

	if len(errs) > 0 {
		return topics, ErrList(errs)
	}
	return topics, nil
}

// GetNSQDProducers returns Producers of all the given nsqd
func (c *ClusterInfo) GetNSQDProducers(nsqdHTTPAddrs []string) (Producers, error) {
	var producers Producers
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type infoRespType struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
	}

	type statsRespType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/info", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var infoResp infoRespType
			err := c.client.GETV1(endpoint, &infoResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			endpoint = fmt.Sprintf("http://%s/stats?format=json&include_clients=false", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var statsResp statsRespType
			err = c.client.GETV1(endpoint, &statsResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			var producerTopics ProducerTopics
			for _, t := range statsResp.Topics {
				producerTopics = append(producerTopics, ProducerTopic{Topic: t.Name})
			}

			version, err := semver.Parse(infoResp.Version)
			if err != nil {
				version, _ = semver.Parse("0.0.0")
			}

			lock.Lock()
			defer lock.Unlock()
			producers = append(producers, &Producer{
				Version:          infoResp.Version,
				VersionObj:       version,
				BroadcastAddress: infoResp.BroadcastAddress,
				Hostname:         infoResp.Hostname,
				HTTPPort:         infoResp.HTTPPort,
				TCPPort:          infoResp.TCPPort,
				Topics:           producerTopics,
			})
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetNSQDTopicProducers returns Producers containing the addresses of all the nsqd
// that produce the given topic
func (c *ClusterInfo) GetNSQDTopicProducers(topic string, nsqdHTTPAddrs []string) (Producers, error) {
	var producers Producers
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type infoRespType struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
	}

	type statsRespType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/stats?format=json&topic=%s&include_clients=false",
				addr, url.QueryEscape(topic))
			c.logf("CI: querying nsqd %s", endpoint)

			var statsResp statsRespType
			err := c.client.GETV1(endpoint, &statsResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			var producerTopics ProducerTopics
			for _, t := range statsResp.Topics {
				producerTopics = append(producerTopics, ProducerTopic{Topic: t.Name})
			}

			for _, t := range statsResp.Topics {
				if t.Name == topic {
					endpoint := fmt.Sprintf("http://%s/info", addr)
					c.logf("CI: querying nsqd %s", endpoint)

					var infoResp infoRespType
					err := c.client.GETV1(endpoint, &infoResp)
					if err != nil {
						lock.Lock()
						errs = append(errs, err)
						lock.Unlock()
						return
					}

					version, err := semver.Parse(infoResp.Version)
					if err != nil {
						version, _ = semver.Parse("0.0.0")
					}

					// if BroadcastAddress/HTTPPort are missing, use the values from `addr` for
					// backwards compatibility

					if infoResp.BroadcastAddress == "" {
						var p string
						infoResp.BroadcastAddress, p, _ = net.SplitHostPort(addr)
						infoResp.HTTPPort, _ = strconv.Atoi(p)
					}
					if infoResp.Hostname == "" {
						infoResp.Hostname, _, _ = net.SplitHostPort(addr)
					}

					lock.Lock()
					producers = append(producers, &Producer{
						Version:          infoResp.Version,
						VersionObj:       version,
						BroadcastAddress: infoResp.BroadcastAddress,
						Hostname:         infoResp.Hostname,
						HTTPPort:         infoResp.HTTPPort,
						TCPPort:          infoResp.TCPPort,
						Topics:           producerTopics,
					})
					lock.Unlock()

					return
				}
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetNSQDStats returns aggregate topic and channel stats from the given Producers
//
// if selectedChannel is empty, this will return stats for topic/channel
// if selectedTopic is empty, this will return stats for *all* topic/channels
// if includeClients is false, this will *not* return client stats for channels
// and the ChannelStats dict will be keyed by topic + ':' + channel
func (c *ClusterInfo) GetNSQDStats(producers Producers,
	selectedTopic string, selectedChannel string,
	includeClients bool) ([]*TopicStats, map[string]*ChannelStats, error) {
	var lock sync.Mutex
	var wg sync.WaitGroup
	var topicStatsList TopicStatsList
	var errs []error

	channelStatsMap := make(map[string]*ChannelStats)

	type respType struct {
		Topics []*TopicStats `json:"topics"`
	}

	for _, p := range producers {
		wg.Add(1)
		go func(p *Producer) {
			defer wg.Done()

			addr := p.HTTPAddress()

			endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
			if selectedTopic != "" {
				endpoint += "&topic=" + url.QueryEscape(selectedTopic)
				if selectedChannel != "" {
					endpoint += "&channel=" + url.QueryEscape(selectedChannel)
				}
			}
			if !includeClients {
				endpoint += "&include_clients=false"
			}

			c.logf("CI: querying nsqd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, topic := range resp.Topics {
				topic.Node = addr
				topic.Hostname = p.Hostname
				topic.MemoryDepth = topic.Depth - topic.BackendDepth
				if selectedTopic != "" && topic.TopicName != selectedTopic {
					continue
				}
				topicStatsList = append(topicStatsList, topic)

				for _, channel := range topic.Channels {
					channel.Node = addr
					channel.Hostname = p.Hostname
					channel.TopicName = topic.TopicName
					channel.MemoryDepth = channel.Depth - channel.BackendDepth
					key := channel.ChannelName
					if selectedTopic == "" {
						key = fmt.Sprintf("%s:%s", topic.TopicName, channel.ChannelName)
					}
					channelStats, ok := channelStatsMap[key]
					if !ok {
						channelStats = &ChannelStats{
							Node:        addr,
							TopicName:   topic.TopicName,
							ChannelName: channel.ChannelName,
						}
						channelStatsMap[key] = channelStats
					}
					for _, c := range channel.Clients {
						c.Node = addr
					}
					channelStats.Add(channel)
				}
			}
		}(p)
	}
	wg.Wait()

	if len(errs) == len(producers) {
		return nil, nil, fmt.Errorf("failed to query any nsqd: %s", ErrList(errs))
	}

	sort.Sort(TopicStatsByHost{topicStatsList})

	if len(errs) > 0 {
		return topicStatsList, channelStatsMap, ErrList(errs)
	}
	return topicStatsList, channelStatsMap, nil
}

// TombstoneNodeForTopic tombstones the given node for the given topic on all the given nsqlookupd
// and deletes the topic from the node
func (c *ClusterInfo) TombstoneNodeForTopic(topic string, node string, lookupdHTTPAddrs []string) error {
	var errs []error

	// tombstone the topic on all the lookupds
	qs := fmt.Sprintf("topic=%s&node=%s", url.QueryEscape(topic), url.QueryEscape(node))
	err := c.nsqlookupdPOST(lookupdHTTPAddrs, "topic/tombstone", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	producers, err := c.GetNSQDProducers([]string{node})
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	// delete the topic on the producer
	qs = fmt.Sprintf("topic=%s", url.QueryEscape(topic))
	err = c.producersPOST(producers, "topic/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) CreateTopicChannel(topicName string, channelName string, lookupdHTTPAddrs []string) error {
	var errs []error

	// create the topic on all the nsqlookupd
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	err := c.nsqlookupdPOST(lookupdHTTPAddrs, "topic/create", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(channelName) > 0 {
		qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))

		// create the channel on all the nsqlookupd
		err := c.nsqlookupdPOST(lookupdHTTPAddrs, "channel/create", qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}

		// create the channel on all the nsqd that produce the topic
		producers, err := c.GetLookupdTopicProducers(topicName, lookupdHTTPAddrs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
		err = c.producersPOST(producers, "channel/create", qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) DeleteTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	var errs []error

	// for topic removal, you need to get all the producers _first_
	producers, err := c.GetTopicProducers(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))

	// remove the topic from all the nsqlookupd
	err = c.nsqlookupdPOST(lookupdHTTPAddrs, "topic/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	// remove the topic from all the nsqd that produce this topic
	err = c.producersPOST(producers, "topic/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) DeleteChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	var errs []error

	producers, err := c.GetTopicProducers(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))

	// remove the channel from all the nsqlookupd
	err = c.nsqlookupdPOST(lookupdHTTPAddrs, "channel/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	// remove the channel from all the nsqd that produce this topic
	err = c.producersPOST(producers, "channel/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) PauseTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "topic/pause", qs)
}

func (c *ClusterInfo) UnPauseTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "topic/unpause", qs)
}

func (c *ClusterInfo) PauseChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "channel/pause", qs)
}

func (c *ClusterInfo) UnPauseChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "channel/unpause", qs)
}

func (c *ClusterInfo) EmptyTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "topic/empty", qs)
}

func (c *ClusterInfo) EmptyChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "channel/empty", qs)
}

func (c *ClusterInfo) actionHelper(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string, uri string, qs string) error {
	var errs []error

	producers, err := c.GetTopicProducers(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	err = c.producersPOST(producers, uri, qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) GetProducers(lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) (Producers, error) {
	if len(lookupdHTTPAddrs) != 0 {
		return c.GetLookupdProducers(lookupdHTTPAddrs)
	}
	return c.GetNSQDProducers(nsqdHTTPAddrs)
}

func (c *ClusterInfo) GetTopicProducers(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) (Producers, error) {
	if len(lookupdHTTPAddrs) != 0 {
		return c.GetLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	}
	return c.GetNSQDTopicProducers(topicName, nsqdHTTPAddrs)
}

func (c *ClusterInfo) nsqlookupdPOST(addrs []string, uri string, qs string) error {
	var errs []error
	for _, addr := range addrs {
		endpoint := fmt.Sprintf("http://%s/%s?%s", addr, uri, qs)
		c.logf("CI: querying nsqlookupd %s", endpoint)
		err := c.client.POSTV1(endpoint, nil, nil)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) producersPOST(pl Producers, uri string, qs string) error {
	var errs []error
	for _, p := range pl {
		endpoint := fmt.Sprintf("http://%s/%s?%s", p.HTTPAddress(), uri, qs)
		c.logf("CI: querying nsqd %s", endpoint)
		err := c.client.POSTV1(endpoint, nil, nil)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}
