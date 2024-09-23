package clusterinfo

import (
	"fmt"
	"github.com/zhangweijie11/zNsq/internal/http_api"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"github.com/zhangweijie11/zNsq/internal/stringy"
	"net/url"
	"sort"
	"strings"
	"sync"
)

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
