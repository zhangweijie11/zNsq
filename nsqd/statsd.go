package nsqd

import (
	"fmt"
	"github.com/zhangweijie11/zNsq/internal/statsd"
	"github.com/zhangweijie11/zNsq/internal/writers"
	"math"
	"net"
	"strings"
	"time"
)

type Uint64Slice []uint64

func (s Uint64Slice) Len() int {
	return len(s)
}

func (s Uint64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Uint64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

// percentile 计算给定排序数组arr中指定百分位数的值。
// perc 是要计算的百分位数（例如，50表示第50百分位数）。
// arr 是已排序的uint64类型数组。
// length 是arr数组的长度。
// 返回arr数组中指定百分位数对应的值。
func percentile(perc float64, arr []uint64, length int) uint64 {
	// 如果数组长度为0，则直接返回0。
	if length == 0 {
		return 0
	}

	// 计算百分位数对应的实际索引。
	indexOfPerc := int(math.Floor(((perc / 100.0) * float64(length)) + 0.5))
	// 确保计算出的索引在有效范围内。
	// 如果索引超过数组长度，则将其设置为数组长度减1。
	if indexOfPerc >= length {
		indexOfPerc = length - 1
	}

	// 返回该索引处的数组元素作为百分位数的值。
	return arr[indexOfPerc]
}

// statsdLoop 启动一个 goroutine，用于定期将统计数据推送到 statsd 服务器。
func (n *NSQD) statsdLoop() {
	// 初始化上次收集的内存和统计信息
	var lastMemStats memStats
	var lastStats Stats
	// 获取配置的 statsd 推送间隔
	interval := n.getOpts().StatsdInterval
	// 创建一个定时器，每隔 interval 时间触发一次
	ticker := time.NewTicker(interval)
	for {
		select {
		// 接收到退出信号，准备退出循环
		case <-n.exitChan:
			goto exit
		// 定时器触发，执行 statsd 数据推送
		case <-ticker.C:
			// 获取 statsd 服务器地址
			addr := n.getOpts().StatsdAddress
			// 获取 statsd 前缀
			prefix := n.getOpts().StatsdPrefix
			// 获取是否排除临时主题的配置
			excludeEphemeral := n.getOpts().StatsdExcludeEphemeral
			// 创建到 statsd 的 UDP 连接
			conn, err := net.DialTimeout("udp", addr, time.Second)
			if err != nil {
				// 连接失败，记录错误并继续下一次循环
				n.logf(LOG_ERROR, "failed to create UDP socket to statsd(%s)", addr)
				continue
			}
			// 创建分发写入器，用于平滑地写入数据
			sw := writers.NewSpreadWriter(conn, interval-time.Second, n.exitChan)
			// 创建带边界的缓冲写入器，防止数据包过大
			bw := writers.NewBoundaryBufferedWriter(sw, n.getOpts().StatsdUDPPacketSize)
			// 创建 statsd 客户端
			client := statsd.NewClient(bw, prefix)

			// 记录日志，准备推送统计信息
			n.logf(LOG_INFO, "STATSD: pushing stats to %s", addr)

			// 获取当前统计信息
			stats := n.GetStats("", "", false)
			// 遍历每个主题
			for _, topic := range stats.Topics {
				// 如果需要排除临时主题，则跳过临时主题
				if excludeEphemeral && strings.HasSuffix(topic.TopicName, "#ephemeral") {
					continue
				}

				// 尝试在上次收集的主题中找到当前主题
				lastTopic := TopicStats{}
				for _, checkTopic := range lastStats.Topics {
					if topic.TopicName == checkTopic.TopicName {
						lastTopic = checkTopic
						break
					}
				}
				// 计算消息数量的差值，并发送到 statsd
				diff := topic.MessageCount - lastTopic.MessageCount
				stat := fmt.Sprintf("topic.%s.message_count", topic.TopicName)
				client.Incr(stat, int64(diff))

				// 计算消息字节数的差值，并发送到 statsd
				diff = topic.MessageBytes - lastTopic.MessageBytes
				stat = fmt.Sprintf("topic.%s.message_bytes", topic.TopicName)
				client.Incr(stat, int64(diff))

				// 发送主题深度到 statsd
				stat = fmt.Sprintf("topic.%s.depth", topic.TopicName)
				client.Gauge(stat, topic.Depth)

				// 发送主题后端深度到 statsd
				stat = fmt.Sprintf("topic.%s.backend_depth", topic.TopicName)
				client.Gauge(stat, topic.BackendDepth)

				// 遍历每个百分位数，并发送 e2e 处理延迟到 statsd
				for _, item := range topic.E2eProcessingLatency.Percentiles {
					stat = fmt.Sprintf("topic.%s.e2e_processing_latency_%.0f", topic.TopicName, item["quantile"]*100.0)
					client.Gauge(stat, int64(item["value"]))
				}

				// 遍历每个通道
				for _, channel := range topic.Channels {
					// 如果需要排除临时通道，则跳过临时通道
					if excludeEphemeral && strings.HasSuffix(channel.ChannelName, "#ephemeral") {
						continue
					}

					// 尝试在上次收集的通道中找到当前通道
					lastChannel := ChannelStats{}
					for _, checkChannel := range lastTopic.Channels {
						if channel.ChannelName == checkChannel.ChannelName {
							lastChannel = checkChannel
							break
						}
					}
					// 计算消息数量的差值，并发送到 statsd
					diff := channel.MessageCount - lastChannel.MessageCount
					stat := fmt.Sprintf("topic.%s.channel.%s.message_count", topic.TopicName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					// 发送通道深度到 statsd
					stat = fmt.Sprintf("topic.%s.channel.%s.depth", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, channel.Depth)

					// 发送通道后端深度到 statsd
					stat = fmt.Sprintf("topic.%s.channel.%s.backend_depth", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, channel.BackendDepth)

					// 发送通道中正在处理的消息数量到 statsd
					stat = fmt.Sprintf("topic.%s.channel.%s.in_flight_count", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, int64(channel.InFlightCount))

					// 发送通道中延迟的消息数量到 statsd
					stat = fmt.Sprintf("topic.%s.channel.%s.deferred_count", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, int64(channel.DeferredCount))

					// 计算重新排队数量的差值，并发送到 statsd
					diff = channel.RequeueCount - lastChannel.RequeueCount
					stat = fmt.Sprintf("topic.%s.channel.%s.requeue_count", topic.TopicName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					// 计算超时数量的差值，并发送到 statsd
					diff = channel.TimeoutCount - lastChannel.TimeoutCount
					stat = fmt.Sprintf("topic.%s.channel.%s.timeout_count", topic.TopicName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					// 发送通道中客户端数量到 statsd
					stat = fmt.Sprintf("topic.%s.channel.%s.clients", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, int64(channel.ClientCount))

					// 遍历每个百分位数，并发送 e2e 处理延迟到 statsd
					for _, item := range channel.E2eProcessingLatency.Percentiles {
						stat = fmt.Sprintf("topic.%s.channel.%s.e2e_processing_latency_%.0f", topic.TopicName, channel.ChannelName, item["quantile"]*100.0)
						client.Gauge(stat, int64(item["value"]))
					}
				}
			}
			// 更新上次收集的统计信息
			lastStats = stats

			// 如果配置了推送内存统计信息，则获取并发送内存统计信息到 statsd
			if n.getOpts().StatsdMemStats {
				ms := getMemStats()

				client.Gauge("mem.heap_objects", int64(ms.HeapObjects))
				client.Gauge("mem.heap_idle_bytes", int64(ms.HeapIdleBytes))
				client.Gauge("mem.heap_in_use_bytes", int64(ms.HeapInUseBytes))
				client.Gauge("mem.heap_released_bytes", int64(ms.HeapReleasedBytes))
				client.Gauge("mem.gc_pause_usec_100", int64(ms.GCPauseUsec100))
				client.Gauge("mem.gc_pause_usec_99", int64(ms.GCPauseUsec99))
				client.Gauge("mem.gc_pause_usec_95", int64(ms.GCPauseUsec95))
				client.Gauge("mem.next_gc_bytes", int64(ms.NextGCBytes))
				client.Incr("mem.gc_runs", int64(ms.GCTotalRuns-lastMemStats.GCTotalRuns))

				// 更新上次收集的内存统计信息
				lastMemStats = ms
			}

			// 刷新并关闭连接
			bw.Flush()
			sw.Flush()
			conn.Close()
		}
	}

exit:
	// 停止定时器，并记录日志，statsd 循环已退出
	ticker.Stop()
	n.logf(LOG_INFO, "STATSD: closing")
}
