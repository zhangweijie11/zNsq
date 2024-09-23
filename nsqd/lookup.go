package nsqd

import (
	"bytes"
	"encoding/json"
	"github.com/nsqio/go-nsq"
	"github.com/zhangweijie11/zNsq/internal/version"
	"net"
	"os"
	"strconv"
	"time"
)

// connectCallback 用于在 NSQD 实例与 lookupd 建立连接时调用。
// 参数 n 是 NSQD 实例的指针，hostname 是主机名。
func connectCallback(n *NSQD, hostname string) func(*lookupPeer) {
	return func(lp *lookupPeer) {
		// 准备客户端信息以发送给 lookupd。
		ci := make(map[string]interface{})
		ci["version"] = version.Binary
		ci["tcp_port"] = n.getOpts().BroadcastTCPPort
		ci["http_port"] = n.getOpts().BroadcastHTTPPort
		ci["hostname"] = hostname
		ci["broadcast_address"] = n.getOpts().BroadcastAddress

		// 构造 Identify 命令并发送给 lookupd。
		cmd, err := nsq.Identify(ci)
		if err != nil {
			lp.Close()
			return
		}

		// 根据 lookupd 的响应处理结果。
		resp, err := lp.Command(cmd)
		if err != nil {
			n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
			return
		} else if bytes.Equal(resp, []byte("E_INVALID")) {
			n.logf(LOG_INFO, "LOOKUPD(%s): lookupd returned %s", lp, resp)
			lp.Close()
			return
		}

		// 解析 lookupd 的响应以更新 lookupPeer 的信息。
		err = json.Unmarshal(resp, &lp.Info)
		if err != nil {
			n.logf(LOG_ERROR, "LOOKUPD(%s): parsing response - %s", lp, resp)
			lp.Close()
			return
		}
		n.logf(LOG_INFO, "LOOKUPD(%s): peer info %+v", lp, lp.Info)
		if lp.Info.BroadcastAddress == "" {
			n.logf(LOG_ERROR, "LOOKUPD(%s): no broadcast address", lp)
		}

		// 构建注册命令列表，确保快速释放锁。
		var commands []*nsq.Command
		n.RLock()
		for _, topic := range n.topicMap {
			topic.RLock()
			if len(topic.channelMap) == 0 {
				commands = append(commands, nsq.Register(topic.name, ""))
			} else {
				for _, channel := range topic.channelMap {
					commands = append(commands, nsq.Register(channel.topicName, channel.name))
				}
			}
			topic.RUnlock()
		}
		n.RUnlock()

		// 向 lookupd 发送注册命令。
		for _, cmd := range commands {
			n.logf(LOG_INFO, "LOOKUPD(%s): %s", lp, cmd)
			_, err := lp.Command(cmd)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
				return
			}
		}
	}
}

// lookupdHTTPAddrs 返回当前NSQD实例已知的lookupd的HTTP地址列表。
// 该函数首先检查lookupPeers是否已加载，如果未加载，则直接返回nil。
// 对于每个lookupPeer，如果其广播地址非空，则根据广播地址和HTTP端口构造完整的HTTP地址，
// 并将其添加到lookupHTTPAddrs切片中。
// 最终返回包含所有已知lookupd的HTTP地址的切片。
func (n *NSQD) lookupdHTTPAddrs() []string {
	// 初始化HTTP地址切片
	var lookupHTTPAddrs []string

	// 加载lookup peers
	lookupPeers := n.lookupPeers.Load()
	if lookupPeers == nil {
		return nil
	}

	// 遍历所有lookup peers
	for _, lp := range lookupPeers.([]*lookupPeer) {
		// 检查广播地址是否有效
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		// 构造HTTP地址
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort))
		// 将HTTP地址添加到切片中
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}

	// 返回所有lookupd的HTTP地址
	return lookupHTTPAddrs
}

// in函数用于检查字符串s是否存在于字符串切片lst中
// 参数s：需要查找的字符串
// 参数lst：包含一系列字符串的切片，用于在其中查找s
// 返回值：如果s存在于lst中，则返回true；否则返回false
func in(s string, lst []string) bool {
	// 遍历lst中的每个元素
	for _, v := range lst {
		// 如果s与当前元素v相等，则说明s存在于lst中，返回true
		if s == v {
			return true
		}
	}
	// 遍历完lst后，仍未找到与s相等的元素，返回false
	return false
}

// lookupLoop 函数是一个无限循环，用于与lookupd进行通信，以保持TCP连接，
func (n *NSQD) lookupLoop() {
	// 初始化lookupPeers和lookupAddrs变量
	var lookupPeers []*lookupPeer
	var lookupAddrs []string
	// 标记是否需要连接到lookupd
	connect := true

	// 获取主机名，用于TCP连接标识
	hostname, err := os.Hostname()
	if err != nil {
		n.logf(LOG_FATAL, "failed to get hostname - %s", err)
		os.Exit(1)
	}

	// for announcements, lookupd determines the host automatically
	// 使用定时器发送心跳信号
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		// 根据connect标志添加新的lookupd peers
		if connect {
			for _, host := range n.getOpts().NSQLookupdTCPAddresses {
				if in(host, lookupAddrs) {
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): adding peer", host)
				lookupPeer := newLookupPeer(host, n.getOpts().MaxBodySize, n.logf,
					connectCallback(n, hostname))
				lookupPeer.Command(nil) // 开始连接
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
			}
			n.lookupPeers.Store(lookupPeers)
			connect = false
		}

		// 处理不同的条件分支
		select {
		case <-ticker.C:
			// 发送心跳并读取响应 (读操作检测连接是否关闭)
			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_DEBUG, "LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := nsq.Ping()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case val := <-n.notifyChan:
			// 处理通道或主题的更新
			var cmd *nsq.Command
			var branch string

			switch val := val.(type) {
			case *Channel:
				branch = "channel"
				channel := val
				if channel.Exiting() {
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else {
					cmd = nsq.Register(channel.topicName, channel.name)
				}
			case *Topic:
				branch = "topic"
				topic := val
				if topic.Exiting() {
					cmd = nsq.UnRegister(topic.name, "")
				} else {
					cmd = nsq.Register(topic.name, "")
				}
			}

			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_INFO, "LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case <-n.optsNotificationChan:
			// 根据配置更新peer列表
			var tmpPeers []*lookupPeer
			var tmpAddrs []string
			for _, lp := range lookupPeers {
				if in(lp.addr, n.getOpts().NSQLookupdTCPAddresses) {
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.addr)
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): removing peer", lp)
				lp.Close()
			}
			lookupPeers = tmpPeers
			lookupAddrs = tmpAddrs
			connect = true
		case <-n.exitChan:
			// 接收到退出信号，退出循环
			goto exit
		}
	}

exit:
	n.logf(LOG_INFO, "LOOKUP: closing")
}
