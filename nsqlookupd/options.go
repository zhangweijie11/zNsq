package nsqlookupd

import (
	"github.com/zhangweijie11/zNsq/internal/lg"
	"log"
	"os"
	"time"
)

// Options 配置
type Options struct {
	Logger                  Logger        // 日志记录器
	LogLevel                lg.LogLevel   `flag:"log-level"`                 // 日志级别
	LogPrefix               string        `flag:"log-prefix"`                // 日志前缀
	TCPAddress              string        `flag:"tcp-address"`               // TCP地址
	HTTPAddress             string        `flag:"http-address"`              // HTTP地址
	BroadcastAddress        string        `flag:"broadcast-address"`         // 广播地址
	InactiveProducerTimeout time.Duration `flag:"inactive-producer-timeout"` // 非活跃生产者的超时时间
	TombstoneLifetime       time.Duration `flag:"tombstone-lifetime"`        // 墓碑的生命周期
}

// NewOptions 返回一个配置了默认选项的Options指针。
// 这个函数初始化默认的配置参数，包括日志设置、网络地址、节点标识等。
func NewOptions() *Options {
	// 获取当前主机名，用于标识实例
	hostname, err := os.Hostname()
	if err != nil {
		// 如果无法获取主机名，记录错误并终止程序
		log.Fatal(err)
	}

	// 返回一个新的Options对象，使用默认配置和获取到的主机名
	return &Options{
		LogPrefix:        "[nsqlookupd] ", // 设置日志前缀，用于标识日志来源
		LogLevel:         lg.INFO,         // 设置日志级别为INFO，记录一般重要性的日志信息
		TCPAddress:       "0.0.0.0:4160",  // 设置TCP监听地址和端口
		HTTPAddress:      "0.0.0.0:4161",  // 设置HTTP监听地址和端口
		BroadcastAddress: hostname,        // 使用获取到的主机名作为广播地址

		InactiveProducerTimeout: 300 * time.Second, // 设置非活跃生产者的超时时间为300秒
		TombstoneLifetime:       45 * time.Second,  // 设置Tombstone生命周期为45秒
	}
}
