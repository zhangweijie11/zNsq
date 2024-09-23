package http_api

import (
	"errors"
	"github.com/zhangweijie11/zNsq/internal/protocol"
)

type getter interface {
	Get(key string) (string, error)
}

// GetTopicChannelArgs 从给定的getter接口中获取主题(topic)和频道(channel)名称。
// 这个函数主要用于解析协议所需的topic和channel参数，并验证它们的有效性。
// 参数:
//
//	rp: 一个实现了Get方法的getter接口，用于获取配置或动态参数。
//
// 返回值:
//
//	topicName: 解析出的主题名称字符串。
//	channelName: 解析出的频道名称字符串。
//	error: 如果参数缺失或无效，返回相应的错误。
func GetTopicChannelArgs(rp getter) (string, string, error) {
	// 获取topic名称
	topicName, err := rp.Get("topic")
	if err != nil {
		// 如果无法获取到topic，则返回错误
		return "", "", errors.New("MISSING_ARG_TOPIC")
	}

	// 验证topic名称是否有效
	if !protocol.IsValidTopicName(topicName) {
		// 如果topic名称无效，则返回错误
		return "", "", errors.New("INVALID_ARG_TOPIC")
	}

	// 获取channel名称
	channelName, err := rp.Get("channel")
	if err != nil {
		// 如果无法获取到channel，则返回错误
		return "", "", errors.New("MISSING_ARG_CHANNEL")
	}

	// 验证channel名称是否有效
	if !protocol.IsValidChannelName(channelName) {
		// 如果channel名称无效，则返回错误
		return "", "", errors.New("INVALID_ARG_CHANNEL")
	}

	// 如果所有参数都有效，则返回解析出的topic和channel名称
	return topicName, channelName, nil
}
