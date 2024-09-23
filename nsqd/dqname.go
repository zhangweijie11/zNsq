//go:build !windows
// +build !windows

package nsqd

// getBackendName 构建并返回一个唯一的后端名称，该名称由主题名和频道名组合而成。
// 这个函数的存在是因为在某些场景下，需要通过主题和频道的组合来唯一标识一个后端。
// 参数:
// topicName: 消息的主题名。用于区分不同的消息主题。
// channelName: 消息的频道名。用于在同一主题下进一步区分不同的频道。
// 返回值: 返回一个字符串，包含了组合的主题名和频道名，格式为 <topic>:<channel>。
func getBackendName(topicName, channelName string) string {
	// 自动组合主题和频道名称，以冒号分隔，形成唯一的后端名称。
	backendName := topicName + ":" + channelName
	return backendName
}
