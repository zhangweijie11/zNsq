package util

import "net"

// TypeOfAddr 根据地址类型返回相应的协议类型。
// 它通过尝试将地址分割成主机和端口来判断地址是TCP类型的还是UNIX类型的。
// 参数:
//
//	addr - 需要判断的地址字符串。
//
// 返回值:
//
//	如果地址是TCP类型的，返回"tcp"；如果是UNIX类型的，返回"unix"。
func TypeOfAddr(addr string) string {
	// 尝试将地址分割成主机和端口，如果成功则说明地址是TCP类型的。
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return "tcp"
	}
	// 如果地址不能被分割成主机和端口，假设它是UNIX类型的。
	return "unix"
}
