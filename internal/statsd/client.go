package statsd

import (
	"fmt"
	"io"
)

type Client struct {
	w      io.Writer
	prefix string
}

// NewClient 创建并返回一个新的Client实例。
// 该函数接收一个io.Writer类型的writer和一个字符串类型的前缀作为参数。
// 参数w用于后续的数据写入操作，参数prefix则用于标识或格式化客户端的数据。
// 返回值是一个指向Client实例的指针，该实例使用给定的writer和前缀初始化。
func NewClient(w io.Writer, prefix string) *Client {
	// 使用给定的writer和前缀初始化并返回Client实例。
	return &Client{
		w:      w,
		prefix: prefix,
	}
}

// Incr Client的Incr方法用于增加统计值
// 该方法接受一个stat字符串作为统计名称，和一个int64类型的count作为要增加的数值
// 返回值为error类型，表示操作是否成功
// 此方法主要用于性能计数器的递增操作
func (c *Client) Incr(stat string, count int64) error {
	// 调用c的send方法，将stat、格式字符串和count作为参数传递
	// 这里使用"%d|c"格式化count，表示这是一个计数操作
	return c.send(stat, "%d|c", count)
}

// Decr 减少指定统计项的计数。
//
// 参数:
//
//	stat: 统计项的名称。
//	count: 需要减少的数值。如果count为负数，则表示增加的数值。
//
// 返回值:
//
//	error: 表示发送操作中可能出现的错误。如果发送成功，则返回nil。
//
// 该方法用于减少某个统计项的计数。通过发送一个负数值，结合"|c"标志，来实现计数的减少。
func (c *Client) Decr(stat string, count int64) error {
	return c.send(stat, "%d|c", -count)
}

// Timing Client的Timing方法用于发送 timing数据。
// 该方法构建并发送一条消息，该消息包含统计信息的名称和自最后记录以来经过的时间（以毫秒为单位）。
// 参数:
//
//	stat: 统计信息的名称。
//	delta: 自最后记录以来经过的时间（以毫秒为单位）。
//
// 返回值:
//
//	该方法返回一个错误，表示发送操作是否成功。
func (c *Client) Timing(stat string, delta int64) error {
	return c.send(stat, "%d|ms", delta)
}

// Gauge Client的Gauge方法用于发送一个度量值到Statsd服务器。
// 该方法主要用于性能或健康度指标的上报，其中value参数表示具体的度量值。
// 方法通过调用c.send来实际发送数据，发送的数据格式由"%d|g"指定，表示一个整数类型的度量值。
// 如果发送过程中发生错误，该方法将返回错误。
func (c *Client) Gauge(stat string, value int64) error {
	return c.send(stat, "%d|g", value)
}

// Client的send方法用于向服务器发送统计信息。
// 参数stat表示统计信息的名称，format定义了值的格式，value是要发送的具体统计数值。
// 该方法返回一个错误类型，表示发送操作是否成功。
func (c *Client) send(stat string, format string, value int64) error {
	// 构建发送消息的完整格式，包括前缀、统计信息名、格式说明和换行符。
	format = fmt.Sprintf("%s%s:%s\n", c.prefix, stat, format)

	// 使用fmt.Fprintf方法按照定义的格式化字符串和值向连接的Writer(w)发送数据。
	// 这里不关心返回的字节数，只关注是否有错误发生。
	_, err := fmt.Fprintf(c.w, format, value)

	// 返回可能的错误，nil表示发送成功。
	return err
}
