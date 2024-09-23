package http_api

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	c *http.Client
}

// NewClient 创建并返回一个新的Client实例。
// 该函数接受TLS配置、连接超时时间和请求超时时间作为参数，
// 用于配置HTTP客户端的行为。
//
// 参数:
//   - tlsConfig: 指定用于安全连接的TLS配置。
//   - connectTimeout: 指定连接超时时间。
//   - requestTimeout: 指定请求超时时间。
//
// 返回值:
//   - *Client: 返回一个配置好的Client实例指针。
func NewClient(tlsConfig *tls.Config, connectTimeout time.Duration, requestTimeout time.Duration) *Client {
	// 创建一个带有连接和请求超时时间的自定义Transport。
	transport := NewDeadlineTransport(connectTimeout, requestTimeout)
	// 配置Transport的TLS客户端配置。
	transport.TLSClientConfig = tlsConfig

	// 使用自定义Transport和请求超时时间创建HTTP客户端，并将其包装在Client结构体中返回。
	return &Client{
		c: &http.Client{
			Transport: transport,
			Timeout:   requestTimeout,
		},
	}
}

// NewDeadlineTransport 创建一个具有指定连接和请求超时设置的 http.Transport 实例。
//
// 参数:
//
//	connectTimeout: 连接超时时间，表示建立连接的最长时间。
//	requestTimeout: 请求超时时间，表示读取响应的最长时间。
//
// 返回值:
//
//	*http.Transport: 一个初始化了超时设置的 http.Transport 实例。
func NewDeadlineTransport(connectTimeout time.Duration, requestTimeout time.Duration) *http.Transport {
	// 使用指定的连接超时时间和请求超时时间初始化 transport。
	// 其他默认配置值基于 http.DefaultTransport 进行设置。
	transport := &http.Transport{
		// 配置 DialContext 以使用指定的连接超时时间。
		DialContext: (&net.Dialer{
			Timeout:   connectTimeout,
			KeepAlive: 30 * time.Second, // 保持连接的存活时间。
			DualStack: true,             // 支持 IPv4 和 IPv6 双栈。
		}).DialContext,

		// 设置响应头的读取超时时间。
		ResponseHeaderTimeout: requestTimeout,

		// 设置空闲连接池中的最大连接数。
		MaxIdleConns: 100,

		// 设置连接空闲状态的超时时间。
		IdleConnTimeout: 90 * time.Second,

		// 设置 TLS 握手的超时时间。
		TLSHandshakeTimeout: 10 * time.Second,
	}

	// 返回配置好的 http.Transport 实例。
	return transport
}

// GETV1 向指定的endpoint发送HTTP GET请求，并将响应的JSON数据解析到给定的结构体中。
// endpoint: 请求的URL路径。
// v: 解析JSON响应数据的目标结构体指针。
// 返回值: 如果发生错误，则返回错误；否则返回nil。
func (c *Client) GETV1(endpoint string, v interface{}) error {
retry:
	// 创建一个新的HTTP GET请求。
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}

	// 设置请求头，指定接受的内容类型。
	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	// 发送请求并获取响应。
	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}

	// 读取响应体并关闭响应体。
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}

	// 检查响应状态码。
	if resp.StatusCode != 200 {
		// 如果状态码是403且请求不是HTTPS，则尝试获取HTTPS endpoint并重试请求。
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		// 如果状态码不是200，则返回错误。
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}

	// 将响应体的JSON数据解析到目标结构体。
	err = json.Unmarshal(body, &v)
	if err != nil {
		return err
	}

	return nil
}

// POSTV1 向指定的endpoint发送POST请求，数据格式为JSON。
// endpoint 表示请求的URL地址。
// data 为要发送的数据，以键值对形式表示。
// v 是解析JSON响应的目标接口，函数会将响应体解析到此接口指向的结构体中。
// 返回错误信息，如果请求成功且不解析响应体则返回nil。
func (c *Client) POSTV1(endpoint string, data url.Values, v interface{}) error {
retry:
	// 根据data是否为空，初始化请求体
	var reqBody io.Reader
	if data != nil {
		js, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal POST data to endpoint: %v", endpoint)
		}
		reqBody = bytes.NewBuffer(js)
	}

	// 创建POST请求
	req, err := http.NewRequest("POST", endpoint, reqBody)
	if err != nil {
		return err
	}

	// 设置请求头，指定接受的内容类型
	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")
	// 如果请求体不为空，设置Content-Type
	if reqBody != nil {
		req.Header.Add("Content-Type", "application/json")
	}

	// 发送请求
	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}

	// 读取响应体并关闭响应体
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}

	// 检查响应状态码
	if resp.StatusCode != 200 {
		// 如果状态码是403且endpoint不是HTTPS，则尝试获取HTTPS endpoint并重试
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}

	// 如果v不为空，则将响应体解析为v指定的结构体
	if v != nil {
		return json.Unmarshal(body, &v)
	}

	return nil
}

// httpsEndpoint 根据给定的端点和主体内容生成一个HTTPS端点。
// 该函数首先尝试解析传入的endpoint字符串，然后使用body中的信息更新endpoint使其指向HTTPS端口。
// 如果解析或更新过程中出现任何错误，函数将返回错误。
// 参数:
//   - endpoint: 需要转换的HTTP端点字符串。
//   - body: 包含HTTPS端口信息的字节切片。
//
// 返回值:
//   - string: 转换后的HTTPS端点字符串。
//   - error: 如果在处理过程中发生错误，则返回错误。
func httpsEndpoint(endpoint string, body []byte) (string, error) {
	// 定义一个结构体用于解析body中的JSON数据，特别是提取HTTPS端口信息。
	var forbiddenResp struct {
		HTTPSPort int `json:"https_port"`
	}
	// 尝试解析body中的JSON数据，将HTTPS端口信息赋值给forbiddenResp。
	err := json.Unmarshal(body, &forbiddenResp)
	if err != nil {
		// 如果解析失败，返回错误。
		return "", err
	}

	// 尝试解析传入的endpoint字符串，为后续转换为HTTPS端点做准备。
	u, err := url.Parse(endpoint)
	if err != nil {
		// 如果解析endpoint失败，返回错误。
		return "", err
	}

	// 从url的主机端口部分分离出主机名，用于构造新的HTTPS端点。
	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		// 如果分离主机名失败，返回错误。
		return "", err
	}

	// 更新url方案为HTTPS，并结合分离出的主机名和解析出的HTTPS端口，构造新的HTTPS端点。
	u.Scheme = "https"
	u.Host = net.JoinHostPort(host, strconv.Itoa(forbiddenResp.HTTPSPort))
	// 返回构造完成的HTTPS端点字符串。
	return u.String(), nil
}
