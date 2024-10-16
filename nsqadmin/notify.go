package nsqadmin

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// AdminAction 该结构体用于记录管理员操作的相关信息。
type AdminAction struct {
	Action    string `json:"action"`            // 操作类型
	Topic     string `json:"topic"`             // 操作涉及的主题
	Channel   string `json:"channel,omitempty"` // 操作涉及的频道，可选
	Node      string `json:"node,omitempty"`    // 操作涉及的节点，可选
	Timestamp int64  `json:"timestamp"`         // 操作的时间戳
	User      string `json:"user,omitempty"`    // 执行操作的用户，可选
	RemoteIP  string `json:"remote_ip"`         // 执行操作时的远程IP地址
	UserAgent string `json:"user_agent"`        // 执行操作时的用户代理字符串
	URL       string `json:"url"`               // The URL of the HTTP request that triggered this action
	Via       string `json:"via"`               // the Hostname of the nsqadmin performing this action
}

// basicAuthUser 从HTTP请求中提取基本认证的用户名。
// 该函数首先从请求头中获取"Authorization"字段，期望其以"Basic"开头。
// 如果请求头格式不正确或未提供，则返回空字符串。
// 函数名"basicAuthUser"意为此函数用于提取基本认证用户名。
func basicAuthUser(req *http.Request) string {
	// 分割"Authorization"头，最多分割一次。
	s := strings.SplitN(req.Header.Get("Authorization"), " ", 2)
	// 检查分割结果是否为"Basic"加上编码后的认证信息。
	if len(s) != 2 || s[0] != "Basic" {
		return ""
	}
	// 解码Base64编码的认证信息。
	b, err := base64.StdEncoding.DecodeString(s[1])
	if err != nil {
		return ""
	}
	// 将解码后的认证信息按冒号分割为用户名和密码。
	pair := strings.SplitN(string(b), ":", 2)
	if len(pair) != 2 {
		return ""
	}
	// 返回提取到的用户名。
	return pair[0]
}

// notifyAdminAction 检查是否需要向管理员发送通知
// 根据提供的参数组装通知内容，并通过HTTP或HTTPS发送通知
// 参数：
// - action: 管理操作类型
// - topic: 操作涉及的主题
// - channel: 操作涉及的频道
// - node: 操作涉及的节点
// - req: HTTP请求，用于确定通知的来源信息
func (s *httpServer) notifyAdminAction(action, topic, channel, node string, req *http.Request) {
	// 如果没有配置通知的HTTP端点，则无需发送通知，直接返回
	if s.nsqadmin.getOpts().NotificationHTTPEndpoint == "" {
		return
	}
	// 获取当前主机名，用于通知中的"via"字段
	via, _ := os.Hostname()

	// 构建通知的目标URL
	u := url.URL{
		Scheme:   "http", // 默认使用HTTP协议
		Host:     req.Host,
		Path:     req.URL.Path,
		RawQuery: req.URL.RawQuery,
	}
	// 根据请求的TLS信息或请求头中的X-Scheme值判断是否应使用HTTPS
	if req.TLS != nil || req.Header.Get("X-Scheme") == "https" {
		u.Scheme = "https"
	}

	// 创建AdminAction实例，包含所有通知相关的信息
	a := &AdminAction{
		Action:    action,
		Topic:     topic,
		Channel:   channel,
		Node:      node,
		Timestamp: time.Now().Unix(),
		User:      basicAuthUser(req),
		RemoteIP:  req.RemoteAddr,
		UserAgent: req.UserAgent(),
		URL:       u.String(),
		Via:       via,
	}
	// 在新的goroutine中执行通知发送，以异步方式处理，避免阻塞当前goroutine
	go func() { s.nsqadmin.notifications <- a }()
}
