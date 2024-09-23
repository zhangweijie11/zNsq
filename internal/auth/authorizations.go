package auth

import (
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/zhangweijie11/zNsq/internal/http_api"
	"math/rand"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type Authorization struct {
	Topic       string   `json:"topic"`
	Channels    []string `json:"channels"`
	Permissions []string `json:"permissions"`
}

type State struct {
	TTL            int             `json:"ttl"`
	Authorizations []Authorization `json:"authorizations"`
	Identity       string          `json:"identity"`
	IdentityURL    string          `json:"identity_url"`
	Expires        time.Time
}

// IsAllowed 检查给定的主题和频道是否被允许访问。
// 该方法遍历State对象中的所有授权对象（Authorizations），
// 并检查每个授权对象是否允许指定的主题和频道组合。
// 如果有任何一个授权对象允许访问，则返回true；否则返回false。
func (a *State) IsAllowed(topic, channel string) bool {
	// 遍历所有授权对象，检查是否允许访问
	for _, aa := range a.Authorizations {
		// 如果当前授权对象允许访问，则返回true
		if aa.IsAllowed(topic, channel) {
			return true
		}
	}
	// 所有授权对象都不允许访问，返回false
	return false
}

// IsExpired 检查状态是否过期。
//
// 该方法通过比较当前时间与状态的过期时间来判断状态是否过期。
// 如果状态的过期时间在当前时间之前，则返回true，表示状态已过期；
// 否则返回false，表示状态未过期。
//
// 返回值:
//   - true，如果状态已过期。
//   - false，如果状态未过期。
func (a *State) IsExpired() bool {
	return a.Expires.Before(time.Now())
}

// IsAllowed 判断在给定的主题和频道上执行操作是否被允许。
// topic: 订阅或发布主题。
// channel: 订阅频道，如果是发布操作则为空。
// 返回值: 如果操作被允许则为true，否则为false。
func (a *Authorization) IsAllowed(topic, channel string) bool {
	// 检查频道是否为空来决定需要的权限。
	if channel != "" {
		// 如果频道不为空且没有订阅权限，则不允许操作。
		if !a.HasPermission("subscribe") {
			return false
		}
	} else {
		// 如果频道为空且没有发布权限，则不允许操作。
		if !a.HasPermission("publish") {
			return false
		}
	}

	// 使用正则表达式检查主题是否匹配授权中的主题模式。
	topicRegex := regexp.MustCompile(a.Topic)
	if !topicRegex.MatchString(topic) {
		return false
	}

	// 遍历授权中的频道列表，检查是否有匹配的频道。
	for _, c := range a.Channels {
		channelRegex := regexp.MustCompile(c)
		if channelRegex.MatchString(channel) {
			return true
		}
	}
	return false
}

// HasPermission 检查用户是否拥有指定的权限。
// 该方法遍历用户的所有权限，如果找到与请求权限相匹配的权限，则返回 true，否则返回 false。
// 参数:
//
//	permission - 用户请求的权限。
//
// 返回值:
//
//	如果用户拥有请求的权限，则返回 true；否则返回 false。
func (a *Authorization) HasPermission(permission string) bool {
	for _, p := range a.Permissions {
		if permission == p {
			return true
		}
	}
	return false
}

// QueryAnyAuthd 尝试使用提供的认证参数列表对远程服务进行认证。
// 该函数通过循环尝试每个认证参数，直到至少有一个成功为止。
// 参数:
// - authd: 一个认证参数列表，用于尝试认证。
// - remoteIP: 远程服务的IP地址。
// - tlsEnabled: 标志位，表示是否启用TLS。
// - commonName: TLS证书的通用名称。
// - authSecret: 认证所需的密钥或令牌。
// - clientTLSConfig: TLS配置对象，用于TLS连接。
// - connectTimeout: 建立连接的超时时间。
// - requestTimeout: 请求的超时时间。
// - httpRequestMethod: 发送请求时使用的HTTP方法（如GET、POST）。
// 返回值:
// - *State: 认证成功时返回的认证状态对象，认证失败时返回nil。
// - error: 如果所有认证尝试都失败，则返回错误信息；否则返回nil。
func QueryAnyAuthd(authd []string, remoteIP string, tlsEnabled bool, commonName string, authSecret string,
	clientTLSConfig *tls.Config, connectTimeout time.Duration, requestTimeout time.Duration, httpRequestMethod string) (*State, error) {
	// 初始化错误变量，用于累积尝试认证失败的错误信息。
	var retErr error

	// 生成随机数作为起始索引，以随机顺序尝试认证参数，提高并发效率。
	start := rand.Int()

	// 计算认证参数列表的长度。
	n := len(authd)

	// 循环尝试每个认证参数。
	for i := 0; i < n; i++ {
		// 计算当前尝试的认证参数索引，使用随机起始索引错开尝试顺序。
		a := authd[(i+start)%n]

		// 使用当前认证参数尝试认证。
		authState, err := QueryAuthd(a, remoteIP, tlsEnabled, commonName, authSecret, clientTLSConfig, connectTimeout, requestTimeout, httpRequestMethod)
		if err != nil {
			// 如果认证失败，构造错误信息。
			es := fmt.Sprintf("failed to auth against %s - %s", a, err)
			// 如果已有错误信息，则将新错误信息追加到已有错误信息之后。
			if retErr != nil {
				es = fmt.Sprintf("%s; %s", retErr, es)
			}
			// 更新错误信息。
			retErr = errors.New(es)
			// 继续尝试下一个认证参数。
			continue
		}
		// 如果认证成功，返回认证状态。
		return authState, nil
	}

	// 如果所有认证尝试都失败，返回累积的错误信息。
	return nil, retErr
}

// QueryAuthd 通过HTTP请求从远程认证服务获取授权状态。
// 该函数支持POST和GET两种HTTP请求方法。
//
// 参数:
// - authd: 认证服务的地址，可以是"http(s)://<host>:<port>"格式的完整URL，或简化的"<host>:<port>"。
// - remoteIP: 客户端的远程IP地址，用于认证过程。
// - tlsEnabled: 表示是否启用TLS连接。
// - commonName: TLS证书的通用名称。
// - authSecret: 用于认证的秘密密钥。
// - clientTLSConfig: 客户端TLS配置，用于建立TLS连接（如果启用）。
// - connectTimeout: 建立连接的超时时间。
// - requestTimeout: 请求的超时时间。
// - httpRequestMethod: HTTP请求方法，可以是"post"或"get"。
//
// 返回值:
// - *State: 包含授权信息的状态对象，如果认证成功且授权信息有效，则返回。
// - error: 如果认证失败或发生错误，则返回错误信息。
func QueryAuthd(authd string, remoteIP string, tlsEnabled bool, commonName string, authSecret string,
	clientTLSConfig *tls.Config, connectTimeout time.Duration, requestTimeout time.Duration, httpRequestMethod string) (*State, error) {
	// 初始化State对象，用于存储认证和授权信息。
	var authState State
	// 构建URL参数。
	v := url.Values{}
	v.Set("remote_ip", remoteIP)
	if tlsEnabled {
		v.Set("tls", "true")
	} else {
		v.Set("tls", "false")
	}
	v.Set("secret", authSecret)
	v.Set("common_name", commonName)

	// 确定认证服务的endpoint。
	var endpoint string
	if strings.Contains(authd, "://") {
		endpoint = authd
	} else {
		endpoint = fmt.Sprintf("http://%s/auth", authd)
	}

	// 创建HTTP客户端。
	client := http_api.NewClient(clientTLSConfig, connectTimeout, requestTimeout)
	// 根据请求方法发起请求。
	if httpRequestMethod == "post" {
		if err := client.POSTV1(endpoint, v, &authState); err != nil {
			return nil, err
		}
	} else {
		endpoint = fmt.Sprintf("%s?%s", endpoint, v.Encode())
		if err := client.GETV1(endpoint, &authState); err != nil {
			return nil, err
		}
	}

	// 验证响应中的授权信息。
	for _, auth := range authState.Authorizations {
		for _, p := range auth.Permissions {
			switch p {
			case "subscribe", "publish":
			default:
				return nil, fmt.Errorf("unknown permission %s", p)
			}
		}

		if _, err := regexp.Compile(auth.Topic); err != nil {
			return nil, fmt.Errorf("unable to compile topic %q %s", auth.Topic, err)
		}

		for _, channel := range auth.Channels {
			if _, err := regexp.Compile(channel); err != nil {
				return nil, fmt.Errorf("unable to compile channel %q %s", channel, err)
			}
		}
	}

	// 验证TTL的有效性，并计算过期时间。
	if authState.TTL <= 0 {
		return nil, fmt.Errorf("invalid TTL %d (must be >0)", authState.TTL)
	}

	authState.Expires = time.Now().Add(time.Duration(authState.TTL) * time.Second)
	return &authState, nil
}
