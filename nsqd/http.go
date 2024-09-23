package nsqd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/zhangweijie11/zNsq/internal/http_api"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"github.com/zhangweijie11/zNsq/internal/protocol"
	"github.com/zhangweijie11/zNsq/internal/version"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

var boolParams = map[string]bool{
	"true":  true,
	"1":     true,
	"false": false,
	"0":     false,
}

// httpServer 结构体定义了HTTP服务器的配置和组件。
// 它用于启动和管理HTTP服务器，提供如检查NSQD状态、管理NSQD配置等接口。
type httpServer struct {
	// nsqd 是NSQD实例的指针，HTTP服务器依赖它来访问和控制NSQD的核心功能。
	nsqd *NSQD

	// tlsEnabled 表示TLS（传输层安全性协议）是否被启用。
	// 当设为true时，HTTP服务器将尝试使用TLS来安全地处理HTTP请求。
	tlsEnabled bool

	// tlsRequired 表示TLS是否是强制性的。
	// 当设为true时，HTTP服务器只接受通过TLS加密的HTTP请求。
	tlsRequired bool

	// router 是HTTP请求的路由处理器。
	// 所有的HTTP请求都会被转发到这个处理器进行进一步的路由和处理。
	router http.Handler
}

func newHTTPServer(nsqd *NSQD, tlsEnabled bool, tlsRequired bool) *httpServer {
	log := http_api.Log(nsqd.logf)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(nsqd.logf)
	router.NotFound = http_api.LogNotFoundHandler(nsqd.logf)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(nsqd.logf)
	s := &httpServer{
		nsqd:        nsqd,
		tlsEnabled:  tlsEnabled,
		tlsRequired: tlsRequired,
		router:      router,
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.V1))

	// v1 negotiate
	router.Handle("POST", "/pub", http_api.Decorate(s.doPUB, http_api.V1))
	router.Handle("POST", "/mpub", http_api.Decorate(s.doMPUB, http_api.V1))
	router.Handle("GET", "/stats", http_api.Decorate(s.doStats, log, http_api.V1))

	// only v1
	router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
	router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
	router.Handle("POST", "/topic/empty", http_api.Decorate(s.doEmptyTopic, log, http_api.V1))
	router.Handle("POST", "/topic/pause", http_api.Decorate(s.doPauseTopic, log, http_api.V1))
	router.Handle("POST", "/topic/unpause", http_api.Decorate(s.doPauseTopic, log, http_api.V1))
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("POST", "/channel/empty", http_api.Decorate(s.doEmptyChannel, log, http_api.V1))
	router.Handle("POST", "/channel/pause", http_api.Decorate(s.doPauseChannel, log, http_api.V1))
	router.Handle("POST", "/channel/unpause", http_api.Decorate(s.doPauseChannel, log, http_api.V1))
	router.Handle("GET", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))
	router.Handle("PUT", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))

	// debug
	router.HandlerFunc("GET", "/debug/pprof/", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handle("PUT", "/debug/setblockrate", http_api.Decorate(setBlockRateHandler, log, http_api.PlainText))
	router.Handle("POST", "/debug/freememory", http_api.Decorate(freeMemory, log, http_api.PlainText))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return s
}

// ServeHTTP处理HTTP请求
// 该方法的主要目的是检查入站HTTP请求是否需要TLS（SSL）
// 如果服务器配置了TLS但请求未使用TLS，该方法会返回一个包含TLS必要信息的错误响应
// 如果TLS不是必需的，或者请求已经使用了TLS，则将请求委托给路由器处理
//
// 参数:
//
//	w: http.ResponseWriter，用于写入响应
//	req: *http.Request，表示接收到的HTTP请求
func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// 检查TLS配置和要求
	if !s.tlsEnabled && s.tlsRequired {
		// 构造告知客户端TLS所需的响应
		resp := fmt.Sprintf(`{"message": "TLS_REQUIRED", "https_port": %d}`,
			s.nsqd.RealHTTPSAddr().Port)

		// 设置响应头，指示内容类型
		w.Header().Set("X-NSQ-Content-Type", "nsq; version=1.0")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		// 发送禁止访问的HTTP状态码（403）
		w.WriteHeader(403)

		// 写入响应内容
		io.WriteString(w, resp)
		return
	}
	// 如果不需要TLS或者已经使用TLS，则继续处理请求
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	health := s.nsqd.GetHealth()
	if !s.nsqd.IsHealthy() {
		return nil, http_api.Err{500, health}
	}
	return health, nil
}

// doInfo 处理获取服务器信息的请求。
// 该函数获取并返回关于当前运行的 NSQd 服务器的基本信息，包括版本、地址、端口等。
// 参数:
//
//	w: http.ResponseWriter，用于向客户端发送响应。
//	req: *http.Request，表示当前的HTTP请求，未使用但必须存在。
//	ps: httprouter.Params，路由参数，未使用但必须存在。
//
// 返回值:
//
//	interface{}: 包含服务器信息的结构体。
//	error: 如果执行过程中出现错误，返回错误信息。
func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 获取主机名，用于响应中标识服务器。
	hostname, err := os.Hostname()
	if err != nil {
		// 如果获取主机名失败，返回500错误和错误详情。
		return nil, http_api.Err{500, err.Error()}
	}

	// 初始化TCP端口为-1，用于非TCP场景（如Unix Socket）。
	tcpPort := -1
	// 如果NSQd的TCP地址是TCP类型，则设置tcpPort为相应的TCP端口。
	if s.nsqd.RealTCPAddr().Network() == "tcp" {
		tcpPort = s.nsqd.RealTCPAddr().(*net.TCPAddr).Port
	}

	// 初始化HTTP端口为-1，用于非TCP场景（如Unix Socket）。
	httpPort := -1
	// 如果NSQd的HTTP地址是TCP类型，则设置httpPort为相应的HTTP端口。
	if s.nsqd.RealHTTPAddr().Network() == "tcp" {
		httpPort = s.nsqd.RealHTTPAddr().(*net.TCPAddr).Port
	}

	// 构建并返回包含服务器信息的结构体。
	return struct {
		Version              string        `json:"version"`
		BroadcastAddress     string        `json:"broadcast_address"`
		Hostname             string        `json:"hostname"`
		HTTPPort             int           `json:"http_port"`
		TCPPort              int           `json:"tcp_port"`
		StartTime            int64         `json:"start_time"`
		MaxHeartBeatInterval time.Duration `json:"max_heartbeat_interval"`
		MaxOutBufferSize     int64         `json:"max_output_buffer_size"`
		MaxOutBufferTimeout  time.Duration `json:"max_output_buffer_timeout"`
		MaxDeflateLevel      int           `json:"max_deflate_level"`
	}{
		Version:              version.Binary,
		BroadcastAddress:     s.nsqd.getOpts().BroadcastAddress,
		Hostname:             hostname,
		TCPPort:              tcpPort,
		HTTPPort:             httpPort,
		StartTime:            s.nsqd.GetStartTime().Unix(),
		MaxHeartBeatInterval: s.nsqd.getOpts().MaxHeartbeatInterval,
		MaxOutBufferSize:     s.nsqd.getOpts().MaxOutputBufferSize,
		MaxOutBufferTimeout:  s.nsqd.getOpts().MaxOutputBufferTimeout,
		MaxDeflateLevel:      s.nsqd.getOpts().MaxDeflateLevel,
	}, nil
}

// getTopicFromQuery 从HTTP请求的查询字符串中提取主题信息。
// 该方法同时解析请求的查询字符串，并根据其中的 topic 参数获取对应的 Topic 实例。
// 参数:
//
//	req - 即将处理的HTTP请求
//
// 返回值:
//
//	url.Values - 解析后的请求查询参数
//	*Topic - 从请求中提取的主题实例指针
//	error - 如果发生错误，返回包含错误信息的http_api.Err类型
func (s *httpServer) getTopicFromQuery(req *http.Request) (url.Values, *Topic, error) {
	// 解析请求的查询字符串
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		// 如果解析失败，记录错误并返回INVALID_REQUEST错误
		s.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 检查是否存在 topic 参数
	topicNames, ok := reqParams["topic"]
	if !ok {
		// 如果缺少 topic 参数，返回MISSING_ARG_TOPIC错误
		return nil, nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	// 获取 topic 参数的第一个值
	topicName := topicNames[0]

	// 验证 topic 名称是否有效
	if !protocol.IsValidTopicName(topicName) {
		// 如果主题名称无效，返回INVALID_TOPIC错误
		return nil, nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	// 返回解析后的查询参数和对应的Topic实例
	return reqParams, s.nsqd.GetTopic(topicName), nil
}

// doPUB 处理HTTP PUB请求，将消息发布到NSQ话题。
// 该函数首先检查请求的消息大小是否超过允许的最大消息大小，
// 然后读取请求的身体内容。接着，它会验证消息是否为空、是否过大，
// 以及计算延迟时间。最后，它会创建一个新的消息对象并将其放入话题中。
// 参数:
//
//	w: HTTP响应作家，用于将响应写回客户端。
//	req: HTTP请求，包含客户端的请求数据。
//	ps: httprouter参数，用于处理URL中的参数。
//
// 返回值:
//
//	接口{}类型，用于返回响应体内容，错误类型，用于返回可能发生的错误。
func (s *httpServer) doPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 检查消息大小是否超过最大限制
	if req.ContentLength > s.nsqd.getOpts().MaxMsgSize {
		return nil, http_api.Err{413, "MSG_TOO_BIG"}
	}

	// 设置读取最大值，以便于检测消息是否超过了最大限制
	readMax := s.nsqd.getOpts().MaxMsgSize + 1
	body, err := io.ReadAll(io.LimitReader(req.Body, readMax))
	if err != nil {
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}
	// 检查消息是否过大
	if int64(len(body)) == readMax {
		return nil, http_api.Err{413, "MSG_TOO_BIG"}
	}
	// 检查消息是否为空
	if len(body) == 0 {
		return nil, http_api.Err{400, "MSG_EMPTY"}
	}

	// 从请求查询参数中获取话题
	reqParams, topic, err := s.getTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	// 计算消息的延迟时间
	var deferred time.Duration
	if ds, ok := reqParams["defer"]; ok {
		var di int64
		di, err = strconv.ParseInt(ds[0], 10, 64)
		if err != nil {
			return nil, http_api.Err{400, "INVALID_DEFER"}
		}
		deferred = time.Duration(di) * time.Millisecond
		// 检查延迟时间是否有效
		if deferred < 0 || deferred > s.nsqd.getOpts().MaxReqTimeout {
			return nil, http_api.Err{400, "INVALID_DEFER"}
		}
	}

	// 创建新的消息对象并放入话题中
	msg := NewMessage(topic.GenerateID(), body)
	msg.deferred = deferred
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, http_api.Err{503, "EXITING"}
	}

	// 返回OK响应
	return "OK", nil
}

// doMPUB 处理HTTP POST请求，用于批量发布消息到NSQ。
// 该函数根据请求参数确定是否采用二进制模式，并对每条消息进行处理。
// 最后，将所有消息发布到指定主题。
func (s *httpServer) doMPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 初始化消息切片
	var msgs []*Message
	// 初始化退出标志，用于文本模式下循环终止
	var exit bool

	// 检查请求体长度是否超过最大限制
	if req.ContentLength > s.nsqd.getOpts().MaxBodySize {
		return nil, http_api.Err{413, "BODY_TOO_BIG"}
	}

	// 从请求中获取主题
	reqParams, topic, err := s.getTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	// 默认为文本模式，若请求参数中存在 unrecognized binary 选项则视为开启二进制模式
	binaryMode := false
	if vals, ok := reqParams["binary"]; ok {
		if binaryMode, ok = boolParams[vals[0]]; !ok {
			binaryMode = true
			s.nsqd.logf(LOG_WARN, "deprecated value '%s' used for /mpub binary param", vals[0])
		}
	}

	// 根据模式读取并处理消息
	if binaryMode {
		tmp := make([]byte, 4)
		msgs, err = readMPUB(req.Body, tmp, topic,
			s.nsqd.getOpts().MaxMsgSize, s.nsqd.getOpts().MaxBodySize)
		if err != nil {
			return nil, http_api.Err{413, err.(*protocol.FatalClientErr).Code[2:]}
		}
	} else {
		readMax := s.nsqd.getOpts().MaxBodySize + 1
		rdr := bufio.NewReader(io.LimitReader(req.Body, readMax))
		total := 0
		for !exit {
			var block []byte
			block, err = rdr.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					return nil, http_api.Err{500, "INTERNAL_ERROR"}
				}
				exit = true
			}
			total += len(block)
			if int64(total) == readMax {
				return nil, http_api.Err{413, "BODY_TOO_BIG"}
			}

			if len(block) > 0 && block[len(block)-1] == '\n' {
				block = block[:len(block)-1]
			}

			if len(block) == 0 {
				continue
			}

			if int64(len(block)) > s.nsqd.getOpts().MaxMsgSize {
				return nil, http_api.Err{413, "MSG_TOO_BIG"}
			}

			msg := NewMessage(topic.GenerateID(), block)
			msgs = append(msgs, msg)
		}
	}

	// 将所有消息发布到主题
	err = topic.PutMessages(msgs)
	if err != nil {
		return nil, http_api.Err{503, "EXITING"}
	}

	return "OK", nil
}

// 创建主题
func (s *httpServer) doCreateTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, _, err := s.getTopicFromQuery(req)
	return nil, err
}

// doEmptyTopic 处理清空主题的请求。
// 该函数接收一个HTTP请求并尝试解析请求参数以找到指定的主题。
// 如果找到主题，则会清空该主题（即移除所有消息）。
// 参数:
//   - w: 响应写入器，用于发送响应。
//   - req: 请求对象，包含请求数据。
//   - ps: 路由参数，可能包含URL中的参数。
//
// 返回值:
//   - interface{}: 响应体的内容（本例中为nil）。
//   - error: 错误对象，如果处理过程中发生错误。
func (s *httpServer) doEmptyTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 解析请求参数。
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		// 如果无法解析请求参数，则记录错误并返回错误信息。
		s.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 从请求参数中获取主题名。
	topicName, err := reqParams.Get("topic")
	if err != nil {
		// 如果缺少主题名参数，则返回错误信息。
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	// 验证主题名是否有效。
	if !protocol.IsValidTopicName(topicName) {
		// 如果主题名无效，则返回错误信息。
		return nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	// 从nsqd实例中获取已存在的主题。
	topic, err := s.nsqd.GetExistingTopic(topicName)
	if err != nil {
		// 如果找不到主题，则返回错误信息。
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	// 尝试清空主题。
	err = topic.Empty()
	if err != nil {
		// 如果清空主题过程中发生错误，则返回错误信息。
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	// 主题清空成功，返回nil和nil表示无错误。
	return nil, nil
}

// doDeleteTopic 处理删除主题的HTTP请求。
// 该函数接收一个HTTP响应写入器，一个HTTP请求和一组HTTP路由参数。
// 它尝试解析请求参数并删除指定的主题。
// 如果成功删除主题或者没有找到该主题，它将返回nil作为主体。
// 如果发生错误，它将返回相应的错误代码和消息。
func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 解析HTTP请求的参数
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		// 如果无法解析请求参数，记录错误并返回BAD_REQUEST错误
		s.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 从请求参数中获取主题名
	topicName, err := reqParams.Get("topic")
	if err != nil {
		// 如果缺少主题名参数，返回MISSING_ARG_TOPIC错误
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	// 尝试删除指定的主题
	err = s.nsqd.DeleteExistingTopic(topicName)
	if err != nil {
		// 如果主题不存在，返回TOPIC_NOT_FOUND错误
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	// 成功删除主题，返回nil表示无错误
	return nil, nil
}

// doPauseTopic 处理暂停或恢复主题的请求。
// 根据HTTP请求中的参数，从nsqd服务器中查找一个现有的主题，
// 并根据URL路径中是否包含"unpause"来决定是暂停还是恢复该主题。
// 在操作前后，会锁定nsqd服务器以持久化元数据。
// 参数:
//
//	w: http.ResponseWriter用于发送HTTP响应
//	req: *http.Request包含HTTP请求的详细信息
//	ps: httprouter.Params包含匹配的路由参数
//
// 返回值:
//
//	interface{}: 操作成功时返回nil，否则返回错误信息
//	error: 操作过程中出现的错误，没有错误时返回nil
func (s *httpServer) doPauseTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 解析请求参数
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 获取主题名称参数
	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	// 根据主题名称获取现有主题实例
	topic, err := s.nsqd.GetExistingTopic(topicName)
	if err != nil {
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	// 根据URL路径决定是暂停还是恢复主题
	if strings.Contains(req.URL.Path, "unpause") {
		err = topic.UnPause()
	} else {
		err = topic.Pause()
	}
	if err != nil {
		s.nsqd.logf(LOG_ERROR, "failure in %s - %s", req.URL.Path, err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	// 锁定nsqd服务器以持久化元数据
	s.nsqd.Lock()
	s.nsqd.PersistMetadata()
	s.nsqd.Unlock()
	return nil, nil
}

func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}
	topic.GetChannel(channelName)
	return nil, nil
}

// doEmptyChannel 清空指定频道的消息。
//
// 该函数通过HTTP请求清空一个频道中的所有消息。步骤如下：
// 1. 从请求中提取并验证主题和频道名称。
// 2. 确保主题存在，并且该主题下存在指定的频道。
// 3. 清空频道中的所有消息。
//
// 参数:
//
//	w: http.ResponseWriter，用于向客户端发送响应。
//	req: *http.Request，包含请求细节，用于提取请求参数和控制请求处理。
//	ps: httprouter.Params，当前路由的参数，可能包括路径参数，但在此函数中未直接使用。
//
// 返回值:
//
//	interface{}: 在成功清空频道消息后，返回nil。
//	error: 如果发生错误，返回具体错误信息；否则返回nil。
func (s *httpServer) doEmptyChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 从请求中提取并验证主题和频道名称。
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	// 确保主题存在，并且该主题下存在指定的频道。
	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		// 如果频道不存在，返回404错误。
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	// 清空频道中的所有消息。
	err = channel.Empty()
	if err != nil {
		// 如果清空消息过程中出现内部错误，返回500错误。
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	// 成功清空频道消息，返回nil。
	return nil, nil
}

// doDeleteChannel 删除指定的频道
//
// 该函数通过HTTP请求方法DELETE来删除一个特定的频道。首先，它会从请求中提取出已存在的主题和频道名称。
// 如果找到了对应的主题和频道名称，那么它会尝试从该主题中删除这个现有的频道。如果删除操作成功，
// 函数将返回nil, nil；如果频道不存在，则返回404错误。
//
// 参数:
//
//	w: http.ResponseWriter，用于向客户端发送响应的接口
//	req: *http.Request，包含客户端请求的HTTP请求
//	ps: httprouter.Params，从URL路径中提取的参数
//
// 返回值:
//
//	interface{}: 删除操作的结果，当前实现中总是返回nil
//	error: 如果在删除频道过程中发生错误，返回相应的错误对象，否则返回nil
func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 从请求中获取现有的主题
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		// 如果无法从请求中正确获取主题，返回错误
		return nil, err
	}

	// 尝试删除主题中的指定频道
	err = topic.DeleteExistingChannel(channelName)
	if err != nil {
		// 如果频道不存在，返回404错误
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	// 频道删除成功，返回nil, nil
	return nil, nil
}

// doPauseChannel 根据请求暂停或恢复给定频道的处理。
// 该函数接收一个 HTTP 请求并根据路径中是否包含 "unpause" 来决定是暂停还是恢复频道。
// 如果路径包含 "unpause"，则调用 UnPause 方法，否则调用 Pause 方法。
// 在操作前后，函数会尝试持久化元数据以确保频道状态的一致性。
// 参数:
//
//	w: HTTP 响应写入器，用于发送响应。
//	req: 指向当前请求的指针，包含请求的相关信息。
//	ps: httprouter 的参数，包含匹配的路由参数。
//
// 返回值:
//
//	interface{}: 响应体的内容，这里总是返回 nil。
//	error: 如果发生错误，返回具体的错误信息。
func (s *httpServer) doPauseChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 从请求中提取现有主题
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	// 从主题中获取现有频道
	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	// 根据请求路径决定暂停还是恢复频道
	if strings.Contains(req.URL.Path, "unpause") {
		err = channel.UnPause()
	} else {
		err = channel.Pause()
	}
	if err != nil {
		// 日志记录操作失败
		s.nsqd.logf(LOG_ERROR, "failure in %s - %s", req.URL.Path, err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	// 为确保进程失败时频道状态不会突然改变，主动持久化元数据
	s.nsqd.Lock()
	s.nsqd.PersistMetadata()
	s.nsqd.Unlock()
	return nil, nil
}

// doStats 处理统计信息的请求。
// 它根据请求参数收集NSQD的统计信息，并以指定的格式返回。
// 参数：
//
//	w: HTTP响应作家
//	req: HTTP请求
//	ps: httprouter的参数
//
// 返回值：
//
//	interface{}: 统计信息数据
//	error: 错误，如果有的话
func (s *httpServer) doStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 解析请求参数
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		// 如果解析失败，记录错误并返回错误信息
		s.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 从请求参数中获取格式、主题、通道及是否包含客户端和内存信息
	formatString, _ := reqParams.Get("format")
	topicName, _ := reqParams.Get("topic")
	channelName, _ := reqParams.Get("channel")
	includeClientsParam, _ := reqParams.Get("include_clients")
	includeMemParam, _ := reqParams.Get("include_mem")
	jsonFormat := formatString == "json"

	// 初始化 includeClients 和 includeMem 变量
	includeClients, ok := boolParams[includeClientsParam]
	if !ok {
		includeClients = true
	}
	includeMem, ok := boolParams[includeMemParam]
	if !ok {
		includeMem = true
	}

	// 从 NSQD 获取统计信息、健康状态、启动时间和运行时间
	stats := s.nsqd.GetStats(topicName, channelName, includeClients)
	health := s.nsqd.GetHealth()
	startTime := s.nsqd.GetStartTime()
	uptime := time.Since(startTime)

	// 根据需要获取内存统计信息
	var ms *memStats
	if includeMem {
		m := getMemStats()
		ms = &m
	}

	// 根据格式返回统计信息
	if !jsonFormat {
		return s.printStats(stats, ms, health, startTime, uptime), nil
	}

	// 返回 JSON 格式的统计信息
	return struct {
		Version   string        `json:"version"`
		Health    string        `json:"health"`
		StartTime int64         `json:"start_time"`
		Topics    []TopicStats  `json:"topics"`
		Memory    *memStats     `json:"memory,omitempty"`
		Producers []ClientStats `json:"producers"`
	}{version.Binary, health, startTime.Unix(), stats.Topics, ms, stats.Producers}, nil
}

// printStats 生成并返回HTTP服务器关于nsqd实例的统计信息的字节切片。
// 它接收Stats对象、内存统计信息的指针、健康状态字符串、启动时间和运行时间作为参数。
func (s *httpServer) printStats(stats Stats, ms *memStats, health string, startTime time.Time, uptime time.Duration) []byte {
	// 初始化缓冲区和写入器以构建统计信息字符串。
	var buf bytes.Buffer
	w := &buf

	// 写入nsqd版本信息。
	fmt.Fprintf(w, "%s\n", version.String("nsqd"))
	// 写入nsqd启动时间。
	fmt.Fprintf(w, "start_time %v\n", startTime.Format(time.RFC3339))
	// 写入nsqd运行时间。
	fmt.Fprintf(w, "uptime %s\n", uptime)

	// 添加健康状态信息。
	fmt.Fprintf(w, "\nHealth: %s\n", health)

	// 如果内存统计信息非空，写入内存相关信息。
	if ms != nil {
		fmt.Fprintf(w, "\nMemory:\n")
		fmt.Fprintf(w, "   %-25s\t%d\n", "heap_objects", ms.HeapObjects)
		fmt.Fprintf(w, "   %-25s\t%d\n", "heap_idle_bytes", ms.HeapIdleBytes)
		fmt.Fprintf(w, "   %-25s\t%d\n", "heap_in_use_bytes", ms.HeapInUseBytes)
		fmt.Fprintf(w, "   %-25s\t%d\n", "heap_released_bytes", ms.HeapReleasedBytes)
		fmt.Fprintf(w, "   %-25s\t%d\n", "gc_pause_usec_100", ms.GCPauseUsec100)
		fmt.Fprintf(w, "   %-25s\t%d\n", "gc_pause_usec_99", ms.GCPauseUsec99)
		fmt.Fprintf(w, "   %-25s\t%d\n", "gc_pause_usec_95", ms.GCPauseUsec95)
		fmt.Fprintf(w, "   %-25s\t%d\n", "next_gc_bytes", ms.NextGCBytes)
		fmt.Fprintf(w, "   %-25s\t%d\n", "gc_total_runs", ms.GCTotalRuns)
	}

	// 根据Topics的长度，写入相应的Topics信息。
	if len(stats.Topics) == 0 {
		fmt.Fprintf(w, "\nTopics: None\n")
	} else {
		fmt.Fprintf(w, "\nTopics:")
	}

	// 遍历Topics，写入每个Topic及其Channel的详细统计信息。
	for _, t := range stats.Topics {
		var pausedPrefix string
		if t.Paused {
			pausedPrefix = "*P "
		} else {
			pausedPrefix = "   "
		}
		fmt.Fprintf(w, "\n%s[%-15s] depth: %-5d be-depth: %-5d msgs: %-8d e2e%%: %s\n",
			pausedPrefix,
			t.TopicName,
			t.Depth,
			t.BackendDepth,
			t.MessageCount,
			t.E2eProcessingLatency,
		)
		for _, c := range t.Channels {
			if c.Paused {
				pausedPrefix = "   *P "
			} else {
				pausedPrefix = "      "
			}
			fmt.Fprintf(w, "%s[%-25s] depth: %-5d be-depth: %-5d inflt: %-4d def: %-4d re-q: %-5d timeout: %-5d msgs: %-8d e2e%%: %s\n",
				pausedPrefix,
				c.ChannelName,
				c.Depth,
				c.BackendDepth,
				c.InFlightCount,
				c.DeferredCount,
				c.RequeueCount,
				c.TimeoutCount,
				c.MessageCount,
				c.E2eProcessingLatency,
			)
			for _, client := range c.Clients {
				fmt.Fprintf(w, "        %s\n", client)
			}
		}
	}

	// 根据Producers的长度，写入相应的Producers信息。
	if len(stats.Producers) == 0 {
		fmt.Fprintf(w, "\nProducers: None\n")
	} else {
		fmt.Fprintf(w, "\nProducers:\n")
		for _, client := range stats.Producers {
			fmt.Fprintf(w, "   %s\n", client)
		}
	}

	// 返回包含统计信息的字节切片。
	return buf.Bytes()
}

// doConfig 处理配置相关的信息。
// 该函数支持通过PUT请求来更新nsqd的运行时配置选项。
// 支持的选项包括"nsqlookupd_tcp_addresses"和"log_level"。
// 通过查询字符串'opt'来指定具体要更新的选项。
// 参数:
//
//	w: http.ResponseWriter用于发送响应。
//	req: http.Request包含请求的详细信息。
//	ps: httprouter.Params用于提取URL参数。
//
// 返回值:
//
//	interface{}: 成功时返回请求的配置值。
//	error: 如果发生错误，返回相应的错误。
func (s *httpServer) doConfig(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	opt := ps.ByName("opt")

	// 处理PUT请求，更新配置选项。
	if req.Method == "PUT" {
		// 设置一个读取上限，用于检查上传的配置大小是否超过最大消息大小限制。
		readMax := s.nsqd.getOpts().MaxMsgSize + 1
		body, err := io.ReadAll(io.LimitReader(req.Body, readMax))
		if err != nil {
			// 如果读取请求体失败，返回内部错误。
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
		// 如果请求体为空或超过限制，返回无效请求错误。
		if int64(len(body)) == readMax || len(body) == 0 {
			return nil, http_api.Err{413, "INVALID_VALUE"}
		}

		opts := *s.nsqd.getOpts()
		// 根据请求参数opt来切换不同的配置更新逻辑。
		switch opt {
		case "nsqlookupd_tcp_addresses":
			// 更新NSQLookupd的TCP地址列表。
			err := json.Unmarshal(body, &opts.NSQLookupdTCPAddresses)
			if err != nil {
				// 如果JSON反序列化失败，返回无效值错误。
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
		case "log_level":
			// 更新日志级别。
			logLevelStr := string(body)
			logLevel, err := lg.ParseLogLevel(logLevelStr)
			if err != nil {
				// 如果解析日志级别失败，返回无效值错误。
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
			opts.LogLevel = logLevel
		default:
			// 如果请求的配置选项不受支持，返回无效选项错误。
			return nil, http_api.Err{400, "INVALID_OPTION"}
		}
		// 应用新的配置选项，并触发配置变更通知。
		s.nsqd.swapOpts(&opts)
		s.nsqd.triggerOptsNotification()
	}

	// 检索并返回请求的配置值。
	v, ok := getOptByCfgName(s.nsqd.getOpts(), opt)
	if !ok {
		// 如果请求的配置选项不存在，返回无效选项错误。
		return nil, http_api.Err{400, "INVALID_OPTION"}
	}

	return v, nil
}

// getExistingTopicFromQuery 从HTTP请求中获取现有的主题。
// 此函数解析请求参数，提取主题和频道名称，并确保主题存在。
// 如果主题不存在或参数无效，将返回相应的错误。
// 参数:
//   - req: HTTP请求对象
//
// 返回值:
//   - *http_api.ReqParams: 解析后的请求参数
//   - *Topic: 现有的主题对象
//   - string: 频道名称
//   - error: 错误对象，如果操作成功则为nil
func (s *httpServer) getExistingTopicFromQuery(req *http.Request) (*http_api.ReqParams, *Topic, string, error) {
	// 解析请求参数
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		// 如果请求参数解析失败，记录错误并返回
		s.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, nil, "", http_api.Err{400, "INVALID_REQUEST"}
	}

	// 从请求参数中提取主题和频道名称
	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		// 如果提取失败，返回错误
		return nil, nil, "", http_api.Err{400, err.Error()}
	}

	// 确保主题存在
	topic, err := s.nsqd.GetExistingTopic(topicName)
	if err != nil {
		// 如果主题不存在，返回错误
		return nil, nil, "", http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	// 返回解析后的请求参数、主题对象和频道名称
	return reqParams, topic, channelName, err
}

// getOptByCfgName 通过配置名称获取选项值。
//
// opts 接口类型，包含配置选项。
// name 字符串类型，要查找的配置选项名称。
//
// 返回值：
// - interface{}：如果找到匹配的配置选项，则返回该选项的值。
// - bool：表示是否找到匹配的配置选项。
//
// 该函数通过反射处理 opts 参数中的配置选项，根据 name 查找对应的值并返回。
// 如果没有找到匹配的配置选项，则返回 nil 和 false。
func getOptByCfgName(opts interface{}, name string) (interface{}, bool) {
	// 获取 opts 指针指向的值。
	val := reflect.ValueOf(opts).Elem()
	// 获取 opts 的类型，用于遍历其字段。
	typ := val.Type()

	// 遍历 opts 的字段。
	for i := 0; i < typ.NumField(); i++ {
		// 获取当前字段的信息。
		field := typ.Field(i)
		// 获取字段上的 "flag" 和 "cfg" 标签值。
		flagName := field.Tag.Get("flag")
		cfgName := field.Tag.Get("cfg")

		// 如果字段没有 "flag" 标签，则跳过。
		if flagName == "" {
			continue
		}

		// 如果字段没有 "cfg" 标签，则使用 "flag" 标签值的下划线版本作为默认的 "cfg" 名称。
		if cfgName == "" {
			cfgName = strings.Replace(flagName, "-", "_", -1)
		}

		// 如果给定的名称与当前字段的 "cfg" 名称匹配，则返回字段的值和 true。
		if name == cfgName {
			return val.FieldByName(field.Name).Interface(), true
		}
	}

	// 如果没有找到匹配的字段，返回 nil 和 false。
	return nil, false
}

// setBlockRateHandler处理设置阻塞率的请求。
// 该处理器接收一个HTTP请求，从中提取阻塞率（rate），将其转换为整数，
// 并使用runtime包的SetBlockProfileRate函数设置新的阻塞率。
// 如果转换过程中出现错误，将返回错误响应。
// 参数:
//
//	w: HTTP响应写入者，用于向客户端发送响应。
//	req: HTTP请求，从中提取阻塞率参数。
//	ps: httprouter参数，未在此函数中使用。
//
// 返回值:
//
//	interface{}: 响应体，此函数不返回具体内容，所以总是nil。
//	error: 错误，如果发生错误，则返回一个自定义的错误对象。
func setBlockRateHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 从请求中提取阻塞率参数并转换为整数。
	rate, err := strconv.Atoi(req.FormValue("rate"))
	if err != nil {
		// 如果转换失败，返回一个带有详细错误信息的HTTP错误响应。
		return nil, http_api.Err{http.StatusBadRequest, fmt.Sprintf("invalid block rate : %s", err.Error())}
	}
	// 设置新的阻塞率。
	runtime.SetBlockProfileRate(rate)
	// 成功设置阻塞率后，返回nil，nil表示没有错误。
	return nil, nil
}

// freeMemory 立即触发操作系统的内存回收机制。
// 该函数主要用于调试目的，以帮助开发者在需要的时候手动释放空闲内存。
// 参数:
//
//	w: http.ResponseWriter - 用于向客户端发送响应的接口。
//	req: *http.Request - 表示HTTP请求的指针。
//	ps: httprouter.Params - 包含URL参数的集合。
//
// 返回值:
//
//	interface{}, error: 该函数不返回任何有意义的数据，所以返回nil, nil。
//
// 注意: 该函数的调用不保证一定能释放内存给操作系统，这取决于操作系统的实现。
func freeMemory(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 调用debug.FreeOSMemory()函数，请求操作系统回收空闲内存。
	debug.FreeOSMemory()
	// 由于该函数不产生任何有用的输出，所以返回两个nil值。
	return nil, nil
}
