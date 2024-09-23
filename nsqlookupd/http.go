package nsqlookupd

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync/atomic"

	"github.com/julienschmidt/httprouter"
	"github.com/zhangweijie11/zNsq/internal/http_api"
	"github.com/zhangweijie11/zNsq/internal/protocol"
	"github.com/zhangweijie11/zNsq/internal/version"
)

type httpServer struct {
	nsqlookupd *NSQLookupd
	router     http.Handler
}

// newHTTPServer 创建并初始化一个新的 HTTP 服务器实例。
// 该函数接收一个 NSQLookupd 实例，用于与 NSQ Lookupd 逻辑交互。
// 返回值是一个初始化完毕的 httpServer 实例。
func newHTTPServer(l *NSQLookupd) *httpServer {
	// 初始化日志记录器，用于记录 HTTP API 的访问日志。
	log := http_api.Log(l.logf)

	// 创建一个新的路由处理器，用于处理 HTTP 请求。
	router := httprouter.New()
	// 配置路由处理器的行为，如处理不存在的路由、方法不允许等。
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(l.logf)
	router.NotFound = http_api.LogNotFoundHandler(l.logf)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(l.logf)

	// 创建 httpServer 实例，并初始化其成员变量。
	s := &httpServer{
		nsqlookupd: l,
		router:     router,
	}

	// 注册基础的 HTTP 处理函数，并为它们添加日志记录和适当的装饰器。
	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.V1))

	// 注册 v1 版本的 HTTP 处理函数，这些函数提供完整的功能集。
	router.Handle("GET", "/debug", http_api.Decorate(s.doDebug, log, http_api.V1))
	router.Handle("GET", "/lookup", http_api.Decorate(s.doLookup, log, http_api.V1))
	router.Handle("GET", "/topics", http_api.Decorate(s.doTopics, log, http_api.V1))
	router.Handle("GET", "/channels", http_api.Decorate(s.doChannels, log, http_api.V1))
	router.Handle("GET", "/nodes", http_api.Decorate(s.doNodes, log, http_api.V1))

	// 注册仅在 v1 版本中可用的 HTTP 处理函数，这些函数提供更高级的功能。
	router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
	router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("POST", "/topic/tombstone", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.V1))

	// 注册一系列用于调试的 HTTP 处理函数，使用 pprof 包来提供性能分析工具。
	router.HandlerFunc("GET", "/debug/pprof", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	// 返回初始化完毕的 HTTP 服务器实例。
	return s
}

// ServeHTTP 处理HTTP请求。
// 该方法是HTTP服务器的请求处理入口。
// 参数:
//
//	w: http.ResponseWriter，用于发送HTTP响应的接口。
//	req: *http.Request，包含HTTP请求的方法、URL、头部等信息的结构体指针。
//
// 通过调用服务器的路由器来分发请求到相应的处理函数。
func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// 调用路由器处理HTTP请求。
	s.router.ServeHTTP(w, req)
}

// pingHandler 是HTTP服务器的路由处理函数。
// 它的主要作用是响应客户端的“ping”请求，验证服务器是否正常运行。
// 该函数不处理任何业务逻辑，只返回一个简单的字符串“OK”和一个空的错误值，
// 表示服务器成功响应了ping请求并且没有遇到任何错误。
//
// 参数:
//
//	w: http.ResponseWriter，用于向客户端发送响应的接口。
//	req: *http.Request，表示客户端请求的只读对象。
//	ps: httprouter.Params，httprouter库使用的路由参数。
//
// 返回值:
//
//	interface{}: 字符串“OK”，表示服务器状态正常。
//	error: nil，表示没有发生任何错误。
func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

// doInfo 返回服务器的版本信息。
// 该方法是 httpServer 的一个方法，用于响应 HTTP 请求。
// 参数:
//
//	w: HTTP 响应的写入器，用于向客户端发送响应。
//	req: HTTP 请求，包含请求的方法、URL、头部等信息。
//	ps: httprouter.Params 是一个键值对集合，这里用于传递路由参数。
//
// 返回值:
//
//	interface{}: 包含服务器版本信息的结构体，以 JSON 格式发送给客户端。
//	error: 如果出现错误，返回 nil，表示没有错误。
func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 构造包含版本信息的结构体，并返回。
	return struct {
		Version string `json:"version"`
	}{
		Version: version.Binary,
	}, nil
}

// doTopics 返回NSQlookupd中注册的所有topic。
// 该函数主要用于获取当前NSQlookupd实例中所有topic的列表。
// 它通过调用DB.FindRegistrations方法，使用"topic"作为参数，
// 搜索所有以"topic"开头的注册项，并返回其键值。
//
// 参数:
//
//	w http.ResponseWriter: HTTP响应写入器，用于向客户端发送响应。
//	req *http.Request: 包含HTTP请求的所有信息。
//	ps httprouter.Params: 从URL路径中提取的参数，可能在此函数中未使用。
//
// 返回:
//
//	interface{}, error: 一个包含"topics"键和所有topic字符串列表的map，
//	以及一个错误值（始终为nil，表示没有错误）。
func (s *httpServer) doTopics(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 使用DB.FindRegistrations方法查找所有以"topic"开头的注册项，
	// 参数"topic"表示查找以"topic"开头的所有项，
	// "*"和空字符串分别表示匹配任何生产者和通道。
	topics := s.nsqlookupd.DB.FindRegistrations("topic", "*", "").Keys()

	// 将找到的topics列表包装在一个map中，以便于返回给调用者。
	return map[string]interface{}{
		"topics": topics,
	}, nil
}

// doChannels 处理获取给定主题下的所有频道的请求。
// 该函数接收一个 HTTP 请求并返回一个包含频道列表的映射，或者返回一个错误。
// 参数:
//
//	w: HTTP 响应写入器，用于发送响应。
//	req: *http.Request 的指针，表示客户端的请求。
//	ps: httprouter.Params 的实例，可能包含额外的路径参数（本例中未使用）。
//
// 返回值:
//
//	interface{}: 成功时返回一个包含 "channels" 键和频道列表值的映射。
//	error: 失败时返回一个错误对象。
func (s *httpServer) doChannels(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 解析请求参数，如果失败则返回错误。
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		// 创建并返回一个指示无效请求的错误。
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 从请求参数中获取主题名称。
	topicName, err := reqParams.Get("topic")
	if err != nil {
		// 如果未找到主题名称，创建并返回一个指示缺少参数的错误。
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	// 通过数据库查找与给定主题相关的所有频道。
	channels := s.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()

	// 准备并返回包含频道列表的响应。
	return map[string]interface{}{
		"channels": channels,
	}, nil
}

// doLookup 处理查找请求，返回指定主题的相关信息。
// 该方法接收一个http.ResponseWriter用于响应，一个http.Request用于请求信息，
// 以及httprouter.Params用于路由参数。
// 参数ps在该函数中未使用，但由httprouter包装器传递。
func (s *httpServer) doLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 解析请求参数，若解析失败，返回错误。
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		// 返回错误响应，指示请求参数无效。
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 从请求参数中获取主题名，若不存在，返回错误。
	topicName, err := reqParams.Get("topic")
	if err != nil {
		// 返回错误响应，指示缺少主题参数。
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	// 在数据库中查找与主题名匹配的注册信息。
	registration := s.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	// 若未找到任何注册信息，返回错误。
	if len(registration) == 0 {
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	// 查找所有与主题名匹配的频道。
	channels := s.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	// 查找所有与主题名匹配的生产者，并根据活跃状态过滤。
	producers := s.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.nsqlookupd.opts.InactiveProducerTimeout,
		s.nsqlookupd.opts.TombstoneLifetime)

	// 构建响应数据，包含频道列表和生产者信息。
	return map[string]interface{}{
		"channels":  channels,
		"producers": producers.PeerInfo(),
	}, nil
}

// doCreateTopic 处理创建主题的请求。
// 该函数接收 HTTP 请求并尝试根据请求参数创建一个新的主题。
// 如果请求无效或缺少参数，函数将返回适当的错误。
// 如果主题创建成功，函数将返回 nil, nil 以指示没有错误。
func (s *httpServer) doCreateTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 解析请求参数，确保请求格式正确。
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		// 如果请求参数无效，返回400错误。
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 从请求参数中获取主题名。
	topicName, err := reqParams.Get("topic")
	if err != nil {
		// 如果缺少主题名参数，返回400错误。
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	// 验证主题名是否有效。
	if !protocol.IsValidTopicName(topicName) {
		// 如果主题名无效，返回400错误。
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	// 记录创建主题的操作。
	s.nsqlookupd.logf(LOG_INFO, "DB: adding topic(%s)", topicName)

	// 向数据库添加主题注册信息。
	key := Registration{"topic", topicName, ""}
	s.nsqlookupd.DB.AddRegistration(key)

	// 主题创建成功，返回 nil, nil 表示没有错误。
	return nil, nil
}

// doDeleteTopic 删除指定的主题。
// 该函数负责处理删除主题的HTTP请求，通过解析请求参数获取主题名称，
// 并从数据库中删除与该主题相关的所有注册信息。
//
// 参数:
//
//	w: HTTP响应写入器，用于发送响应。
//	req: HTTP请求，包含请求参数。
//	ps: 路由参数，用于从URL中提取路径参数（如果有的话）。
//
// 返回值:
//
//	interface{}: 删除操作的结果，如果操作成功，返回nil。
//	error: 错误信息，如果操作过程中发生错误，返回错误信息。
func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 解析请求参数，初始化ReqParams对象。
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		// 如果请求参数无效，返回错误并提示INVALID_REQUEST。
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 从请求参数中获取主题名称。
	topicName, err := reqParams.Get("topic")
	if err != nil {
		// 如果缺少主题名称参数，返回错误并提示MISSING_ARG_TOPIC。
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	// 查找并删除主题下的所有频道注册信息。
	registrations := s.nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	for _, registration := range registrations {
		// 记录删除频道注册信息的日志。
		s.nsqlookupd.logf(LOG_INFO, "DB: removing channel(%s) from topic(%s)", registration.SubKey, topicName)
		// 从数据库中删除频道注册信息。
		s.nsqlookupd.DB.RemoveRegistration(registration)
	}

	// 查找并删除主题本身的注册信息。
	registrations = s.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	for _, registration := range registrations {
		// 记录删除主题注册信息的日志。
		s.nsqlookupd.logf(LOG_INFO, "DB: removing topic(%s)", topicName)
		// 从数据库中删除主题注册信息。
		s.nsqlookupd.DB.RemoveRegistration(registration)
	}

	// 操作成功，返回nil, nil。
	return nil, nil
}

// doTombstoneTopicProducer 对指定主题的生产者设置墓碑标记。
// 该函数通过HTTP请求处理，旨在逻辑删除生产者信息。
// 参数:
//
//	w: http.ResponseWriter，用于向客户端发送响应。
//	req: *http.Request，包含客户端请求的细节。
//	ps: httprouter.Params，当前路由的参数。
//
// 返回值:
//
//	interface{}: 请求处理结果，此处为nil，表示无具体响应体。
//	error: 错误信息，如果操作成功则为nil。
func (s *httpServer) doTombstoneTopicProducer(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 解析请求参数，若失败则返回“INVALID_REQUEST”错误。
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 从请求参数中获取主题名，若缺失则返回“MISSING_ARG_TOPIC”错误。
	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	// 从请求参数中获取节点信息，若缺失则返回“MISSING_ARG_NODE”错误。
	node, err := reqParams.Get("node")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_NODE"}
	}

	// 记录设置生产者墓碑的日志。
	s.nsqlookupd.logf(LOG_INFO, "DB: setting tombstone for producer@%s of topic(%s)", node, topicName)

	// 查找指定主题的所有生产者。
	producers := s.nsqlookupd.DB.FindProducers("topic", topicName, "")
	for _, p := range producers {
		// 格式化当前节点信息，用于匹配请求中的节点。
		thisNode := fmt.Sprintf("%s:%d", p.peerInfo.BroadcastAddress, p.peerInfo.HTTPPort)
		// 如果当前节点与请求中的节点匹配，则对该生产者设置墓碑标记。
		if thisNode == node {
			p.Tombstone()
		}
	}

	// 操作成功，返回nil, nil。
	return nil, nil
}

// doCreateChannel 创建一个NSQ频道并将其添加到指定的主题中。
// 该函数通过HTTP请求接收创建频道的参数，验证请求参数的有效性，
// 并在数据库中注册新的频道和主题。
// 参数:
//
//	w: http.ResponseWriter用于向客户端发送响应。
//	req: *http.Request包含从客户端接收到的请求信息。
//	ps: httprouter.Params包含路由参数（本例中未使用）。
//
// 返回值:
//
//	interface{}: 创建频道的操作结果（本例中总是返回nil）。
//	error: 如果发生错误，返回相应的错误信息，否则返回nil。
func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 解析请求参数，确保请求体结构正确。
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		// 如果请求参数解析失败，返回错误响应。
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 从请求参数中提取主题名和频道名。
	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		// 如果提取主题名和频道名时发生错误，返回错误响应。
		return nil, http_api.Err{400, err.Error()}
	}

	// 在日志中记录添加频道到主题的操作。
	s.nsqlookupd.logf(LOG_INFO, "DB: adding channel(%s) in topic(%s)", channelName, topicName)
	// 构建注册表项Key，并将频道注册到数据库中。
	key := Registration{"channel", topicName, channelName}
	s.nsqlookupd.DB.AddRegistration(key)

	// 在日志中记录添加主题的操作。
	s.nsqlookupd.logf(LOG_INFO, "DB: adding topic(%s)", topicName)
	// 构建注册表项Key，并将主题注册到数据库中。
	key = Registration{"topic", topicName, ""}
	s.nsqlookupd.DB.AddRegistration(key)

	// 操作成功，返回nil, nil表示没有错误发生。
	return nil, nil
}

// doDeleteChannel 处理删除通道的请求。
// 该函数负责解析请求参数，验证通道的存在，并从数据库中移除与该通道相关的所有注册信息。
// 参数:
//
//	w: http.ResponseWriter - 用于向客户端发送响应。
//	req: *http.Request - 包含客户端请求的详细信息。
//	ps: httprouter.Params - 包含URL参数。
//
// 返回值:
//
//	interface{}: 删除操作的结果，这里总是返回nil。
//	error: 如果操作失败，返回错误的详细信息，否则为nil。
func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 解析请求参数，确保请求是有效的。
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 从请求参数中提取主题和通道名称。
	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	// 查找通道在数据库中的注册信息。
	registrations := s.nsqlookupd.DB.FindRegistrations("channel", topicName, channelName)
	if len(registrations) == 0 {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	// 日志记录：开始从数据库中移除通道。
	s.nsqlookupd.logf(LOG_INFO, "DB: removing channel(%s) from topic(%s)", channelName, topicName)
	// 遍历并移除找到的所有注册信息。
	for _, registration := range registrations {
		s.nsqlookupd.DB.RemoveRegistration(registration)
	}

	// 操作成功，返回nil。
	return nil, nil
}

// node 定义了网络节点的信息结构。
// 该结构包含节点的远程地址、主机名、广播地址、TCP端口、HTTP端口、版本信息以及墓碑标记。
type node struct {
	// RemoteAddress 是节点的远程地址。
	RemoteAddress string `json:"remote_address"`
	// Hostname 是节点的主机名。
	Hostname string `json:"hostname"`
	// BroadcastAddress 是节点的广播地址，用于网络发现。
	BroadcastAddress string `json:"broadcast_address"`
	// TCPPort 是节点用于TCP通信的端口。
	TCPPort int `json:"tcp_port"`
	// HTTPPort 是节点用于HTTP服务的端口。
	HTTPPort int `json:"http_port"`
	// Version 是节点的版本信息。
	Version string `json:"version"`
	// Tombstones 是一组墓碑标记，表示节点是否已离线。
	Tombstones []bool `json:"tombstones"`
	// Topics 是节点感兴趣的主題列表。
	Topics []string `json:"topics"`
}

// doNodes 处理与节点相关的HTTP请求。
// 它会返回一个包含生产者信息和墓碑状态的节点列表。
// 参数:
//
//	w: HTTP响应写入器，用于发送响应。
//	req: HTTP请求，未使用，但必须作为参数传递。
//	ps: httprouter.Params，未使用，但必须作为参数传递。
//
// 返回值:
//
//	interface{}: 包含“producers”字段的映射，其值为节点列表。
//	error: 如果发生错误，返回nil。
func (s *httpServer) doNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 不要过滤掉墓碑节点
	producers := s.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(
		s.nsqlookupd.opts.InactiveProducerTimeout, 0)
	nodes := make([]*node, len(producers))
	topicProducersMap := make(map[string]Producers)
	for i, p := range producers {
		topics := s.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()

		// 对于每个主题，找到与该对等节点匹配的生产者，以添加墓碑信息
		tombstones := make([]bool, len(topics))
		for j, t := range topics {
			if _, exists := topicProducersMap[t]; !exists {
				topicProducersMap[t] = s.nsqlookupd.DB.FindProducers("topic", t, "")
			}

			topicProducers := topicProducersMap[t]
			for _, tp := range topicProducers {
				if tp.peerInfo == p.peerInfo {
					tombstones[j] = tp.IsTombstoned(s.nsqlookupd.opts.TombstoneLifetime)
					break
				}
			}
		}

		nodes[i] = &node{
			RemoteAddress:    p.peerInfo.RemoteAddress,
			Hostname:         p.peerInfo.Hostname,
			BroadcastAddress: p.peerInfo.BroadcastAddress,
			TCPPort:          p.peerInfo.TCPPort,
			HTTPPort:         p.peerInfo.HTTPPort,
			Version:          p.peerInfo.Version,
			Tombstones:       tombstones,
			Topics:           topics,
		}
	}

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}

// doDebug 处理调试请求的函数。
// 该函数响应调试相关的HTTP请求，返回NSQ lookupd内部状态的信息。
// 参数:
// - w: http.ResponseWriter，用于向客户端发送响应。
// - req: *http.Request，表示接收到的HTTP请求。
// - ps: httprouter.Params，包含匹配的路由参数。
// 返回值:
// - interface{}: 包含lookupd内部状态的数据，以供调试使用。
// - error: 如果处理过程中出错，返回错误信息。
func (s *httpServer) doDebug(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 对注册表进行只读锁定，以保证并发安全。
	s.nsqlookupd.DB.RLock()
	defer s.nsqlookupd.DB.RUnlock()

	// 初始化一个map，用于存储所有注册的生产者信息。
	data := make(map[string][]map[string]interface{})
	// 遍历注册表中的所有注册信息。
	for r, producers := range s.nsqlookupd.DB.registrationMap {
		// 构造唯一的键，用于区分不同的注册条目。
		key := r.Category + ":" + r.Key + ":" + r.SubKey
		// 遍历当前注册条目下的所有生产者。
		for _, p := range producers {
			// 构造一个map，用于存储当前生产者的信息。
			m := map[string]interface{}{
				"id":                p.peerInfo.id,
				"hostname":          p.peerInfo.Hostname,
				"broadcast_address": p.peerInfo.BroadcastAddress,
				"tcp_port":          p.peerInfo.TCPPort,
				"http_port":         p.peerInfo.HTTPPort,
				"version":           p.peerInfo.Version,
				"last_update":       atomic.LoadInt64(&p.peerInfo.lastUpdate),
				"tombstoned":        p.tombstoned,
				"tombstoned_at":     p.tombstonedAt.UnixNano(),
			}
			// 将当前生产者的信息添加到数据集中。
			data[key] = append(data[key], m)
		}
	}

	// 返回调试数据。
	return data, nil
}
