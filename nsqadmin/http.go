package nsqadmin

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/zhangweijie11/zNsq/internal/clusterinfo"
	"github.com/zhangweijie11/zNsq/internal/http_api"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"github.com/zhangweijie11/zNsq/internal/protocol"
	"github.com/zhangweijie11/zNsq/internal/version"
)

// maybeWarnMsg 检查消息数组并可能返回警告消息。
// 该函数接受一个字符串切片作为参数，该切片包含需要向用户报告的警告消息。
// 如果消息切片非空，则函数拼接所有消息并返回一条综合警告消息。
// 如果消息切片为空，则不返回任何内容。
//
// 参数:
//
//	msgs []string: 包含一个或多个需要报告给用户的警告消息的切片。
//
// 返回值:
//
//	string: 如果有警告消息，则返回格式化的警告消息字符串；否则返回空字符串。
func maybeWarnMsg(msgs []string) string {
	if len(msgs) > 0 {
		// 当消息切片非空时，拼接所有消息并返回警告消息。
		return "WARNING: " + strings.Join(msgs, "; ")
	}
	// 当消息切片为空时，返回空字符串。
	return ""
}

// NewSingleHostReverseProxy 创建一个针对单个目标主机的反向代理。
// 该函数接收一个目标URL、连接超时时间和请求超时时间作为参数，
// 并返回一个指向httputil.ReverseProxy类型的指针。
// 参数target指定要代理的目标服务器的URL。
// connectTimeout是建立连接的超时时间，requestTimeout是请求的超时时间。
// 返回的反向代理实例可以用于转发和代理请求到指定的目标服务器。
func NewSingleHostReverseProxy(target *url.URL, connectTimeout time.Duration, requestTimeout time.Duration) *httputil.ReverseProxy {
	// director函数用于修改传入的http请求，
	// 将其URL的Scheme和Host更改为target的Scheme和Host。
	// 如果target的URL包含用户名和密码，则将它们设置为基本认证。
	director := func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		if target.User != nil {
			passwd, _ := target.User.Password()
			req.SetBasicAuth(target.User.Username(), passwd)
		}
	}

	// 返回一个配置了自定义director和Transport的httputil.ReverseProxy实例。
	// director用于指导请求转发，Transport用于处理HTTP传输，
	// 并且设置了连接和请求的超时时间。
	return &httputil.ReverseProxy{
		Director:  director,
		Transport: http_api.NewDeadlineTransport(connectTimeout, requestTimeout),
	}
}

// httpServer 结构体定义了运行NSQAdmin服务所需的组件和配置。
// 它包含了与NSQ通信、路由处理、API客户端、集群信息以及静态文件服务相关的对象。
type httpServer struct {
	// nsqadmin 是指向NSQAdmin实例的指针，用于管理NSQ相关功能。
	nsqadmin *NSQAdmin

	// router 是HTTP请求的路由处理器，用于根据请求URL分发请求。
	router http.Handler

	// client 是指向http_api.Client实例的指针，用于发起HTTP API请求。
	client *http_api.Client

	// ci 是指向clusterinfo.ClusterInfo实例的指针，用于存储和管理集群信息。
	ci *clusterinfo.ClusterInfo

	// basePath 定义了服务的基路径，用于路由处理。
	basePath string

	// devStaticDir 指定了开发环境下的静态文件目录，用于静态文件服务。
	devStaticDir string
}

// NewHTTPServer 创建并初始化一个新的 HTTP 服务器实例。
// 该函数接收一个 NSQAdmin 实例作为参数，用于配置和初始化 HTTP 服务器。
// 返回值是初始化完成的 httpServer 实例。
func NewHTTPServer(nsqadmin *NSQAdmin) *httpServer {
	// 初始化日志对象
	log := http_api.Log(nsqadmin.logf)

	// 创建 HTTP 客户端
	client := http_api.NewClient(nsqadmin.httpClientTLSConfig, nsqadmin.getOpts().HTTPClientConnectTimeout,
		nsqadmin.getOpts().HTTPClientRequestTimeout)

	// 初始化路由
	router := httprouter.New()
	// 允许方法不存在
	router.HandleMethodNotAllowed = true
	// 设置 panic 处理器
	router.PanicHandler = http_api.LogPanicHandler(nsqadmin.logf)
	// 设置未找到路由时的处理器
	router.NotFound = http_api.LogNotFoundHandler(nsqadmin.logf)
	// 设置方法不允许时的处理器
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(nsqadmin.logf)

	// 初始化 httpServer 实例
	s := &httpServer{
		nsqadmin: nsqadmin,
		router:   router,
		client:   client,
		ci:       clusterinfo.New(nsqadmin.logf, client),

		basePath:     nsqadmin.getOpts().BasePath,
		devStaticDir: nsqadmin.getOpts().DevStaticDir,
	}

	// 创建一个闭包函数，用于生成完整的路由路径
	bp := func(p string) string {
		return path.Join(s.basePath, p)
	}

	// 配置各种 HTTP 路由和处理器
	router.Handle("GET", bp("/"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/ping"), http_api.Decorate(s.pingHandler, log, http_api.PlainText))

	router.Handle("GET", bp("/topics"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/topics/:topic"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/topics/:topic/:channel"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/nodes"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/nodes/:node"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/counter"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/lookup"), http_api.Decorate(s.indexHandler, log))

	router.Handle("GET", bp("/static/:asset"), http_api.Decorate(s.staticAssetHandler, log, http_api.PlainText))
	router.Handle("GET", bp("/fonts/:asset"), http_api.Decorate(s.staticAssetHandler, log, http_api.PlainText))
	if s.nsqadmin.getOpts().ProxyGraphite {
		proxy := NewSingleHostReverseProxy(nsqadmin.graphiteURL, nsqadmin.getOpts().HTTPClientConnectTimeout,
			nsqadmin.getOpts().HTTPClientRequestTimeout)
		router.Handler("GET", bp("/render"), proxy)
	}

	// 配置 v1 API 路由
	router.Handle("GET", bp("/api/topics"), http_api.Decorate(s.topicsHandler, log, http_api.V1))
	router.Handle("GET", bp("/api/topics/:topic"), http_api.Decorate(s.topicHandler, log, http_api.V1))
	router.Handle("GET", bp("/api/topics/:topic/:channel"), http_api.Decorate(s.channelHandler, log, http_api.V1))
	router.Handle("GET", bp("/api/nodes"), http_api.Decorate(s.nodesHandler, log, http_api.V1))
	router.Handle("GET", bp("/api/nodes/:node"), http_api.Decorate(s.nodeHandler, log, http_api.V1))
	router.Handle("POST", bp("/api/topics"), http_api.Decorate(s.createTopicChannelHandler, log, http_api.V1))
	router.Handle("POST", bp("/api/topics/:topic"), http_api.Decorate(s.topicActionHandler, log, http_api.V1))
	router.Handle("POST", bp("/api/topics/:topic/:channel"), http_api.Decorate(s.channelActionHandler, log, http_api.V1))
	router.Handle("DELETE", bp("/api/nodes/:node"), http_api.Decorate(s.tombstoneNodeForTopicHandler, log, http_api.V1))
	router.Handle("DELETE", bp("/api/topics/:topic"), http_api.Decorate(s.deleteTopicHandler, log, http_api.V1))
	router.Handle("DELETE", bp("/api/topics/:topic/:channel"), http_api.Decorate(s.deleteChannelHandler, log, http_api.V1))
	router.Handle("GET", bp("/api/counter"), http_api.Decorate(s.counterHandler, log, http_api.V1))
	router.Handle("GET", bp("/api/graphite"), http_api.Decorate(s.graphiteHandler, log, http_api.V1))
	router.Handle("GET", bp("/config/:opt"), http_api.Decorate(s.doConfig, log, http_api.V1))
	router.Handle("PUT", bp("/config/:opt"), http_api.Decorate(s.doConfig, log, http_api.V1))

	// 返回初始化完成的 httpServer 实例
	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

// indexHandler 处理索引页面的请求。
// 该处理器主要负责将请求的index.html页面进行动态渲染，并考虑了服务器的配置和状态。
// 参数:
//
//	w: http.ResponseWriter，用于向客户端发送响应。
//	req: *http.Request，表示客户端的请求。
//	ps: httprouter.Params，包含路由参数。
//
// 返回值:
//
//	第一个返回值是接口类型，用于返回给客户端的响应体，在本函数中不使用。
//	第二个返回值是error类型，用于返回执行过程中的错误，在本函数中不使用。
func (s *httpServer) indexHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 获取静态资源"index.html"，用于后续的模板渲染。
	asset, _ := staticAsset("index.html")
	// 基于获取的静态资源创建模板，并定义模板函数basePath，用于生成正确的基础路径。
	t, _ := template.New("index").Funcs(template.FuncMap{
		"basePath": func(p string) string {
			return path.Join(s.basePath, p)
		},
	}).Parse(string(asset))

	// 设置响应头的Content-Type为text/html，告知客户端响应的是HTML文档。
	w.Header().Set("Content-Type", "text/html")
	// 执行模板，将服务器的相关信息和配置传递给模板进行渲染。
	t.Execute(w, struct {
		Version             string
		ProxyGraphite       bool
		GraphEnabled        bool
		GraphiteURL         string
		StatsdInterval      int
		StatsdCounterFormat string
		StatsdGaugeFormat   string
		StatsdPrefix        string
		NSQLookupd          []string
		IsAdmin             bool
	}{
		Version:             version.Binary,
		ProxyGraphite:       s.nsqadmin.getOpts().ProxyGraphite,
		GraphEnabled:        s.nsqadmin.getOpts().GraphiteURL != "",
		GraphiteURL:         s.nsqadmin.getOpts().GraphiteURL,
		StatsdInterval:      int(s.nsqadmin.getOpts().StatsdInterval / time.Second),
		StatsdCounterFormat: s.nsqadmin.getOpts().StatsdCounterFormat,
		StatsdGaugeFormat:   s.nsqadmin.getOpts().StatsdGaugeFormat,
		StatsdPrefix:        s.nsqadmin.getOpts().StatsdPrefix,
		NSQLookupd:          s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		IsAdmin:             s.isAuthorizedAdminRequest(req),
	})

	// 完成模板渲染后，返回nil表示没有需要写入响应体的内容，也没有错误发生。
	return nil, nil
}

// staticAssetHandler 处理静态资源的请求。
// 它根据请求的资源名称从文件系统或嵌入的静态资源中加载资源。
// 如果在开发模式下，并且指定了开发静态资源目录，则从该目录读取资源。
// 否则，从嵌入的静态资源中读取。
// 它还负责根据资源的文件扩展名设置正确的Content-Type响应头。
func (s *httpServer) staticAssetHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 从请求参数中提取资源名称。
	assetName := ps.ByName("asset")

	// 定义资产和错误变量，以便在读取资产时使用。
	var (
		asset []byte
		err   error
	)

	// 如果指定了开发模式下的静态目录，使用该目录中的资源。
	if s.devStaticDir != "" {
		s.nsqadmin.logf(LOG_DEBUG, "using dev dir %q for static asset %q", s.devStaticDir, assetName)
		fsPath := path.Join(s.devStaticDir, assetName)
		asset, err = os.ReadFile(fsPath)
	} else {
		// 否则，从嵌入的静态资源中读取。
		asset, err = staticAsset(assetName)
	}

	// 如果读取资源时发生错误，返回404错误。
	if err != nil {
		return nil, http_api.Err{404, "NOT_FOUND"}
	}

	// 获取资源的文件扩展名，用于确定Content-Type。
	ext := path.Ext(assetName)
	// 尝试通过扩展名确定MIME类型。
	ct := mime.TypeByExtension(ext)
	if ct == "" {
		// 对于未识别的扩展名，手动指定一些常见类型。
		switch ext {
		case ".map":
			ct = "application/json"
		case ".svg":
			ct = "image/svg+xml"
		case ".woff":
			ct = "application/font-woff"
		case ".ttf":
			ct = "application/font-sfnt"
		case ".eot":
			ct = "application/vnd.ms-fontobject"
		case ".woff2":
			ct = "application/font-woff2"
		}
	}
	// 如果确定了Content-Type，设置响应头。
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}

	// 返回资源内容作为字符串，以及无错误状态。
	return string(asset), nil
}

// topicsHandler 是一个处理HTTP请求的函数，用于获取NSQ话题信息。
// 它根据不同的条件从NSQLookupd或NSQD获取话题，并可选地返回非活动话题信息。
// 参数:
//
//	w: http.ResponseWriter，用于向客户端发送响应。
//	req: *http.Request，表示来自客户端的请求。
//	ps: httprouter.Params，路由参数。
//
// 返回值:
//
//	interface{}: 响应体中的数据，包括话题信息。
//	error: 如果发生错误，则返回错误信息。
func (s *httpServer) topicsHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 初始化一个字符串切片，用于存储消息
	var messages []string

	// 解析请求参数，如果出错，则返回错误
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	// 根据配置获取话题信息
	var topics []string
	if len(s.nsqadmin.getOpts().NSQLookupdHTTPAddresses) != 0 {
		topics, err = s.ci.GetLookupdTopics(s.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
	} else {
		topics, err = s.ci.GetNSQDTopics(s.nsqadmin.getOpts().NSQDHTTPAddresses)
	}
	// 处理获取话题信息时的错误
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get topics - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 检查是否需要返回非活动话题信息
	inactive, _ := reqParams.Get("inactive")
	if inactive == "true" {
		topicChannelMap := make(map[string][]string)
		if len(s.nsqadmin.getOpts().NSQLookupdHTTPAddresses) == 0 {
			goto respond
		}
		for _, topicName := range topics {
			producers, _ := s.ci.GetLookupdTopicProducers(
				topicName, s.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
			if len(producers) == 0 {
				topicChannels, _ := s.ci.GetLookupdTopicChannels(
					topicName, s.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
				topicChannelMap[topicName] = topicChannels
			}
		}
	respond:
		return struct {
			Topics  map[string][]string `json:"topics"`
			Message string              `json:"message"`
		}{topicChannelMap, maybeWarnMsg(messages)}, nil
	}

	// 返回活动话题信息
	return struct {
		Topics  []string `json:"topics"`
		Message string   `json:"message"`
	}{topics, maybeWarnMsg(messages)}, nil
}

// topicHandler 处理针对特定主题的请求。
// 该函数获取指定主题的生产者信息和主题统计信息，并返回聚合后的主题统计信息。
// 参数:
// - w: HTTP 响应作家，用于发送响应。
// - req: HTTP 请求，包含请求信息。
// - ps: 包含请求参数的httprouter.Params，用于提取路径参数。
// 返回值:
// - interface{}: 聚合后的主题统计信息，包括主题名称和可能的警告信息。
// - error: 遇到错误时返回。
func (s *httpServer) topicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 初始化一个用于存储消息的切片，用于后续可能的警告信息汇总。
	var messages []string

	// 从路径参数中提取主题名称。
	topicName := ps.ByName("topic")

	// 通过集群信息控制器获取主题的生产者列表。
	// 如果获取失败，检查错误是否为 PartialErr，如果不是，则记录错误并返回502错误。
	producers, err := s.ci.GetTopicProducers(topicName,
		s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get topic producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 从 NSQD 实例获取主题的统计信息。
	// 如果获取失败，检查错误是否为 PartialErr，如果不是，则记录错误并返回502错误。
	topicStats, _, err := s.ci.GetNSQDStats(producers, topicName, "", false)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get topic metadata - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 创建一个全局的主题统计对象，并将所有节点的主题统计信息聚合到该对象中。
	allNodesTopicStats := &clusterinfo.TopicStats{TopicName: topicName}
	for _, t := range topicStats {
		allNodesTopicStats.Add(t)
	}

	// 返回聚合后的主题统计信息以及可能的警告信息。
	return struct {
		*clusterinfo.TopicStats
		Message string `json:"message"`
	}{allNodesTopicStats, maybeWarnMsg(messages)}, nil
}

// channelHandler 处理针对特定主题和通道的HTTP请求。
// 它会获取主题的生产者信息，并检索通道的统计信息。
// 如果出现错误，它会记录错误并返回相应的错误信息。
// 参数:
//
//	w: HTTP响应作家，用于将响应写回客户端。
//	req: HTTP请求，包含请求的方法、URL等信息。
//	ps: 包含从请求URL中提取的参数的httprouter.Params结构。
//
// 返回值:
//
//	interface{}: 包含要返回给客户端的响应体，通常是一个结构。
//	error: 如果处理过程中发生错误，返回该错误。
func (s *httpServer) channelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 初始化一个用于存储错误信息的消息切片。
	var messages []string

	// 从请求参数中提取主题名和通道名。
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")

	// 通过集群信息管理器获取主题的生产者列表。
	producers, err := s.ci.GetTopicProducers(topicName,
		s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		// 判断错误类型，如果是部分错误，则记录警告日志并收集错误信息。
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			// 对于非部分错误，记录错误日志并返回错误响应。
			s.nsqadmin.logf(LOG_ERROR, "failed to get topic producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		// 记录部分错误的警告日志，并将错误信息添加到消息列表中。
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 通过生产者列表获取NSQD的统计信息，包括指定的主题和通道。
	_, channelStats, err := s.ci.GetNSQDStats(producers, topicName, channelName, true)
	if err != nil {
		// 处理获取通道元数据时的错误，逻辑与获取生产者信息时类似。
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get channel metadata - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 返回包含通道统计信息和可能的警告消息的结构体。
	// 如果有警告消息，则将其添加到返回的结构体中。
	return struct {
		*clusterinfo.ChannelStats
		Message string `json:"message"`
	}{channelStats[channelName], maybeWarnMsg(messages)}, nil
}

// nodesHandler 处理与获取节点相关HTTP请求的函数。
// 它从NSQ lookupd和nsqd获取生产者节点信息，并将其返回。
// 参数:
// - w: HTTP响应写入器，用于发送响应。
// - req: HTTP请求，包含请求信息。
// - ps: httprouter的参数，用于匹配URL中的参数。
// 返回值:
// - interface{}: 包含节点信息和可能的消息。
// - error: 在处理请求时发生的错误（如果有）。
func (s *httpServer) nodesHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	// 从lookupd和nsqd地址列表中获取生产者节点信息。
	producers, err := s.ci.GetProducers(s.nsqadmin.getOpts().NSQLookupdHTTPAddresses, s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		// 判断错误是否为部分错误。
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			// 记录错误日志，并返回502错误。
			s.nsqadmin.logf(LOG_ERROR, "failed to get nodes - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		// 记录警告日志，并将错误信息添加到消息列表中。
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 返回生产者节点信息和可能的警告消息。
	return struct {
		Nodes   clusterinfo.Producers `json:"nodes"`
		Message string                `json:"message"`
	}{producers, maybeWarnMsg(messages)}, nil
}

// nodeHandler 处理针对特定节点的请求。
// 它接收一个HTTP请求，查询相应的节点信息，并返回该节点的统计信息。
// w: 用于向客户端发送响应的ResponseWriter。
// req: 包含客户端请求的详细信息的HttpRequest。
// ps: 从URL路径提取的参数。
// 返回值:
// - 一个interface{}，通常用于向客户端返回JSON格式的数据。
// - 一个error，表示处理请求时是否发生了错误。
func (s *httpServer) nodeHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 用于存储可能的警告信息。
	var messages []string

	// 从URL参数中提取节点名称。
	node := ps.ByName("node")

	// 获取NSQ集群中的生产者列表。
	producers, err := s.ci.GetProducers(s.nsqadmin.getOpts().NSQLookupdHTTPAddresses, s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		// 判断错误类型是否为PartialErr，如果不是，记录错误并返回502错误。
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		// 如果是PartialErr，记录警告信息。
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 根据节点名称查找对应的生产者。
	producer := producers.Search(node)
	if producer == nil {
		// 如果找不到对应的生产者，返回404错误。
		return nil, http_api.Err{404, "NODE_NOT_FOUND"}
	}

	// 获取找到的生产者的统计信息。
	topicStats, _, err := s.ci.GetNSQDStats(clusterinfo.Producers{producer}, "", "", true)
	if err != nil {
		// 如果获取统计信息失败，记录错误并返回502错误。
		s.nsqadmin.logf(LOG_ERROR, "failed to get nsqd stats - %s", err)
		return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
	}

	// 计算所有主题的总客户端数和总消息数。
	var totalClients int64
	var totalMessages int64
	for _, ts := range topicStats {
		for _, cs := range ts.Channels {
			totalClients += int64(len(cs.Clients))
		}
		totalMessages += ts.MessageCount
	}

	// 准备要返回的响应体。
	return struct {
		Node          string                    `json:"node"`
		TopicStats    []*clusterinfo.TopicStats `json:"topics"`
		TotalMessages int64                     `json:"total_messages"`
		TotalClients  int64                     `json:"total_clients"`
		Message       string                    `json:"message"`
	}{
		Node:          node,
		TopicStats:    topicStats,
		TotalMessages: totalMessages,
		TotalClients:  totalClients,
		Message:       maybeWarnMsg(messages),
	}, nil
}

// tombstoneNodeForTopicHandler处理为特定主题在节点上设置墓碑的请求。
// 该方法需要管理员权限才能执行操作。
// 参数包括响应对象、请求对象和路由参数。
func (s *httpServer) tombstoneNodeForTopicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// messages用于存储处理过程中可能产生的警告信息。
	var messages []string

	// 检查请求是否为授权的管理员请求。
	if !s.isAuthorizedAdminRequest(req) {
		return nil, http_api.Err{403, "FORBIDDEN"}
	}

	// 从路由参数中提取节点名称。
	node := ps.ByName("node")

	// 解析请求体，期望包含主题信息。
	var body struct {
		Topic string `json:"topic"`
	}
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_BODY"}
	}

	// 验证主题名称是否有效。
	if !protocol.IsValidTopicName(body.Topic) {
		return nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	// 尝试在集群信息中为指定主题和节点设置墓碑。
	err = s.ci.TombstoneNodeForTopic(body.Topic, node,
		s.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
	if err != nil {
		// 错误处理：区分普通错误和部分错误。
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			// 记录严重错误，并返回上游错误指示。
			s.nsqadmin.logf(LOG_ERROR, "failed to tombstone node for topic - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		// 对于部分错误，记录警告信息。
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 通知管理员动作。
	s.notifyAdminAction("tombstone_topic_producer", body.Topic, "", node, req)

	// 准备并返回响应。
	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

// createTopicChannelHandler 处理创建主题或频道的请求。
// 该函数仅由授权的管理员请求调用。
// 参数：
//
//	w: http.ResponseWriter，用于向客户端发送响应。
//	req: *http.Request，表示当前的HTTP请求。
//	ps: httprouter.Params，包含匹配的URL参数。
//
// 返回值：
//
//	interface{}: 成功时返回一个包含消息的结构体。
//	error: 失败时返回一个错误，包含错误代码和消息。
func (s *httpServer) createTopicChannelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 初始化一个字符串切片，用于存储可能的警告消息。
	var messages []string

	// 定义一个结构体，用于解析请求体中的JSON数据。
	var body struct {
		Topic   string `json:"topic"`   // 主题名
		Channel string `json:"channel"` // 频道名
	}

	// 如果请求未被授权为管理员，则返回403错误。
	if !s.isAuthorizedAdminRequest(req) {
		return nil, http_api.Err{403, "FORBIDDEN"}
	}

	// 解析请求体中的JSON数据。
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	// 验证主题名是否有效。
	if !protocol.IsValidTopicName(body.Topic) {
		return nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	// 如果提供了频道名，则验证频道名是否有效。
	if len(body.Channel) > 0 && !protocol.IsValidChannelName(body.Channel) {
		return nil, http_api.Err{400, "INVALID_CHANNEL"}
	}

	// 尝试创建主题或频道。
	err = s.ci.CreateTopicChannel(body.Topic, body.Channel,
		s.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
	if err != nil {
		// 根据错误类型进行日志记录和错误处理。
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to create topic/channel - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 通知管理员创建主题的操作。
	s.notifyAdminAction("create_topic", body.Topic, "", "", req)
	// 如果创建了频道，则通知管理员创建频道的操作。
	if len(body.Channel) > 0 {
		s.notifyAdminAction("create_channel", body.Topic, body.Channel, "", req)
	}

	// 返回结果或错误。
	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

// deleteTopicHandler 处理删除主题的HTTP请求。
// 该函数需要管理员权限进行操作，通过验证后，会尝试删除给定名称的主题。
// 如果删除过程中出现错误，会根据错误类型返回相应的响应。
// 参数:
//
//	w: http.ResponseWriter - 用于发送HTTP响应的接口。
//	req: *http.Request - 包含HTTP请求信息的结构体。
//	ps: httprouter.Params - 包含URL参数的结构体。
//
// 返回值:
//
//	interface{} - 删除主题操作的结果信息。
//	error - 如果发生错误，返回一个错误实例。
func (s *httpServer) deleteTopicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 初始化一个用于存储可能的错误信息的切片。
	var messages []string

	// 检查当前请求是否为授权的管理员请求。
	if !s.isAuthorizedAdminRequest(req) {
		return nil, http_api.Err{403, "FORBIDDEN"}
	}

	// 从URL参数中获取主题名称。
	topicName := ps.ByName("topic")

	// 尝试删除指定的主题。
	err := s.ci.DeleteTopic(topicName,
		s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		// 根据错误类型进行处理。
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			// 记录错误并返回502错误响应。
			s.nsqadmin.logf(LOG_ERROR, "failed to delete topic - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		// 对部分错误进行记录并添加错误信息到messages切片。
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 通知管理员动作：删除主题。
	s.notifyAdminAction("delete_topic", topicName, "", "", req)

	// 返回删除主题操作的结果信息。
	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

// deleteChannelHandler 处理删除频道的请求。
// 该函数首先检查请求是否为授权的管理员请求，然后根据httprouter.Params中的参数
// 删除指定的主题和频道。最后，通过HTTP响应返回处理结果。
//
// 参数:
//
//	w: http.ResponseWriter，用于发送HTTP响应。
//	req: *http.Request，表示HTTP请求。
//	ps: httprouter.Params，包含路由参数。
//
// 返回值:
//
//	interface{}: 删除操作的响应数据，此处为包含消息的结构体。
//	error: 如果发生错误，则返回相应的错误类型。
func (s *httpServer) deleteChannelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	// 检查请求是否为授权的管理员请求
	if !s.isAuthorizedAdminRequest(req) {
		return nil, http_api.Err{403, "FORBIDDEN"}
	}

	// 从路由参数中获取主题和频道名称
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")

	// 尝试删除指定的主题和频道
	err := s.ci.DeleteChannel(topicName, channelName,
		s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		// 根据错误类型进行处理
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			// 记录错误并返回502错误
			s.nsqadmin.logf(LOG_ERROR, "failed to delete channel - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		// 记录警告信息并将错误信息添加到messages列表
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 通知管理员进行了删除频道的操作
	s.notifyAdminAction("delete_channel", topicName, channelName, "", req)

	// 返回删除操作的结果消息
	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

// topicActionHandler 是一个处理主题操作的函数，主要负责根据HTTP请求和路由参数对特定的主题执行预定义的操作。
// 该函数是一个HTTP请求处理函数，它使用了httprouter库来处理路由。
// 参数:
//
//	w: http.ResponseWriter，用于向客户端发送响应的接口。
//	req: *http.Request，表示客户端请求的请求头和请求体。
//	ps: httprouter.Params，包含从请求URL中提取的路径参数。
//
// 返回值:
//
//	interface{}: 操作执行的结果，类型可以根据具体实现而变化。
//	error: 如果操作过程中发生错误，返回错误信息；否则返回nil。
func (s *httpServer) topicActionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 从路由参数中提取主题名称。这是通过httprouter库处理路由时自动填充的。
	topicName := ps.ByName("topic")
	// 调用topicChannelAction处理特定主题的操作，将请求、主题名称和一个空字符串作为参数传递。
	// 空字符串是该函数的第三个参数，用于处理其他相关的操作或标识，具体用途依赖于topicChannelAction的实现。
	return s.topicChannelAction(req, topicName, "")
}

// channelActionHandler 是一个处理频道操作的函数。
// 该函数根据HTTP请求和路由参数中的主题名与频道名，
// 调用topicChannelAction方法来执行相应的业务逻辑。
//
// 参数:
// - w: http.ResponseWriter，用于发送响应到客户端。
// - req: *http.Request，表示客户端的请求。
// - ps: httprouter.Params，包含匹配的路由参数。
//
// 返回值:
// - interface{}，表示响应的数据，可以是任意类型，具体取决于业务逻辑。
// - error，如果执行过程中发生错误，返回错误信息。
func (s *httpServer) channelActionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 从路由参数中提取主题名
	topicName := ps.ByName("topic")
	// 从路由参数中提取频道名
	channelName := ps.ByName("channel")
	// 调用topicChannelAction方法处理请求，并传入主题名与频道名
	return s.topicChannelAction(req, topicName, channelName)
}

// topicChannelAction 处理针对指定主题或频道的管理操作。
// 该函数根据HTTP请求中的动作（如暂停、恢复、清空）对指定的主题或频道进行相应的操作。
// 参数:
//
//	req (*http.Request): 客户端的HTTP请求。
//	topicName (string): 要操作的主题名称。
//	channelName (string): 要操作的频道名称（可选）。
//
// 返回值:
//
//	(interface{}, error): 操作结果和可能的错误。
func (s *httpServer) topicChannelAction(req *http.Request, topicName string, channelName string) (interface{}, error) {
	// 定义一个字符串切片，用于存储操作过程中可能产生的警告信息。
	var messages []string

	// 定义一个结构体，用于解析请求体中的操作类型。
	var body struct {
		Action string `json:"action"`
	}

	// 检查请求是否来自授权的管理员，如果不是，则返回403错误。
	if !s.isAuthorizedAdminRequest(req) {
		return nil, http_api.Err{403, "FORBIDDEN"}
	}

	// 解析请求体，提取操作类型。
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		// 如果解析失败，返回400错误和解析错误信息。
		return nil, http_api.Err{400, err.Error()}
	}

	// 根据请求中的操作类型执行相应的业务逻辑。
	switch body.Action {
	case "pause":
		// 暂停操作，针对主题或频道。
		if channelName != "" {
			// 操作特定频道。
			err = s.ci.PauseChannel(topicName, channelName,
				s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("pause_channel", topicName, channelName, "", req)
		} else {
			// 操作整个主题。
			err = s.ci.PauseTopic(topicName,
				s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("pause_topic", topicName, "", "", req)
		}
	case "unpause":
		// 恢复操作，针对主题或频道。
		if channelName != "" {
			// 操作特定频道。
			err = s.ci.UnPauseChannel(topicName, channelName,
				s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("unpause_channel", topicName, channelName, "", req)
		} else {
			// 操作整个主题。
			err = s.ci.UnPauseTopic(topicName,
				s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("unpause_topic", topicName, "", "", req)
		}
	case "empty":
		// 清空操作，针对主题或频道。
		if channelName != "" {
			// 操作特定频道。
			err = s.ci.EmptyChannel(topicName, channelName,
				s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("empty_channel", topicName, channelName, "", req)
		} else {
			// 操作整个主题。
			err = s.ci.EmptyTopic(topicName,
				s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("empty_topic", topicName, "", "", req)
		}
	default:
		// 如果请求的操作类型不受支持，返回400错误。
		return nil, http_api.Err{400, "INVALID_ACTION"}
	}

	// 处理操作过程中可能产生的错误。
	if err != nil {
		// 判断错误类型，记录相应的日志，并可能添加警告信息。
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			// 对于非部分错误，记录错误日志并返回502错误。
			s.nsqadmin.logf(LOG_ERROR, "failed to %s topic/channel - %s", body.Action, err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		// 对于部分错误，记录警告日志并添加警告信息。
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 返回操作结果，可能包含警告消息。
	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

// counterStats 结构体用于存储计数器统计信息。
// 它包括以下字段：
// - Node: 节点名称，表示消息所在的节点。
// - TopicName: 主题名称，表示消息所属的主题。
// - ChannelName: 频道名称，表示消息所在的频道。
// - MessageCount: 消息计数，表示在特定节点、主题和频道下统计的消息数量。
type counterStats struct {
	Node         string `json:"node"`          // 节点名称
	TopicName    string `json:"topic_name"`    // 主题名称
	ChannelName  string `json:"channel_name"`  // 频道名称
	MessageCount int64  `json:"message_count"` // 消息计数
}

// counterHandler 处理统计信息请求，主要关注消息计数。
// 它从nsqd获取生产者列表，并收集每个通道的统计信息，然后汇总并返回这些统计信息。
// 参数:
//
//	w: http.ResponseWriter用于发送响应。
//	req: *http.Request表示入站请求。
//	ps: httprouter.Params包含从URL路径中提取的参数。
//
// 返回值:
//
//	interface{}: 包含统计信息的结构体，用于生成JSON响应。
//	error: 如果处理过程中出现错误，返回该错误。
func (s *httpServer) counterHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// messages用于存储可能的警告信息。
	var messages []string
	// stats用于汇总和存储所有统计信息。
	stats := make(map[string]*counterStats)

	// 从nsqlookupd和nsqd获取生产者列表。
	producers, err := s.ci.GetProducers(s.nsqadmin.getOpts().NSQLookupdHTTPAddresses, s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		// 判断错误类型，如果是PartialErr，则记录警告并添加到messages中。
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get counter producer list - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 获取nsqd的统计信息。
	_, channelStats, err := s.ci.GetNSQDStats(producers, "", "", false)
	if err != nil {
		// 同样的错误处理逻辑。
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get nsqd stats - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	// 遍历所有通道统计信息，汇总消息计数。
	for _, channelStats := range channelStats {
		for _, hostChannelStats := range channelStats.NodeStats {
			// 使用主题名、通道名和节点地址作为唯一键。
			key := fmt.Sprintf("%s:%s:%s", channelStats.TopicName, channelStats.ChannelName, hostChannelStats.Node)
			// 如果键不存在，则创建新的counterStats。
			s, ok := stats[key]
			if !ok {
				s = &counterStats{
					Node:        hostChannelStats.Node,
					TopicName:   channelStats.TopicName,
					ChannelName: channelStats.ChannelName,
				}
				stats[key] = s
			}
			// 累加消息计数。
			s.MessageCount += hostChannelStats.MessageCount
		}
	}

	// 返回统计信息和可能的警告消息。
	return struct {
		Stats   map[string]*counterStats `json:"stats"`
		Message string                   `json:"message"`
	}{stats, maybeWarnMsg(messages)}, nil
}

// graphiteHandler 处理来自HTTP的graphite相关请求。
// 它接收一个ResponseWriter来响应请求，一个*http.Request对象来处理请求，
// 以及httprouter.Params对象来处理URL参数。
// 返回一个interface{}类型的数据和一个可能的错误。
func (s *httpServer) graphiteHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 解析请求参数，如果解析失败，返回错误。
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 获取metric参数，如果参数不存在或者值不是"rate"，返回错误。
	metric, err := reqParams.Get("metric")
	if err != nil || metric != "rate" {
		return nil, http_api.Err{400, "INVALID_ARG_METRIC"}
	}

	// 获取target参数，如果参数不存在，返回错误。
	target, err := reqParams.Get("target")
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TARGET"}
	}

	// 构建请求参数，用于向graphite发起请求。
	params := url.Values{}
	params.Set("from", fmt.Sprintf("-%dsec", s.nsqadmin.getOpts().StatsdInterval*2/time.Second))
	params.Set("until", fmt.Sprintf("-%dsec", s.nsqadmin.getOpts().StatsdInterval/time.Second))
	params.Set("format", "json")
	params.Set("target", target)
	query := fmt.Sprintf("/render?%s", params.Encode())
	url := s.nsqadmin.getOpts().GraphiteURL + query

	// 记录请求的URL。
	s.nsqadmin.logf(LOG_INFO, "GRAPHITE: %s", url)

	// 定义一个结构体，用于解析graphite响应的数据。
	var response []struct {
		Target     string       `json:"target"`
		DataPoints [][]*float64 `json:"datapoints"`
	}
	// 发起GET请求到graphite，并解析响应。
	err = s.client.GETV1(url, &response)
	if err != nil {
		// 如果请求失败，记录错误并返回。
		s.nsqadmin.logf(LOG_ERROR, "graphite request failed - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	// 计算并格式化rate值。
	var rateStr string
	rate := *response[0].DataPoints[0][0]
	if rate < 0 {
		rateStr = "N/A"
	} else {
		rateDivisor := s.nsqadmin.getOpts().StatsdInterval / time.Second
		rateStr = fmt.Sprintf("%.2f", rate/float64(rateDivisor))
	}
	// 返回计算后的rate值。
	return struct {
		Rate string `json:"rate"`
	}{rateStr}, nil
}

// doConfig 处理配置相关的HTTP请求。
// 该函数支持通过HTTP PUT方法更新nsqadmin的配置。
// 参数:
//
//	w: http.ResponseWriter用于响应HTTP请求。
//	req: *http.Request包含HTTP请求的详细信息。
//	ps: httprouter.Params用于提取URL中的参数。
//
// 返回值:
//
//	interface{}: 成功时返回配置项的值。
//	error: 发生错误时返回错误信息。
func (s *httpServer) doConfig(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// 从URL参数中提取操作类型
	opt := ps.ByName("opt")

	// 获取允许配置变更的CIDR块
	allowConfigFromCIDR := s.nsqadmin.getOpts().AllowConfigFromCIDR
	if allowConfigFromCIDR != "" {
		// 解析CIDR块并检查请求来源是否在允许的CIDR块内
		_, ipnet, _ := net.ParseCIDR(allowConfigFromCIDR)
		addr, _, err := net.SplitHostPort(req.RemoteAddr)
		if err != nil {
			// 日志记录解析错误
			s.nsqadmin.logf(LOG_ERROR, "failed to parse RemoteAddr %s", req.RemoteAddr)
			// 返回错误响应
			return nil, http_api.Err{400, "INVALID_REMOTE_ADDR"}
		}
		ip := net.ParseIP(addr)
		if ip == nil {
			// 日志记录解析错误
			s.nsqadmin.logf(LOG_ERROR, "failed to parse RemoteAddr %s", req.RemoteAddr)
			// 返回错误响应
			return nil, http_api.Err{400, "INVALID_REMOTE_ADDR"}
		}
		if !ipnet.Contains(ip) {
			// 请求来源不在允许的CIDR块内，拒绝访问
			return nil, http_api.Err{403, "FORBIDDEN"}
		}
	}

	// 如果是PUT请求，处理配置更新
	if req.Method == "PUT" {
		// 限制请求体的最大读取大小为1MB+1
		readMax := int64(1024*1024 + 1)
		body, err := io.ReadAll(io.LimitReader(req.Body, readMax))
		if err != nil {
			// 内部错误，无法读取请求体
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
		// 检查请求体大小是否超过限制或为空
		if int64(len(body)) == readMax || len(body) == 0 {
			return nil, http_api.Err{413, "INVALID_VALUE"}
		}

		// 根据请求体更新配置
		opts := *s.nsqadmin.getOpts()
		switch opt {
		case "nsqlookupd_http_addresses":
			// 更新NSQ Lookupd HTTP地址列表
			err := json.Unmarshal(body, &opts.NSQLookupdHTTPAddresses)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
		case "log_level":
			// 更新日志级别
			logLevelStr := string(body)
			logLevel, err := lg.ParseLogLevel(logLevelStr)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
			opts.LogLevel = logLevel
		default:
			// 未知的配置更新请求
			return nil, http_api.Err{400, "INVALID_OPTION"}
		}
		// 应用新的配置
		s.nsqadmin.swapOpts(&opts)
	}

	// 返回当前配置项的值
	v, ok := getOptByCfgName(s.nsqadmin.getOpts(), opt)
	if !ok {
		return nil, http_api.Err{400, "INVALID_OPTION"}
	}

	return v, nil
}

// isAuthorizedAdminRequest 验证HTTP请求是否由授权的管理员用户发出。
// 该函数首先检查是否存在配置的管理员用户列表。如果列表为空，
// 则认为所有请求都是授权的。否则，它会根据预设的ACLHTTPHeader
// 从请求头中获取用户信息，并检查该用户是否在管理员用户列表中。
// 参数:
//
//	req (*http.Request): 待验证的HTTP请求指针。
//
// 返回值:
//
//	bool: 如果请求是由授权的管理员用户发出，则为true；否则为false。
func (s *httpServer) isAuthorizedAdminRequest(req *http.Request) bool {
	// 获取管理员用户列表
	adminUsers := s.nsqadmin.getOpts().AdminUsers
	// 如果没有配置管理员用户列表，或列表为空，则认为所有请求都是授权的
	if len(adminUsers) == 0 {
		return true
	}
	// 获取用于ACL验证的HTTP头键
	aclHTTPHeader := s.nsqadmin.getOpts().ACLHTTPHeader
	// 从请求头中获取用户信息
	user := req.Header.Get(aclHTTPHeader)
	// 遍历管理员用户列表，检查请求是否由授权用户发出
	for _, v := range adminUsers {
		if v == user {
			return true
		}
	}
	// 如果用户不在授权列表中，则请求未授权
	return false
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
