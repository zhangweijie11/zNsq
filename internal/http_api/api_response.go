package http_api

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"io"
	"net/http"
	"time"
)

// Decorator 是一个函数类型，用于装饰 APIHandler。
// 它接收一个 APIHandler 函数作为参数，并返回一个被装饰过的 APIHandler 函数。
// 被装饰的函数可以在原有函数执行前后添加额外的功能或处理逻辑。
type Decorator func(APIHandler) APIHandler

// APIHandler 是一个函数类型，代表处理 API 请求的处理器。
// 它接收 http.ResponseWriter 用于发送响应，*http.Request 用于读取请求信息，
// 和 httprouter.Params 用于匹配的路由参数。
// 它返回一个 interface{} 类型的数据和一个 error 类型的错误。
type APIHandler func(http.ResponseWriter, *http.Request, httprouter.Params) (interface{}, error)

// Err 是一个结构体类型，用于表示带有代码和文本信息的错误。
// Code 字段用于存储错误代码，Text 字段用于存储错误文本。
type Err struct {
	Code int    // 错误代码
	Text string // 错误文本
}

func (e Err) Error() string {
	return e.Text
}

// Log 是一个返回Decorator的函数，用于记录API处理的日志。
// 它通过接收一个日志记录函数logf和一个API处理函数f，返回一个新的APIHandler。
// 新的APIHandler会在执行原始处理函数前后添加日志记录操作，记录请求的方法、URL、客户端IP、响应状态码和请求处理时间。
// 参数:
//   - logf: 一个日志记录函数，用于输出日志信息。
//
// 返回值:
//   - Decorator: 一个装饰器函数，用于装饰APIHandler，增加日志记录功能。
func Log(logf lg.AppLogFunc) Decorator {
	// 返回一个Decorator，该Decorator进一步包装了APIHandler。
	return func(f APIHandler) APIHandler {
		// 返回一个包装后的APIHandler函数。
		return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
			// 记录请求开始时间。
			start := time.Now()
			// 调用原始的API处理函数。
			response, err := f(w, req, ps)
			// 计算请求处理时间。
			elapsed := time.Since(start)
			// 默认状态码为200，表示成功。
			status := 200
			// 如果错误不是nil，并且错误类型为Err，则使用错误关联的状态码。
			if e, ok := err.(Err); ok {
				status = e.Code
			}
			// 格式化并记录日志，包括状态码、请求方法、URL、客户端IP和请求处理时间。
			logf(lg.INFO, "%d %s %s (%s) %s",
				status, req.Method, req.URL.RequestURI(), req.RemoteAddr, elapsed)
			// 返回原始API处理函数的响应和错误。
			return response, err
		}
	}
}

// Decorate 用于将一个或多个装饰器应用到API处理程序上。
// 它接受一个API处理程序f和任意数量的装饰器ds作为参数，并返回一个httprouter的处理函数。
// 该函数通过依次应用所有装饰器来增强或修改传入的API处理程序f的功能。
//
// 参数:
//
//	f: API处理程序，表示要装饰的核心功能或已经被其他装饰器装饰过的处理程序。
//	ds: 一个或多个装饰器的变长参数列表，每个装饰器都是一个函数，它接受一个API处理程序并返回一个修改后的API处理程序。
//
// 返回值:
//
//	返回一个httprouter的处理函数，它封装了经过所有装饰器增强后的API处理程序f。
//	此处理函数负责最终执行所有装饰逻辑和原始的API处理逻辑。
func Decorate(f APIHandler, ds ...Decorator) httprouter.Handle {
	// 从原始的API处理程序f开始，逐步应用每个装饰器。
	decorated := f
	for _, decorate := range ds {
		// 将当前的装饰状态传递给下一个装饰器，并接收装饰后的处理程序。
		decorated = decorate(decorated)
	}
	// 返回一个处理函数，该函数实际上是在应用所有装饰器后的API处理程序。
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		decorated(w, req, ps)
	}
}

// LogPanicHandler 返回一个函数，用于处理HTTP请求中的panic情况。
// 该函数接收一个日志记录函数logf，用于记录错误日志。
// 参数:
//
//	logf: 一个AppLogFunc类型的功能，用于记录应用日志。
//
// 返回值:
//
//	一个接收http.ResponseWriter, *http.Request和interface{}参数的函数，
//	用于处理HTTP请求，并在请求处理过程中发生panic时进行日志记录和错误响应。
func LogPanicHandler(logf lg.AppLogFunc) func(w http.ResponseWriter, req *http.Request, p interface{}) {
	return func(w http.ResponseWriter, req *http.Request, p interface{}) {
		// 当发生panic时，使用logf记录错误信息和panic的具体原因。
		logf(lg.ERROR, "panic in HTTP handler - %s", p)
		// 使用Decorate函数包装返回的函数，装饰包括日志记录和版本控制。
		// 装饰后的函数不使用httprouter.Params实际参数，因此传入nil。
		Decorate(func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
			// 返回nil和一个错误对象，表示内部错误。
			// 错误对象包含HTTP状态码500和错误消息"INTERNAL_ERROR"。
			return nil, Err{500, "INTERNAL_ERROR"}
		}, Log(logf), V1)(w, req, nil)
	}
}

// V1 V1装饰器函数，用于处理API请求并响应符合V1规范的数据
// 该函数接收一个APIHandler类型的函数f作为参数，返回一个新的APIHandler
// 参数:
//
//	f: 一个API处理函数，负责处理具体的API逻辑
//
// 返回值:
//
//	一个APIHandler，用于接收HTTP请求，调用传入的API处理函数f处理请求，然后根据处理结果生成符合V1规范的响应
func V1(f APIHandler) APIHandler {
	// 返回一个新的APIHandler，该处理函数封装了传入的f，用于处理请求并生成响应
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
		// 调用传入的API处理函数f处理请求，获取处理结果或错误
		data, err := f(w, req, ps)
		// 如果在处理请求时发生错误，则根据错误类型生成符合V1规范的错误响应
		if err != nil {
			RespondV1(w, err.(Err).Code, err)
			return nil, nil
		}
		// 如果请求处理成功，则生成符合V1规范的成功响应，响应状态码为200，响应体为处理结果data
		RespondV1(w, 200, data)
		return nil, nil
	}
}

// RespondV1 处理HTTP响应，根据状态码和数据生成适当的响应。
// 该函数支持不同的响应格式，优先处理200状态码的特殊情况，
// 对于非200状态码，默认生成JSON格式的错误信息。
// 参数:
//
//	w: http.ResponseWriter，用于向客户端发送响应。
//	code: HTTP状态码。
//	data: 响应数据，可以是字符串、字节切片或其他需要序列化为JSON的类型。
func RespondV1(w http.ResponseWriter, code int, data interface{}) {
	// 初始化响应数据、错误和标记是否为JSON响应的变量。
	var response []byte
	var err error
	var isJSON bool

	// 处理200状态码的特殊情况。
	if code == 200 {
		switch data := data.(type) {
		case string:
			// 数据为字符串类型，直接转换为字节切片。
			response = []byte(data)
		case []byte:
			// 数据已经是以字节切片形式，直接使用。
			response = data
		case nil:
			// 数据为空，使用空的字节切片。
			response = []byte{}
		default:
			// 默认情况，尝试将数据序列化为JSON。
			isJSON = true
			response, err = json.Marshal(data)
			if err != nil {
				// 序列化失败，更新状态码为500，并将错误信息作为响应数据。
				code = 500
				data = err
			}
		}
	}

	// 处理非200状态码的情况，生成JSON格式的错误信息。
	if code != 200 {
		isJSON = true
		response, _ = json.Marshal(struct {
			Message string `json:"message"`
		}{fmt.Sprintf("%s", data)})
	}

	// 根据是否为JSON响应，设置合适的Content-Type。
	if isJSON {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}
	// 设置自定义的Header，标识响应的类型和版本。
	w.Header().Set("X-NSQ-Content-Type", "nsq; version=1.0")
	// 设置响应的状态码。
	w.WriteHeader(code)
	// 发送响应数据。
	w.Write(response)
}

// LogNotFoundHandler 返回一个HTTP处理程序，用于处理404未找到的请求。
// 它使用提供的日志函数记录请求，并始终返回404错误。
// 参数:
//   - logf: 一个AppLogFunc类型的日志记录函数，用于记录HTTP请求的日志。
//
// 返回值:
//   - http.Handler: 一个HTTP处理程序，用于处理404未找到的请求。
func LogNotFoundHandler(logf lg.AppLogFunc) http.Handler {
	// 返回一个HTTP处理程序，该处理程序使用Decorate函数包装了一个固定返回404错误的处理函数。
	// 这个处理程序不接受任何实际参数，也不返回任何有用的响应体，只是确保所有请求都返回404错误。
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// 调用Decorate函数包装的处理函数，其中包含日志记录和版本控制装饰器。
		// 由于处理函数总是返回nil和一个404错误，因此这个调用最终会导致HTTP 404响应被发送回客户端。
		Decorate(func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
			return nil, Err{404, "NOT_FOUND"}
		}, Log(logf), V1)(w, req, nil)
	})
}

// LogMethodNotAllowedHandler 创建一个HTTP处理程序，用于处理方法不允许可的请求，并记录日志。
// 该函数接收一个日志记录的函数引用作为参数，用于记录请求信息和错误信息。
// 参数:
//
//	logf: 一个lg.AppLogFunc类型的函数，用于向应用程序日志记录系统记录信息。
//
// 返回值:
//
//	返回一个http.Handler，它是一个函数，可以处理HTTP请求。
//
// 该处理程序主要用于处理不支持的HTTP方法，返回405错误，并记录请求信息。
func LogMethodNotAllowedHandler(logf lg.AppLogFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// 使用Decorate函数包装处理程序，该函数添加了日志记录和版本控制功能。
		// 这里的处理程序匿名函数返回nil和一个Err实例，表示405错误，方法不允许可。
		// 调用Decorate函数的Log和V1参数确保了请求会被正确地记录，并应用版本控制。
		// 最后，调用该装饰后的处理程序，传入请求、响应和一个nil的httprouter.Params参数。
		// nil参数表示该处理程序不使用httprouter的参数功能。
		Decorate(func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
			return nil, Err{405, "METHOD_NOT_ALLOWED"}
		}, Log(logf), V1)(w, req, nil)
	})
}

// PlainText 是一个高阶函数，它接受一个 APIHandler 函数作为参数，并返回一个新的 APIHandler 函数。
// 返回的函数处理 HTTP 请求后，将响应数据以纯文本形式写入 http.ResponseWriter。
// 如果响应数据是错误，将根据错误类型设置适当的 HTTP 状态码和响应体。
// 参数:
//
//	f - 一个 APIHandler 函数，用于处理 HTTP 请求。
//
// 返回值:
//
//	APIHandler - 一个新的 APIHandler 函数，它调用传入的 f 函数处理请求，并将响应数据以纯文本形式写入 http.ResponseWriter。
func PlainText(f APIHandler) APIHandler {
	// 返回的 APIHandler 函数处理 HTTP 请求。
	// 参数:
	//   w - http.ResponseWriter，用于向客户端写入响应。
	//   req - *http.Request，表示客户端的请求。
	//   ps - httprouter.Params，包含路由参数。
	// 返回值:
	//   interface{} - 响应数据，具体类型取决于 f 函数的实现。
	//   error - 如果发生错误，返回错误信息。
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
		// 默认 HTTP 状态码。
		code := 200
		// 调用传入的 APIHandler 函数处理请求。
		data, err := f(w, req, ps)
		if err != nil {
			// 如果发生错误，根据错误类型获取相应的 HTTP 状态码和错误信息。
			code = err.(Err).Code
			data = err.Error()
		}
		// 根据响应数据的类型，以纯文本形式写入 http.ResponseWriter。
		switch d := data.(type) {
		case string:
			// 设置 HTTP 状态码并写入字符串响应。
			w.WriteHeader(code)
			io.WriteString(w, d)
		case []byte:
			// 设置 HTTP 状态码并写入字节切片响应。
			w.WriteHeader(code)
			w.Write(d)
		default:
			// 如果响应数据类型未知，抛出 panic。
			panic(fmt.Sprintf("unknown response type %T", data))
		}
		// 处理完成，返回 nil 表示没有错误。
		return nil, nil
	}
}
