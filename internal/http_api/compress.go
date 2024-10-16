package http_api

import (
	"compress/flate"
	"compress/gzip"
	"io"
	"net/http"
	"strings"
)

// compressResponseWriter 是一个自定义的结构体，用于支持响应写入器的压缩功能。
// 它嵌入了 io.Writer, http.ResponseWriter 和 http.Hijacker 接口，以便实现 HTTP 响应的写入和可能的劫持。
type compressResponseWriter struct {
	io.Writer
	http.ResponseWriter
	http.Hijacker
}

// Header 返回响应中当前设置的 HTTP 头。
// 它允许访问或修改响应的头信息。
func (w *compressResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

// WriteHeader 写入响应头，但在写入之前删除了 "Content-Length" 头。
// 这对于某些类型的响应（如分块传输）很有用，因为它允许写入器稍后写入更多数据。
// 参数 c 是 HTTP 响应状态码。
func (w *compressResponseWriter) WriteHeader(c int) {
	w.ResponseWriter.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(c)
}

// Write 将数据 b 写入到响应中，在写入之前处理 "Content-Type" 和 "Content-Length" 头。
// 如果 "Content-Type" 头未设置，则根据数据 b 的内容尝试自动检测并设置。
// "Content-Length" 头总是在写入之前被删除，因为压缩可能会改变内容的长度。
// 参数 b 是要写入到 HTTP 响应中的数据。
// 返回值是写入的字节数和可能的错误。
func (w *compressResponseWriter) Write(b []byte) (int, error) {
	h := w.ResponseWriter.Header()
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", http.DetectContentType(b))
	}
	h.Del("Content-Length")
	return w.Writer.Write(b)
}

// CompressHandler 是一个中间件，它包装了一个 http.Handler 并为其添加压缩响应的功能。
// 它会根据客户端请求头中接受的编码类型，选择使用 gzip 或 deflate 来压缩响应内容。
// 参数 h 是被包装的 http.Handler，它将使用新的压缩响应功能。
func CompressHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 遍历客户端请求头中接受的编码类型
	L:
		for _, enc := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
			// 去除编码类型前后的空白字符
			switch strings.TrimSpace(enc) {
			case "gzip":
				// 如果客户端接受 gzip 编码，则设置响应头为 gzip 编码
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Add("Vary", "Accept-Encoding")

				// 创建一个新的 gzip.Writer 用于压缩响应
				gw := gzip.NewWriter(w)
				// 确保在处理完请求后关闭 gzip.Writer
				defer gw.Close()

				// 尝试将当前响应对象转换为 Hijacker 接口
				h, hok := w.(http.Hijacker)
				if !hok { /* 如果响应对象不支持 Hijacker 接口，放弃转换 */
					h = nil
				}

				// 将响应对象替换为实现了压缩功能的自定义响应对象
				w = &compressResponseWriter{
					Writer:         gw,
					ResponseWriter: w,
					Hijacker:       h,
				}

				// 跳出循环，不再尝试其他编码类型
				break L
			case "deflate":
				// 如果客户端接受 deflate 编码，则设置响应头为 deflate 编码
				w.Header().Set("Content-Encoding", "deflate")
				w.Header().Add("Vary", "Accept-Encoding")

				// 创建一个新的 flate.Writer 用于压缩响应
				fw, _ := flate.NewWriter(w, flate.DefaultCompression)
				// 确保在处理完请求后关闭 flate.Writer
				defer fw.Close()

				// 尝试将当前响应对象转换为 Hijacker 接口
				h, hok := w.(http.Hijacker)
				if !hok { /* 如果响应对象不支持 Hijacker 接口，放弃转换 */
					h = nil
				}

				// 将响应对象替换为实现了压缩功能的自定义响应对象
				w = &compressResponseWriter{
					Writer:         fw,
					ResponseWriter: w,
					Hijacker:       h,
				}

				// 跳出循环，不再尝试其他编码类型
				break L
			}
		}

		// 使用包装过的响应对象处理请求
		h.ServeHTTP(w, r)
	})
}
