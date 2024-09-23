package http_api

import (
	"errors"
	"io"
	"net/http"
	"net/url"
)

// ReqParams 定义了请求参数的结构。
// 该结构包括URL的查询参数以及请求体的内容。
type ReqParams struct {
	url.Values        // URL查询参数，使用url.Values类型来存储。
	Body       []byte // 请求体内容，以字节切片表示。
}

// NewReqParams 从给定的http请求中提取查询参数和请求体数据，并返回一个ReqParams对象。
// 该函数首先解析URL中的查询参数，然后读取请求体的全部内容。
// 如果在解析查询参数或读取请求体时发生错误，则返回nil和相应的错误。
// 参数:
//
//	req (*http.Request): 待提取参数的http请求指针。
//
// 返回值:
//
//	(*ReqParams): 包含查询参数和请求体数据的结构体指针，如果发生错误则为nil。
//	(error): 在解析查询参数或读取请求体时可能发生的错误，如果没有错误则为nil。
func NewReqParams(req *http.Request) (*ReqParams, error) {
	// 解析URL中的查询参数
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, err
	}

	// 读取请求体的全部内容
	data, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	// 返回包含查询参数和请求体数据的ReqParams对象
	return &ReqParams{reqParams, data}, nil
}

func (r *ReqParams) Get(key string) (string, error) {
	v, ok := r.Values[key]
	if !ok {
		return "", errors.New("key not in query params")
	}
	return v[0], nil
}

func (r *ReqParams) GetAll(key string) ([]string, error) {
	v, ok := r.Values[key]
	if !ok {
		return nil, errors.New("key not in query params")
	}
	return v, nil
}
