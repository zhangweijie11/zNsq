package protocol

type ChildErr interface {
	Parent() error
}

// FatalClientErr 定义了一个致命的客户端错误，包含错误代码、描述和原始错误。
// 这个结构体用于表示客户端在操作过程中遇到的不可恢复的错误。
type FatalClientErr struct {
	ParentErr error  // ParentErr 存储原始错误信息
	Code      string // Code 用于存储错误代码
	Desc      string // Desc 用于存储错误描述
}

// ClientErr 定义了一个客户端错误的结构体。
// 该结构体用于封装API调用或其他操作失败时的错误信息。
type ClientErr struct {
	ParentErr error  // ParentErr 表示引发错误的原始错误对象。
	Code      string // Code 表示错误的代码，用于快速识别错误类型。
	Desc      string // Desc 提供错误的详细描述，帮助定位和解决问题。
}

// Error 实现了error接口，返回错误的代码和描述。
// 返回值是错误的代码和描述拼接成的字符串。
func (e *FatalClientErr) Error() string {
	return e.Code + " " + e.Desc
}

// Parent 返回这个FatalClientErr的原始错误。
// 返回值是原始错误，以便进一步错误处理。
func (e *FatalClientErr) Parent() error {
	return e.ParentErr
}

// NewFatalClientErr 创建并返回一个新的FatalClientErr实例。
// 参数:
//
//	parent: 原始错误
//	code: 错误代码
//	description: 错误描述
//
// 返回值:
//
//	一个新的FatalClientErr实例，包含给定的原始错误、错误代码和描述。
func NewFatalClientErr(parent error, code string, description string) *FatalClientErr {
	return &FatalClientErr{parent, code, description}
}

// NewClientErr 创建并返回一个新的ClientErr实例。
// 这个函数主要用于在需要报告客户端错误时，封装一个基本错误信息的情境下。
// 参数:
// - parent: 原始的错误，通常是由其他库或系统产生的。
// - code: 错误代码，用于快速定位和分类错误。
// - description: 错误描述，提供关于错误的详细信息。
// 返回值:
// - *ClientErr: 一个指向新创建的ClientErr实例的指针，包含了封装后的错误信息。
func NewClientErr(parent error, code string, description string) *ClientErr {
	return &ClientErr{parent, code, description}
}

// ClientErr.Error 返回错误的代码和描述
// 该方法实现了error接口，允许ClientErr能够像标准库中的error一样被使用。
func (e *ClientErr) Error() string {
	return e.Code + " " + e.Desc
}

// Parent ClientErr.Parent 返回该错误的父错误
// 用于错误链的处理，可以通过这个方法获取到链中的下一个错误。
func (e *ClientErr) Parent() error {
	return e.ParentErr
}
