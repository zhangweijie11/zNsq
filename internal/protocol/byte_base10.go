package protocol

import "errors"

var errBase10 = errors.New("failed to convert to Base10")

// ByteToBase10 将字节切片转换为十进制无符号整数。
// 参数 b 是待转换的字节切片。
// 返回值 n 是转换后的十进制无符号整数。
// 返回值 err 是转换过程中可能发生的错误，如果没有错误则为 nil。
func ByteToBase10(b []byte) (n uint64, err error) {
	// 定义十进制基数
	base := uint64(10)

	// 初始化n为0
	n = 0

	// 遍历字节切片中的每个字节
	for i := 0; i < len(b); i++ {
		// 定义变量v用于存储数字字节的值
		var v byte

		// 获取当前字节
		d := b[i]

		// 根据当前字节的值进行转换
		switch {
		case '0' <= d && d <= '9':
			// 如果当前字节是数字字符，则将其转换为对应的数值
			v = d - '0'
		default:
			// 如果当前字节不是数字字符，则重置n为0，设置错误信息，并返回
			n = 0
			err = errBase10
			return
		}

		// 将当前结果乘以基数，并加上新计算的数值
		n *= base
		n += uint64(v)
	}

	// 返回转换后的结果和可能的错误
	return n, err
}
