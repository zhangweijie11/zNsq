package stringy

import "fmt"

// NanoSecondToHuman 将纳秒转换为人类可读的时间格式。
// 参数 v 代表时间，以纳秒为单位。函数根据 v 的值返回最合适的时间单位表示。
// 返回值是一个字符串，表示转换后的时间，精确到小数点后一位。
func NanoSecondToHuman(v float64) string {
	// 根据 v 的值，suffix 用于存储时间单位的后缀。
	var suffix string
	switch {
	// 如果 v 大于1000000000纳秒，转换为秒。
	case v > 1000000000:
		v /= 1000000000
		suffix = "s"
	// 如果 v 大于1000000纳秒，转换为毫秒。
	case v > 1000000:
		v /= 1000000
		suffix = "ms"
	// 如果 v 大于1000纳秒，转换为微秒。
	case v > 1000:
		v /= 1000
		suffix = "us"
	// 默认情况下，v 以纳秒为单位。
	default:
		suffix = "ns"
	}
	// 返回格式化后的时间字符串。
	return fmt.Sprintf("%0.1f%s", v, suffix)
}
