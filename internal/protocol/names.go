package protocol

import "regexp"

// 字符串由一个或多个字母（大小写）、数字、点（.）、下划线（_）、或连字符（-）组成。
// 字符串可以以可选的 #ephemeral 结尾
var validTopicChannelNameRegex = regexp.MustCompile(`^[.a-zA-Z0-9_-]+(#ephemeral)?$`)

// IsValidTopicName 检查给定的主题名称是否有效。
// 名称必须在1到64个字符之间，并且只能包含特定的字符集。
// 参数:
//
//	name - 待检查的主题名称。
//
// 返回值:
//
//	bool - 如果名称有效则返回true，否则返回false。
func IsValidTopicName(name string) bool {
	return isValidName(name)
}

// IsValidChannelName 检查给定的频道名称是否有效。
// 名称必须在1到64个字符之间，并且只能包含特定的字符集。
// 参数:
//
//	name - 待检查的频道名称。
//
// 返回值:
//
//	bool - 如果名称有效则返回true，否则返回false。
func IsValidChannelName(name string) bool {
	return isValidName(name)
}

// isValidName 验证给定的名称是否符合主题或频道名称的规则。
// 这个函数不直接暴露给外部使用，但它包含了实际的逻辑来验证名称是否有效。
// 参数:
//
//	name - 待验证的名称。
//
// 返回值:
//
//	bool - 如果名称有效则返回true，否则返回false。
func isValidName(name string) bool {
	// 检查名称长度是否在1到64个字符之间。
	if len(name) > 64 || len(name) < 1 {
		return false
	}

	// 使用正则表达式进一步验证名称是否只包含ASCII字母、数字、连字符、下划线或点。
	return validTopicChannelNameRegex.MatchString(name)
}
