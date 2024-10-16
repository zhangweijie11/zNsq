package main

import (
	"fmt"

	"github.com/zhangweijie11/zNsq/internal/lg"
)

type config map[string]interface{}

// Validate 函数用于验证配置项中的 log_level 是否有效。
// 它通过尝试将配置项中的 log_level 字段解析为 lg.LogLevel 类型来实现。
// 如果解析成功，它会更新配置项中的 log_level 为解析后的值。
// 如果解析失败，它会记录错误并终止程序。
func (cfg config) Validate() {
	// 检查配置项中是否存在 log_level。
	if v, exists := cfg["log_level"]; exists {
		// 尝试将配置项中的 log_level 转换为 lg.LogLevel 类型。
		var t lg.LogLevel
		err := t.Set(fmt.Sprintf("%v", v))
		// 如果转换成功，更新配置项中的 log_level 为转换后的值。
		if err == nil {
			cfg["log_level"] = t
		} else {
			// 如果转换失败，记录错误信息并终止程序。
			logFatal("failed parsing log_level %+v", v)
		}
	}
}
