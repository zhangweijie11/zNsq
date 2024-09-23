package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"github.com/zhangweijie11/zNsq/nsqd"
	"strconv"
	"strings"
)

type tlsRequiredOption int

func (t *tlsRequiredOption) Set(s string) error {
	s = strings.ToLower(s)
	if s == "tcp-https" {
		*t = nsqd.TLSRequiredExceptHTTP
		return nil
	}
	required, err := strconv.ParseBool(s)
	if required {
		*t = nsqd.TLSRequired
	} else {
		*t = nsqd.TLSNotRequired
	}
	return err
}

func (t *tlsRequiredOption) Get() interface{} { return int(*t) }

func (t *tlsRequiredOption) String() string {
	return strconv.FormatInt(int64(*t), 10)
}

func (t *tlsRequiredOption) IsBoolFlag() bool { return true }

type tlsMinVersionOption uint16

var tlsVersionTable = []struct {
	val uint16
	str string
}{
	{tls.VersionTLS10, "tls1.0"},
	{tls.VersionTLS11, "tls1.1"},
	{tls.VersionTLS12, "tls1.2"},
	{tls.VersionTLS13, "tls1.3"},
}

func (t *tlsMinVersionOption) Set(s string) error {
	s = strings.ToLower(s)
	if s == "" {
		return nil
	}
	for _, v := range tlsVersionTable {
		if s == v.str {
			*t = tlsMinVersionOption(v.val)
			return nil
		}
	}
	return fmt.Errorf("未知 tlsVersionOption %q", s)
}

func (t *tlsMinVersionOption) Get() interface{} { return uint16(*t) }

func (t *tlsMinVersionOption) String() string {
	for _, v := range tlsVersionTable {
		if uint16(*t) == v.val {
			return v.str
		}
	}
	return strconv.FormatInt(int64(*t), 10)
}

func nsqdFlagSet(opts *nsqd.Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("nsqd", flag.ExitOnError)

	// basic options
	flagSet.Bool("version", false, "输出版本内心戏")
	flagSet.String("config", "", "配置文件路径")

	logLevel := opts.LogLevel
	flagSet.Var(&logLevel, "log-level", "设置日志级别: debug, info, warn, error, or fatal")
	flagSet.String("log-prefix", "[nsqd] ", "日志信息前缀")

	return flagSet
}

type config map[string]interface{}

// Validate 配置参数校验
func (cfg config) Validate() {
	if v, exists := cfg["tls_required"]; exists {
		var t tlsRequiredOption
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			cfg["tls_required"] = t.String()
		} else {
			logFatal("解析失败 tls_required %+v", v)
		}
	}

	if v, exists := cfg["tls_min_version"]; exists {
		var t tlsMinVersionOption
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			newVal := fmt.Sprintf("%v", t.Get())
			if newVal != "0" {
				cfg["tls_min_version"] = newVal
			} else {
				delete(cfg, "tls_min_version")
			}
		} else {
			logFatal("解析失败 tls_min_version %+v", v)
		}
	}

	if v, exists := cfg["log_level"]; exists {
		var t lg.LogLevel
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			cfg["log_level"] = t
		} else {
			logFatal("failed parsing log_level %+v", v)
		}
	}
}
