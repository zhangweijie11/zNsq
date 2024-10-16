module github.com/zhangweijie11/zNsq

go 1.21.0

require (
	github.com/BurntSushi/toml v1.4.0
	github.com/blang/semver v3.5.1+incompatible
	github.com/bmizerany/perks v0.0.0-20230307044200-03f9df79da1e
	github.com/golang/snappy v0.0.4
	github.com/judwhite/go-svc v1.2.2
	github.com/julienschmidt/httprouter v1.3.0
	github.com/mreiferson/go-options v1.0.0
	github.com/nsqio/go-diskqueue v1.1.0
	github.com/nsqio/go-nsq v1.1.0
)

require golang.org/x/sys v0.25.0 // indirect

replace github.com/judwhite/go-svc => github.com/mreiferson/go-svc v1.2.2-0.20210815184239-7a96e00010f6
