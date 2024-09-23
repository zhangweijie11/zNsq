//go:build !windows && !illumos
// +build !windows,!illumos

package dirlock

import (
	"fmt"
	"os"
	"syscall"
)

type DirLock struct {
	dir string
	f   *os.File
}

func New(dir string) *DirLock {
	return &DirLock{
		dir: dir,
	}
}

// Lock 在指定目录上获取一个排它锁，以确保该目录不会被多个实例同时访问
func (l *DirLock) Lock() error {
	f, err := os.Open(l.dir)
	if err != nil {
		return err
	}
	l.f = f
	// 尝试获取文件锁，LOCK_EX 为排它锁，LOCK_NB为非阻塞模式，如果无法立即获取锁则立即返回错误
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("无法使用目录 %s - %s (可能是另外一个 nsqd 实例在使用) ", l.dir, err)
	}
	return nil
}

// Unlock 解锁文件目录
func (l *DirLock) Unlock() error {
	defer l.f.Close()
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}
