package nsqd

import (
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

const (
	// nodeIDBits表示用于存储节点ID的位数
	nodeIDBits = uint64(10)
	// sequenceBits表示用于存储序列号的位数
	sequenceBits = uint64(12)
	// nodeIDShift表示节点ID在ID中的起始位置
	nodeIDShift = sequenceBits
	// timestampShift表示时间戳在ID中的起始位置
	timestampShift = sequenceBits + nodeIDBits
	// sequenceMask用于计算序列号掩码，用于生成下一个序列号
	sequenceMask = int64(-1) ^ (int64(-1) << sequenceBits)

	// twepoch表示Twitter风格的epoch时间，是一个固定的起始时间戳
	twepoch = int64(1288834974288)
)

var ErrTimeBackwards = errors.New("time has gone backwards")
var ErrSequenceExpired = errors.New("sequence expired")
var ErrIDBackwards = errors.New("ID went backward")

type guid int64

type guidFactory struct {
	sync.Mutex
	nodeID        int64
	sequence      int64
	lastTimestamp int64
	lastID        guid
}

// NewGUIDFactory 创建并返回一个新的GUID工厂实例。
// 该函数接收一个nodeID作为参数，nodeID用于在生成GUID时作为节点标识。
// 返回值是一个*guidFactory类型的指针，指向新创建的GUID工厂实例。
func NewGUIDFactory(nodeID int64) *guidFactory {
	return &guidFactory{
		nodeID: nodeID,
	}
}

// NewGUID 生成一个新的全局唯一标识符（GUID）。
// 该方法首先获取当前时间，并将其转换为伪毫秒，然后与节点ID和序列号组合以生成GUID。
// 如果遇到时间回退或序列号过期的情况，将返回相应的错误。
// 返回值guid是生成的全局唯一标识符，如果生成失败则返回错误。
func (f *guidFactory) NewGUID() (guid, error) {
	// 加锁以确保线程安全
	f.Lock()

	// 获取当前时间，并转换为伪毫秒
	ts := time.Now().UnixNano() >> 20

	// 检查时间是否回退，如果是则解锁并返回错误
	if ts < f.lastTimestamp {
		f.Unlock()
		return 0, ErrTimeBackwards
	}

	// 如果当前时间与上一次生成时间相同，则增加序列号
	if f.lastTimestamp == ts {
		f.sequence = (f.sequence + 1) & sequenceMask
		// 如果序列号达到最大值，则解锁并返回错误
		if f.sequence == 0 {
			f.Unlock()
			return 0, ErrSequenceExpired
		}
	} else {
		// 如果是新的时间戳，则重置序列号
		f.sequence = 0
	}

	// 更新上一次生成时间
	f.lastTimestamp = ts

	// 根据时间戳、节点ID和序列号生成GUID
	id := guid(((ts - twepoch) << timestampShift) |
		(f.nodeID << nodeIDShift) |
		f.sequence)

	// 检查生成的GUID是否小于或等于上一个GUID，如果是则解锁并返回错误
	if id <= f.lastID {
		f.Unlock()
		return 0, ErrIDBackwards
	}

	// 更新上一个生成的GUID
	f.lastID = id

	// 解锁以释放锁
	f.Unlock()

	// 返回生成的GUID和nil错误，表示成功
	return id, nil
}

func (g guid) Hex() MessageID {
	var h MessageID
	var b [8]byte

	b[0] = byte(g >> 56)
	b[1] = byte(g >> 48)
	b[2] = byte(g >> 40)
	b[3] = byte(g >> 32)
	b[4] = byte(g >> 24)
	b[5] = byte(g >> 16)
	b[6] = byte(g >> 8)
	b[7] = byte(g)

	hex.Encode(h[:], b[:])
	return h
}
