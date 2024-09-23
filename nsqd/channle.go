package nsqd

import (
	"container/heap"
	"errors"
	"fmt"
	"github.com/nsqio/go-diskqueue"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"github.com/zhangweijie11/zNsq/internal/pqueue"
	"github.com/zhangweijie11/zNsq/internal/quantile"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats(string) ClientStats
	Empty()
}

type Channel struct {
	sync.RWMutex
	nsqd                       *NSQD
	topicName                  string                     // 主题名称
	name                       string                     // 通道名称
	ephemeral                  bool                       // 是否为临时通道
	paused                     int32                      // 通道是否停用
	exitFlag                   int32                      // 退出标志
	exitMutex                  sync.RWMutex               // 退出锁
	clients                    map[int64]Consumer         // 客户端连接
	backend                    BackendQueue               // 后端队列
	deferredMessages           map[MessageID]*pqueue.Item // 延迟消息
	deferredPQ                 pqueue.PriorityQueue       // 延迟消息队列
	deferredMutex              sync.Mutex                 // 延迟消息队列的锁
	inFlightMessages           map[MessageID]*Message     // 正在处理的消息
	inFlightPQ                 inFlightPqueue             // 正在处理的消息队列
	inFlightMutex              sync.Mutex                 // 正在处理的消息队列的锁
	memoryMsgChan              chan *Message              // 内存消息队列
	requeueCount               uint64                     // 重新排队的消息数量
	messageCount               uint64                     // 消息数量
	timeoutCount               uint64                     // 超时消息数量
	deleteCallback             func(*Channel)             // 删除回调函数，当频道被删除时会调用
	e2eProcessingLatencyStream *quantile.Quantile         // 端到端处理延迟的百分位数流
	deleter                    sync.Once                  // 确保删除回调函数只被调用一次
}

// NewChannel 创建并初始化一个新的NSQ频道。
// 该函数接收主题名称、频道名称、指向NSQD实例的指针和一个删除回调函数作为参数。
// 删除回调函数用于在频道被删除时调用，以执行任何必要的清理操作。
// 返回一个指向新创建的Channel对象的指针。
func NewChannel(topicName string, channelName string, nsqd *NSQD, deleteCallback func(*Channel)) *Channel {

	// 初始化Channel对象并设置基础属性
	c := &Channel{
		topicName:      topicName,                                    // 主题名称
		name:           channelName,                                  // 频道名称
		memoryMsgChan:  nil,                                          // 内存中的消息通道，尚未初始化
		clients:        make(map[int64]Consumer),                     // 客户端映射，用于管理订阅此频道的客户端
		deleteCallback: deleteCallback,                               // 删除回调函数，当频道被删除时会调用
		nsqd:           nsqd,                                         // 指向创建此频道的NSQD实例的指针
		ephemeral:      strings.HasSuffix(channelName, "#ephemeral"), // 标记此频道是否为临时频道
	}

	// 根据配置决定是否启用内存消息队列
	// 如果配置的内存队列大小大于0或者频道是临时的，则初始化内存消息队列
	if nsqd.getOpts().MemQueueSize > 0 || c.ephemeral {
		c.memoryMsgChan = make(chan *Message, nsqd.getOpts().MemQueueSize)
	}

	// 初始化端到端处理延迟的百分位数流
	// 如果配置了端到端处理延迟的百分位数，则创建相应的quantile对象
	if len(nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			nsqd.getOpts().E2EProcessingLatencyWindowTime,
			nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}

	// 初始化优先级队列
	c.initPQ()

	// 根据频道是否为临时类型，初始化不同的后端队列
	if c.ephemeral {
		// 对于临时频道，使用虚拟后端队列
		c.backend = newDummyBackendQueue()
	} else {
		// 对于持久化频道，创建diskqueue实例作为后端队列
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		backendName := getBackendName(topicName, channelName) // 生成后端队列的唯一名称
		c.backend = diskqueue.New(
			backendName,
			nsqd.getOpts().DataPath,
			nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			nsqd.getOpts().SyncEvery,
			nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	c.nsqd.Notify(c, !c.ephemeral)

	return c
}

// IsPaused 通道是否停用
func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// Delete 清空并关闭通道
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close Channel 的 Close 方法用于关闭通道。
// 它通过调用 c.exit 方法，并传入 false 参数来实现关闭操作。
// 如果在关闭过程中出现错误，会返回该错误。
func (c *Channel) Close() error {
	// 调用 c.exit 方法，传入 false 表示正常关闭。
	return c.exit(false)
}

// 退出通道
// 当通道被删除或正常关闭时调用此方法
// deleted 参数指示通道是否是因为被显式删除而退出
func (c *Channel) exit(deleted bool) error {
	// 加锁以确保退出过程的原子性
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	// 检查通道是否已经被标记为退出
	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	// 根据 deleted 值决定日志消息和是否通知 lookupd
	if deleted {
		c.nsqd.logf(LOG_INFO, "CHANNEL(%s): deleting", c.name)

		// 显式删除通道时，从 lookupd 取消注册
		c.nsqd.Notify(c, !c.ephemeral)
	} else {
		c.nsqd.logf(LOG_INFO, "CHANNEL(%s): closing", c.name)
	}

	// 强制关闭客户端连接
	c.RLock()
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	// 根据 deleted 值，执行不同的后续操作
	if deleted {
		// 清空队列，也会删除后端文件
		c.Empty()
		return c.backend.Delete()
	}

	// 将剩余数据写入磁盘
	c.flush()
	return c.backend.Close()
}

// Empty Channel的Empty方法清空通道。
// 它通过从所有已注册的客户端和后端设备中移除所有消息来实现这一点。
// 返回值表示清空操作是否成功。
func (c *Channel) Empty() error {
	// 加锁以确保操作的原子性。
	c.Lock()
	defer c.Unlock()

	// 确保优先队列已初始化。
	c.initPQ()

	// 从每个已注册客户端中清空消息。
	for _, client := range c.clients {
		client.Empty()
	}

	// 等待直到memoryMsgChan为空，表明所有消息都已处理。
	for {
		select {
		case <-c.memoryMsgChan:
			// 接收到消息，继续处理。
		default:
			// 没有消息待处理，转到结束逻辑。
			goto finish
		}
	}

finish:
	// 从后端设备中清空消息。
	// 这是确保通道完全清空的最后一步。
	return c.backend.Empty()
}

// initPQ 初始化消息队列中的优先队列结构。
//
// 该函数根据内存队列大小动态计算队列容量，并初始化两个优先队列：
// 1. inFlightPQ：用于管理正在处理的消息，其大小为内存队列大小的十分之一。
// 2. deferredPQ：用于管理延迟发送的消息，其大小同样为内存队列大小的十分之一。
//
// 锁定互斥锁以确保线程安全地初始化队列结构和相关消息映射。
func (c *Channel) initPQ() {
	// 计算优先队列的大小，保证至少为1
	pqSize := int(math.Max(1, float64(c.nsqd.getOpts().MemQueueSize)/10))

	// 锁定以确保线程安全地初始化inFlight队列和消息映射
	c.inFlightMutex.Lock()
	c.inFlightMessages = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	// 锁定以确保线程安全地初始化deferred队列和消息映射
	c.deferredMutex.Lock()
	c.deferredMessages = make(map[MessageID]*pqueue.Item)
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// flush 刷新通道中的消息，将内存中、处理中和延迟的消息写入后端。
// 这个方法需要持有c.nsqd.backendMutex的锁，以确保在调用时后端是可访问的。
func (c *Channel) flush() error {
	// 检查消息通道中是否有未处理的消息
	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.nsqd.logf(LOG_INFO, "CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}

	// 处理内存中的消息
	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(msg, c.backend)
			if err != nil {
				c.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	// 处理中的消息
	c.inFlightMutex.Lock()
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(msg, c.backend)
		if err != nil {
			c.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.inFlightMutex.Unlock()

	// 处理延迟的消息
	c.deferredMutex.Lock()
	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(msg, c.backend)
		if err != nil {
			c.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.deferredMutex.Unlock()

	return nil
}

// PutMessageDeferred 延时将消息放入通道
//
// 该方法使用原子操作增加消息计数，并启动一个延时计时器，用于在指定时间后处理消息。
// 这对于需要在特定时间后处理消息的场景非常有用。
//
// 参数:
//
//	msg (*Message): 指向要放入通道的消息的指针。
//	timeout (time.Duration): 延时处理消息的时间间隔。
func (c *Channel) PutMessageDeferred(msg *Message, timeout time.Duration) {
	atomic.AddUint64(&c.messageCount, 1)
	c.StartDeferredTimeout(msg, timeout)
}

// StartDeferredTimeout 将消息添加到延迟处理队列中，以便在指定时间后进行处理。
// 此方法主要用于需要在特定时间点执行的消息或任务。
// 参数:
//
//	msg (*Message): 要延迟处理的消息。
//	timeout (time.Duration): 消息延迟处理的时间间隔。
//
// 返回值:
//
//	error: 如果消息添加失败则返回错误，否则返回nil。
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	// 计算消息达到延迟处理时间的绝对时间戳。
	absTs := time.Now().Add(timeout).UnixNano()
	// 创建一个优先队列项，包含要延迟处理的消息及其处理时间。
	item := &pqueue.Item{Value: msg, Priority: absTs}
	// 将消息推入延迟消息列表。
	err := c.pushDeferredMessage(item)
	if err != nil {
		// 如果推送消息失败，则返回错误。
		return err
	}
	// 将消息添加到延迟处理的优先队列中。
	c.addToDeferredPQ(item)
	// 消息添加成功，返回nil。
	return nil
}

// pushDeferredMessage 将一条消息延迟发送到通道。
//
// 该方法通过维护一个延迟消息的映射表，以确保消息在指定时间前不会被处理。
// 它首先尝试获取锁，以确保对延迟消息映射表的线程安全操作。
// 然后，它检查消息ID是否已经在映射表中存在，如果存在，则释放锁并返回错误。
// 如果不存在，则将消息添加到映射表中，然后释放锁。
//
// 参数:
//
//	item: *pqueue.Item
//	  包含消息ID的消息项。该消息项从优先队列中获取，值字段包含要延迟发送的实际消息。
//
// 返回值:
//
//	error
//	  如果尝试对同一消息ID进行重复延迟发送，则返回错误。
func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	// 获取锁以保护延迟消息映射表。
	c.deferredMutex.Lock()

	// TODO: 这些映射查找操作是高成本的
	// 获取消息ID，这是映射表中的键。
	id := item.Value.(*Message).ID

	// 检查延迟消息映射表中是否已存在此消息ID。
	_, ok := c.deferredMessages[id]
	if ok {
		// 如果已存在，释放锁并返回错误。
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}

	// 将消息ID与item一起添加到映射表中。
	c.deferredMessages[id] = item

	// 释放锁。
	c.deferredMutex.Unlock()

	// 操作成功，返回nil。
	return nil
}

// addToDeferredPQ 将一个pqueue.Item添加到延迟处理的优先队列中。
//
// 该方法首先通过deferredMutex锁定访问deferredPQ，以确保线程安全，
// 然后使用heap.Push方法将item添加到deferredPQ中，最后释放锁。
//
// 参数:
//
//	item (*pqueue.Item): 要添加到延迟处理队列的项。
//
// 该方法没有返回值。
func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}

// PutMessage 向频道中添加消息。
//
// 该方法首先检查频道是否正在退出，如果是在退出状态，则返回错误。
// 成功添加消息后，会原子性地增加消息计数。
//
// 参数:
//
//	m (*Message): 要添加到频道的消息指针。
//
// 返回值:
//
//	error: 如果频道正在退出或在添加消息时发生错误，则返回错误。
func (c *Channel) PutMessage(m *Message) error {
	// 加读锁以保护退出状态，延迟解锁直到方法结束。
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	// 检查频道是否正在退出，如果是，返回错误。
	if c.Exiting() {
		return errors.New("exiting")
	}

	// 尝试将消息添加到频道中。
	err := c.put(m)
	if err != nil {
		// 如果添加消息时发生错误，返回该错误。
		return err
	}

	// 成功添加消息后，原子性地增加消息计数。
	atomic.AddUint64(&c.messageCount, 1)

	// 消息添加成功，返回nil。
	return nil
}

// Exiting 检查通道是否已退出
//
// 返回值表示通道是否已设置退出标志为true。
// 通过原子操作加载exitFlag，确保在并发环境下安全检查退出状态。
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// put将消息放入内存队列或回退到后端
// 此函数尝试将消息放入Channel的消息队列（memoryMsgChan）中，
// 如果队列已满，则尝试将消息写入后端。
// 参数:
//
//	m: 要处理的消息指针
//
// 返回值:
//
//	error: 在将消息写入后端时发生的错误，如果成功则返回nil
func (c *Channel) put(m *Message) error {
	// 尝试将消息放入内存队列中
	select {
	case c.memoryMsgChan <- m:
		// 如果成功放入，直接返回nil，不需要进一步操作
		return nil
	default:
		// 如果内存队列已满，尝试将消息写入后端
		err := writeMessageToBackend(m, c.backend)
		// 根据写入后端的结果更新nsqd的健康状态
		c.nsqd.SetHealth(err)
		if err != nil {
			// 如果写入后端失败，记录错误并返回
			c.nsqd.logf(LOG_ERROR, "CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	// 如果写入后端成功，返回nil
	return nil
}

func (c *Channel) Pause() error {
	return c.doPause(true)
}

// UnPause 恢复频道的传输。
// 该方法将频道的状态从暂停状态恢复到正常传输状态。
// 它通过调用 doPause 方法实现，传入 false 表示不暂停。
func (c *Channel) UnPause() error {
	return c.doPause(false)
}

// doPause 设置或解除频道的暂停状态。
// 该方法通过原子操作修改频道的 paused 状态，并根据 pause 参数的值，通知所有客户端暂停或恢复传输。
// 参数:
//   - pause: 表示是否暂停频道。true 表示暂停，false 表示恢复。
//
// 返回值:
//
//	该方法不返回任何错误，始终返回 nil。
func (c *Channel) doPause(pause bool) error {
	// 使用原子操作设置 paused 的值，确保线程安全。
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	// 加读锁以保护 clients 切片，确保在操作过程中数据的一致性。
	c.RLock()
	// 遍历所有客户端，根据 pause 参数的值调用对应的方法。
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	// 解锁以释放读锁，允许其他写操作。
	c.RUnlock()
	return nil
}

// StartInFlightTimeout 为一条消息设置处理中的超时，并将其加入到处理中的消息队列。
// 这个方法被设计用于在消息发送到客户端之前，为消息设定一个超时时间。
// 如果在超时时间内消息没有被确认，那么它将被重新发送。
// 参数:
//
//	msg *Message: 待发送的消息指针。
//	clientID int64: 目标客户端的ID。
//	timeout time.Duration: 消息的超时时间。
//
// 返回值:
//
//	error: 如果消息添加失败，返回错误；否则返回nil。
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	// 记录当前时间，用于消息时间戳和超时计算。
	now := time.Now()

	// 设置消息的客户端ID，表明这条消息是为哪个客户端准备的。
	msg.clientID = clientID

	// 设置消息的创建（发送）时间戳。
	msg.deliveryTS = now

	// 根据超时时间计算消息的优先级，并将其设置为消息的UnixNano时间。
	// 这里通过将当前时间加上超时时间来计算出消息的超时绝对时间。
	msg.pri = now.Add(timeout).UnixNano()

	// 将消息添加到处理中的消息列表。
	// 这个步骤包括更新消息的元数据和将消息加入到优先级队列中。
	err := c.pushInFlightMessage(msg)
	if err != nil {
		// 如果添加消息过程中出现错误，则返回该错误。
		return err
	}

	// 将消息添加到处理中的优先级队列中。
	// 这个步骤确保消息根据其超时时间被正确排序。
	c.addToInFlightPQ(msg)

	// 如果所有步骤都成功，返回nil。
	return nil
}

// pushInFlightMessage 将一条消息添加到处理中的消息集合中。
// 这个方法确保同一消息ID不会被重复添加。
// 参数:
//
//	msg (*Message): 待添加的消息指针。
//
// 返回值:
//
//	error: 如果消息ID已经存在于处理中的消息集合中，则返回错误。
func (c *Channel) pushInFlightMessage(msg *Message) error {
	// 加锁以确保线程安全
	c.inFlightMutex.Lock()
	// 检查消息ID是否已经在处理中的消息集合中
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		// 如果消息ID已存在，解锁并返回错误
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	// 将消息添加到处理中的消息集合中
	c.inFlightMessages[msg.ID] = msg
	// 解锁以释放互斥锁
	c.inFlightMutex.Unlock()
	// 操作成功，返回nil
	return nil
}

// addToInFlightPQ 将消息添加到处理中的优先队列中。
// 此方法确保消息添加操作的线程安全。
//
// 参数:
//
//	msg (*Message): 要添加到处理中优先队列的消息指针。
//
// 该方法不返回任何值。
func (c *Channel) addToInFlightPQ(msg *Message) {
	// 加锁以确保线程安全
	c.inFlightMutex.Lock()
	// 将消息推入处理中优先队列
	c.inFlightPQ.Push(msg)
	// 解锁以释放互斥锁
	c.inFlightMutex.Unlock()
}

// FinishMessage 完成一条消息的处理。
//
// 该函数通过clientID和MessageID来定位并移除正在处理中的消息，同时更新端到端的处理延迟统计。
// 如果消息不存在或移除失败，则返回错误。
//
// 参数:
//
//	clientID - 消息所属的客户端ID。
//	id - 消息的唯一标识符。
//
// 返回值:
//
//	error - 在消息不存在或移除操作中出现错误时返回。
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	// 尝试获取正在处理中的消息，如果消息不存在，则返回错误。
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}

	// 从正在处理的消息队列中移除该消息。
	c.removeFromInFlightPQ(msg)

	// 如果存在端到端处理延迟统计流，则插入当前消息的时间戳以进行延迟计算。
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}

	return nil
}

// popInFlightMessage 通过客户端ID和消息ID从处理中的消息映射中删除消息，并返回该消息。
// 如果给定的客户端ID不拥有该消息，或者消息ID不存在于处理中的消息中，则返回错误。
// 参数:
//
//	clientID - 消息所属的客户端的ID
//	id - 要从处理中消息中移除的消息的ID
//
// 返回值:
//
//	*Message - 被移除的消息指针
//	error - 如果消息ID不在处理中或消息不属于给定的客户端ID，则返回错误
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	// 加锁以确保线程安全地访问处理中的消息映射
	c.inFlightMutex.Lock()
	// 从处理中的消息映射中查找指定ID的消息
	msg, ok := c.inFlightMessages[id]
	// 如果消息不存在，解锁并返回错误
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	// 如果消息存在但不属于给定的客户端ID，解锁并返回错误
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	// 从处理中的消息映射中删除消息
	delete(c.inFlightMessages, id)
	// 解锁以允许其他线程访问处理中的消息映射
	c.inFlightMutex.Unlock()
	// 返回移除的消息
	return msg, nil
}

// removeFromInFlightPQ 从正在进行的优先队列中移除指定消息。
// 该方法首先尝试获取锁，如果消息的索引为-1（表示消息已经被处理），则直接释放锁并返回。
// 否则，从inFlightPQ中移除该消息的索引，然后释放锁。
func (c *Channel) removeFromInFlightPQ(msg *Message) {
	// 使用互斥锁确保并发安全
	c.inFlightMutex.Lock()
	// 如果消息索引为-1，表示消息已经被处理，无需进一步操作
	if msg.index == -1 {
		c.inFlightMutex.Unlock()
		return
	}
	// 从进行中的优先队列中移除消息的索引
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

// RequeueMessage 重新排队处理一条消息。
// 该函数尝试从正在处理的消息中找到并移除指定的消息，然后根据情况将其重新排队或延迟处理。
// 参数:
//
//	clientID - 消息所属客户端的ID。
//	id - 消息的唯一标识符。
//	timeout - 重新排队前的延迟时间。如果为0，则立即重新排队。
//
// 返回值:
//
//	error - 在查找或处理消息时可能遇到的错误。
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// 首先尝试获取正在处理的消息队列中指定客户端ID和消息ID的消息。
	// 如果消息不存在或已被处理，则返回错误。
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}

	// 从正在处理的消息优先队列中移除该消息。
	c.removeFromInFlightPQ(msg)

	// 增加重新排队计数，以跟踪重新排队消息的次数。
	atomic.AddUint64(&c.requeueCount, 1)

	// 如果指定的延迟时间为0，表示不需要延迟，立即重新排队消息。
	if timeout == 0 {
		// 为了防止在处理过程中服务退出，需要检查服务状态。
		c.exitMutex.RLock()
		if c.Exiting() {
			c.exitMutex.RUnlock()
			return errors.New("exiting")
		}
		// 将消息重新放入队列中等待处理。
		err := c.put(msg)
		c.exitMutex.RUnlock()
		return err
	}

	// 如果指定了延迟时间，启动一个延迟任务，将消息在指定时间后重新排队。
	return c.StartDeferredTimeout(msg, timeout)
}

// TouchMessage 重新设置消息的超时时间，确保消息在新的超时时间内重新激活
// 参数:
//
//	clientID - 发送消息的客户端ID
//	id - 消息ID
//	clientMsgTimeout - 客户端消息超时时间
//
// 返回值:
//
//	error - 如果操作失败，返回错误信息
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	// 从正在进行的消息映射中移除消息
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	// 从正在进行的消息优先队列中移除消息
	c.removeFromInFlightPQ(msg)

	// 计算新的超时时间
	newTimeout := time.Now().Add(clientMsgTimeout)
	// 确保新的超时时间不超过最大消息超时设置
	if newTimeout.Sub(msg.deliveryTS) >=
		c.nsqd.getOpts().MaxMsgTimeout {
		newTimeout = msg.deliveryTS.Add(c.nsqd.getOpts().MaxMsgTimeout)
	}

	// 更新消息的优先级为新的超时时间
	msg.pri = newTimeout.UnixNano()
	// 将消息重新添加到正在进行的消息映射
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	// 将消息重新添加到正在进行的消息优先队列
	c.addToInFlightPQ(msg)
	return nil
}

// RemoveClient 从频道中移除指定客户端。
// 该方法首先检查频道是否处于退出状态，如果不在退出状态，
// 则检查指定的客户端ID是否已存在于频道的客户端列表中。
// 如果客户端存在，则将其从列表中移除。
// 如果移除客户端后，频道中没有剩余客户端，并且频道是临时的，
// 则触发频道的删除回调。
func (c *Channel) RemoveClient(clientID int64) {
	// 加读锁以保护退出状态，确保数据一致性
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	// 检查频道是否正在退出，如果是，则不处理移除请求
	if c.Exiting() {
		return
	}

	// 加读锁以检查客户端是否存在
	c.RLock()
	_, ok := c.clients[clientID]
	c.RUnlock()
	if !ok {
		return
	}

	// 加写锁以安全地移除客户端
	c.Lock()
	delete(c.clients, clientID)
	numClients := len(c.clients)
	c.Unlock()

	// 如果频道中没有剩余客户端，并且频道是临时的，安排删除回调
	if numClients == 0 && c.ephemeral {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

// AddClient 尝试向 Channel 中添加一个新的消费者。
// 如果消费者已经存在，则不进行任何操作并返回nil。
// 如果 Channel 正在退出，则返回错误。
// 如果添加消费者会导致消费者数量超过设定的限制，则返回错误。
func (c *Channel) AddClient(clientID int64, client Consumer) error {
	// 读锁保护，确保在检查Exiting状态时不会有写操作
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	// 如果Channel正在退出，不允许添加新消费者
	if c.Exiting() {
		return errors.New("exiting")
	}

	// 读锁保护，确保在访问clients时不会有写操作
	c.RLock()
	_, ok := c.clients[clientID]
	numClients := len(c.clients)
	c.RUnlock()

	// 如果消费者已经存在，直接返回nil
	if ok {
		return nil
	}

	// 获取最大消费者限制
	maxChannelConsumers := c.nsqd.getOpts().MaxChannelConsumers
	// 如果当前消费者数已经达到限制，返回错误
	if maxChannelConsumers != 0 && numClients >= maxChannelConsumers {
		return fmt.Errorf("consumers for %s:%s exceeds limit of %d",
			c.topicName, c.name, maxChannelConsumers)
	}

	// 写锁保护，确保在添加消费者时不会有其他并发操作
	c.Lock()
	c.clients[clientID] = client
	c.Unlock()
	return nil
}

// Depth 返回通道中的消息总数。
// 该方法用于获取当前通道中消息的数量，包括内存中的消息和后端存储的消息。
// 返回值是通道中消息的总数。
func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth()
}

// processInFlightQueue 处理正在进行中的队列。
// 该函数尝试处理通道中正在进行中的消息队列，它会检查消息的过期时间，
// 并将过期的消息移出队列，同时通知相应的客户端消息已超时。
// 参数 t 是当前时间戳，用于判断消息是否已经过期。
func (c *Channel) processInFlightQueue(t int64) bool {
	// 加读锁以保护通道的退出状态。
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	// 如果通道正在退出，则不处理消息队列。
	if c.Exiting() {
		return false
	}

	// 标记是否进行了修改。
	dirty := false
	for {
		// 加写锁以保护消息队列。
		c.inFlightMutex.Lock()
		// 尝试从队列中移除比给定时间戳 t 早的消息。
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		// 如果没有可移除的消息，退出循环。
		if msg == nil {
			goto exit
		}
		dirty = true

		// 从正在进行中的消息中移除指定的客户端和消息ID。
		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		// 如果移除失败，退出循环。
		if err != nil {
			goto exit
		}
		// 增加超时计数。
		atomic.AddUint64(&c.timeoutCount, 1)
		// 加读锁以保护客户端列表。
		c.RLock()
		// 查找消息所属的客户端。
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		// 如果找到了对应的客户端，通知客户端消息已超时。
		if ok {
			client.TimedOutMessage()
		}
		// 将处理完的消息重新放入空闲池。
		c.put(msg)
	}

exit:
	// 返回是否对队列进行了修改。
	return dirty
}

// processDeferredQueue 处理延迟消息队列中的消息。
// 该方法根据当前时间戳 t 来判断是否应该处理队列中的消息。
// 如果消息的延迟时间已到，则将其从队列中取出并放入普通消息队列中以进行处理。
// 参数:
//
//	t - 当前时间戳，用于判断消息是否已达到延迟时间。
//
// 返回值:
//
//	bool - 表示是否有消息被处理。
func (c *Channel) processDeferredQueue(t int64) bool {
	// 加读锁以保护通道的状态信息。
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	// 检查通道是否正在退出，如果是，则不处理任何消息。
	if c.Exiting() {
		return false
	}

	// 记录是否有消息被处理的标志。
	dirty := false
	for {
		// 加写锁以保护延迟消息队列。
		c.deferredMutex.Lock()
		// 尝试从延迟优先队列中获取下一个到期的消息。
		item, _ := c.deferredPQ.PeekAndShift(t)
		// 解锁延迟消息队列。
		c.deferredMutex.Unlock()

		// 如果没有消息到期，则退出循环。
		if item == nil {
			goto exit
		}
		// 标记有消息被处理。
		dirty = true

		// 将队列项转换为消息类型。
		msg := item.Value.(*Message)
		// 尝试从延迟消息映射中移除该消息。
		_, err := c.popDeferredMessage(msg.ID)
		// 如果消息已被移除，则跳转到循环结束。
		if err != nil {
			goto exit
		}
		// 将消息放入普通消息队列中以进行处理。
		c.put(msg)
	}

exit:
	// 返回是否有消息被处理的标志。
	return dirty
}

// popDeferredMessage 从延迟消息队列中移除并返回指定ID的延迟消息。
// 该方法需要传入一个MessageID来标识要移除的消息。
// 如果指定ID的消息不存在于延迟消息队列中，则返回一个错误。
func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	// 加锁以确保线程安全
	c.deferredMutex.Lock()
	// 尝试从延迟消息映射中查找指定ID的消息
	item, ok := c.deferredMessages[id]
	if !ok {
		// 如果未找到指定ID的延迟消息，则解锁并返回错误
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	// 从延迟消息映射中移除指定ID的消息
	delete(c.deferredMessages, id)
	// 解锁以释放资源
	c.deferredMutex.Unlock()
	// 返回找到的延迟消息项
	return item, nil
}
