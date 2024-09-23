package nsqd

import (
	"errors"
	"github.com/nsqio/go-diskqueue"
	"github.com/zhangweijie11/zNsq/internal/lg"
	"github.com/zhangweijie11/zNsq/internal/quantile"
	"github.com/zhangweijie11/zNsq/internal/util"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Topic struct {
	sync.RWMutex
	nsqd              *NSQD
	exitFlag          int32                 // 退出标志
	name              string                // 主题名称
	startChan         chan int              // 启动通道
	exitChan          chan int              // 退出通道
	channelUpdateChan chan int              // 通知更新通道
	idFactory         *guidFactory          // GUID 生成器
	deleteCallback    func(*Topic)          // 删除回调函数
	deleter           sync.Once             // 确保删除操作只执行一次
	pauseChan         chan int              // 暂停通道
	ephemeral         bool                  // 临时主题
	paused            int32                 // 主题是否停用
	channelMap        map[string]*Channel   // 存储 Channel
	waitGroup         util.WaitGroupWrapper // goroutine 管理
	backend           BackendQueue          // 存储消息
	memoryMsgChan     chan *Message         // 内存消息队列
	messageCount      uint64                // 消息计数
	messageBytes      uint64                // 消息字节计数
}

// NewTopic 创建并初始化一个新的Topic实例。
// 参数 topicName 是主题的名称。
// 参数 nsqd 是指向NSQD实例的指针，用于获取配置和通知操作。
// 参数 deleteCallback 是当Topic被删除时调用的回调函数。
// 返回值是新创建的Topic实例的指针。
func NewTopic(topicName string, nsqd *NSQD, deleteCallback func(*Topic)) *Topic {
	// 初始化Topic实例的基本属性和队列。
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     make(chan *Message, nsqd.getOpts().MemQueueSize),
		startChan:         make(chan int, 1),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		nsqd:              nsqd,
		paused:            0,
		pauseChan:         make(chan int),
		deleteCallback:    deleteCallback,
		idFactory:         NewGUIDFactory(nsqd.getOpts().ID),
	}

	// 判断topic是否为ephemeral类型。
	if strings.HasSuffix(topicName, "#ephemeral") {
		t.ephemeral = true
		t.backend = newDummyBackendQueue()
	} else {
		// 初始化磁盘队列的日志记录器。
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		// 初始化磁盘队列。
		t.backend = diskqueue.New(
			topicName,
			nsqd.getOpts().DataPath,
			nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			nsqd.getOpts().SyncEvery,
			nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	// 将消息泵操作包装到等待组中。
	t.waitGroup.Wrap(t.messagePump)

	// 通知NSQD实例关于新Topic的创建。
	t.nsqd.Notify(t, !t.ephemeral)

	return t
}

func (t *Topic) Delete() error {
	return t.exit(true)
}

// exit 是 Topic 结构体的方法，用于处理主题的关闭或删除操作。
// 参数 deleted 指定是否删除主题。如果为 true，则从内存和 lookup 表中彻底删除主题；
// 如果为 false，则仅关闭主题但不删除。
// 方法返回一个 error，表示操作是否成功。
func (t *Topic) exit(deleted bool) error {
	// 检查是否已经设置了退出标志。如果已经设置，表示主题已在关闭或删除过程中，返回错误。
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("主题正在处理中")
	}

	// 如果 deleted 为 true，表示需要删除主题。
	if deleted {
		// 记录主题删除日志
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.name)
		// 通知 nsqd 主循环，从 lookup 表中删除该主题
		t.nsqd.Notify(t, !t.ephemeral)
	} else {
		// 记录主题关闭日志
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.name)
	}

	// 关闭退出通道，用于通知所有的消息泵（message pump）停止工作
	close(t.exitChan)

	// 等待所有的消息泵完成退出
	t.waitGroup.Wait()

	// 如果需要删除主题
	if deleted {
		// 加锁，确保并发安全
		t.Lock()
		// 遍历主题下的所有通道，从 map 中移除并删除每个通道
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		// 解锁
		t.Unlock()

		// 清空队列，这也会删除后端文件
		t.Empty()
		// 删除主题的后端存储，并返回结果
		return t.backend.Delete()
	}

	// 如果不需要删除主题，但需要关闭所有通道
	t.RLock()
	for _, channel := range t.channelMap {
		// 关闭每个通道，记录错误（如果有）
		err := channel.Close()
		if err != nil {
			t.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", channel.name, err)
		}
	}
	t.RUnlock()

	// 将任何剩余未处理的数据写入磁盘
	t.flush()
	// 关闭主题的后端存储，并返回结果
	return t.backend.Close()
}

// IsPaused 当前主题是否已经停用
func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}

// Empty 清空主题中的所有消息。
// 该方法首先尝试从 memoryMsgChan 中清空所有消息，
// 然后调用 backend 的 Empty 方法清空后端存储中的消息。
func (t *Topic) Empty() error {
	// 循环尝试从 memoryMsgChan 中清空所有消息
	for {
		select {
		// 尝试从 memoryMsgChan 中接收消息
		case <-t.memoryMsgChan:
			// 接收到消息后，继续循环尝试清空其余消息
		default:
			// 如果 memoryMsgChan 中没有消息可接收，则跳出循环
			goto finish
		}
	}

finish:
	// 调用后端存储的 Empty 方法清空消息
	return t.backend.Empty()
}

// flush 将内存中的消息刷新到后端存储。
// 它会检查内存消息通道中是否有未处理的消息，如果有，则将它们依次写入后端。
// 这个方法对于保持内存的清洁和确保消息持久性至关重要。
func (t *Topic) flush() error {
	// 检查是否有消息需要刷新
	if len(t.memoryMsgChan) > 0 {
		t.nsqd.logf(LOG_INFO,
			"TOPIC(%s): flushing %d memory messages to backend",
			t.name, len(t.memoryMsgChan))
	}

	// 通过select尝试从内存消息通道中取出消息并写入后端
	for {
		select {
		case msg := <-t.memoryMsgChan:
			// 尝试将消息写入后端，如果失败则记录错误
			err := writeMessageToBackend(msg, t.backend)
			if err != nil {
				t.nsqd.logf(LOG_ERROR,
					"ERROR: failed to write message to backend - %s", err)
			}
		default:
			// 如果通道中没有消息了，则退出循环
			goto finish
		}
	}

finish:
	// 循环结束后返回nil，表示刷新完成
	return nil
}

// messagePump 是 Topic 结构体的方法，用于处理主题的消息泵。
func (t *Topic) messagePump() {
	// 初始化变量
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan <-chan []byte

	// 在 Start() 之前不传递消息，但避免 Pause() 或 GetChannel() 阻塞
	for {
		select {
		case <-t.channelUpdateChan:
			// 忽略通道更新
			continue
		case <-t.pauseChan:
			// 忽略暂停信号
			continue
		case <-t.exitChan:
			// 接收到退出信号，跳到退出逻辑
			goto exit
		case <-t.startChan:
			// 接收到启动信号
		}
		break
	}

	// 加读锁，遍历并更新 chans 列表
	t.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()

	// 如果有订阅的通道且未暂停，则初始化消息通道
	if len(chans) > 0 && !t.IsPaused() {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	// 主消息循环
	for {
		select {
		case msg = <-memoryMsgChan:
			// 从内存消息通道接收消息
		case buf = <-backendChan:
			// 从后端消息通道接收并解码消息
			msg, err = decodeMessage(buf)
			if err != nil {
				// 解码失败，记录错误并继续循环
				t.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
		case <-t.channelUpdateChan:
			// 通道更新，重新获取订阅的通道列表
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()

			// 根据当前状态更新消息通道
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.pauseChan:
			// 暂停，根据当前状态更新消息通道
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan:
			// 接收到退出信号，跳到退出逻辑
			goto exit
		}

		// 遍历订阅的通道，分发消息
		for i, channel := range chans {
			chanMsg := msg
			// 如果不是第一个通道，复制消息
			if i > 0 {
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
				chanMsg.deferred = msg.deferred
			}
			// 处理延迟消息
			if chanMsg.deferred != 0 {
				channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
				continue
			}
			// 将消息放入通道
			err := channel.PutMessage(chanMsg)
			if err != nil {
				// 放入消息失败，记录错误
				t.nsqd.logf(LOG_ERROR,
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}

exit:
	// 退出消息泵，记录日志
	t.nsqd.logf(LOG_INFO, "TOPIC(%s): closing ... messagePump")
}

// GetChannel 通过频道名称获取频道对象。
// 如果频道不存在，则创建新的频道。
// 参数:
//
//	channelName - 需要获取的频道的名称。
//
// 返回值:
//
//	*Channel - 对应名称的频道对象指针。
func (t *Topic) GetChannel(channelName string) *Channel {
	// 加锁保护频道的并发访问
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	// 解锁
	t.Unlock()

	// 如果创建了新的频道，更新消息泵的状态
	if isNew {
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	// 返回获取的频道对象
	return channel
}

func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.nsqd, deleteCallback)
		t.channelMap[channelName] = channel
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

// DeleteExistingChannel 删除一个已存在的频道。
// 参数 channelName 指定要删除的频道的名称。
// 返回错误如果指定的频道不存在。
func (t *Topic) DeleteExistingChannel(channelName string) error {
	// 读取锁保护channelMap
	t.RLock()
	channel, ok := t.channelMap[channelName]
	t.RUnlock()
	if !ok {
		return errors.New("channel does not exist")
	}

	// 记录删除频道日志
	t.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.name, channel.name)

	// 删除频道中的所有消息并在关闭频道之前清空
	// 这样做可以确保在我们下面无锁状态下处理频道时
	// 任何传入的订阅请求都会出错，并且不会创建新的频道
	channel.Delete()

	// 写锁保护channelMap进行修改
	t.Lock()
	delete(t.channelMap, channelName)
	numChannels := len(t.channelMap)
	t.Unlock()

	// 更新messagePump状态
	// 确保频道删除后，相关消息泵状态得到正确更新
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}

	// 如果删除频道后没有剩余频道，并且主题是临时的，则调用删除回调
	if numChannels == 0 && t.ephemeral {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// Start 开启话题处理
//
// 通过向 startChan 发送信号来启动话题处理。使用 select 语句以非阻塞方式发送信号，
// 避免了因重复启动造成的资源浪费。
func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
		// 当 startChan 准备好接收时，发送信号，表示话题可以开始处理。
	default:
		// 当 startChan 无法接收时，跳过启动操作，避免重复启动。
	}
}

// Pause Topic 的 Pause 方法用于暂停当前主题的处理
// 该方法会调用底层的 doPause 方法来实现暂停逻辑
// 如果已经暂停，则再次调用 Pause 方法不会产生任何效果
func (t *Topic) Pause() error {

	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

// doPause 是实际执行暂停或恢复处理的函数
// 它通过原子操作更新 paused 标志，并通过 pauseChan 通知其他协程
// 参数 pause 指定了是暂停（true）还是恢复（false）
// 该方法不返回任何错误，因为它仅负责设置内部状态
func (t *Topic) doPause(pause bool) error {
	// 使用原子操作设置 paused 标志，确保线程安全
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	// 通过选择器尝试发送暂停信号或等待退出信号
	// 这里不阻塞，如果 pauseChan 没有接收方，那么会立即选择 case <-t.exitChan
	// 这个设计确保了在退出时不会阻塞过久
	select {
	case t.pauseChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

// GenerateID 生成消息ID。
//
// 本函数尝试生成一个全局唯一标识符（GUID）用于消息ID。当生成GUID失败时，它会记录错误并重试，直到成功为止。
// 此方法解释了为什么会有错误日志输出，即使GUID生成的失败率非常低，因为失败仍然有可能发生，尽管极为罕见。
//
// t *Topic - 主题对象，调用idFactory来生成GUID。
// 返回值 MessageID - 返回生成的消息ID，以十六进制字符串形式表示。
func (t *Topic) GenerateID() MessageID {
	var i int64 = 0
	for {
		id, err := t.idFactory.NewGUID()
		if err == nil {
			return id.Hex()
		}
		if i%10000 == 0 {
			t.nsqd.logf(LOG_ERROR, "TOPIC(%s): failed to create guid - %s", t.name, err)
		}
		time.Sleep(time.Millisecond)
		i++
	}
}

// PutMessage 向主题(Topic)中发送消息(Message)。
// 该方法首先检查Topic是否处于退出状态，如果处于退出状态，则返回错误。
// 如果不是退出状态，则尝试发送消息，并更新消息统计计数和字节统计。
// 参数:
//
//	m *Message: 要发送的消息指针。
//
// 返回值:
//
//	error: 如果发送消息失败或Topic正在退出，则返回相应的错误。
func (t *Topic) PutMessage(m *Message) error {
	// 加读锁并 defer 解锁，保护共享资源不被并发访问。
	t.RLock()
	defer t.RUnlock()

	// 检查Topic是否在退出过程中，如果是，则返回错误。
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	// 尝试发送消息。
	err := t.put(m)
	if err != nil {
		return err
	}

	// 成功发送消息后，更新消息计数和字节计数。
	atomic.AddUint64(&t.messageCount, 1)
	atomic.AddUint64(&t.messageBytes, uint64(len(m.Body)))

	return nil
}

// put尝试将消息m放入主题t的内存队列或后端队列。
// 如果内存队列已满，则尝试直接写入后端。
// 返回错误如果写入后端失败。
func (t *Topic) put(m *Message) error {
	// 当内存队列大小不为0时，尝试使用内存队列，
	// 对于临时主题或延迟消息，也尝试使用内存队列，
	// 因为这类消息在后端队列中会丢失延迟定时器。
	if cap(t.memoryMsgChan) > 0 || t.ephemeral || m.deferred != 0 {
		// 尝试将消息放入内存队列
		select {
		case t.memoryMsgChan <- m:
			// 如果成功放入内存队列，直接返回成功
			return nil
		default:
			// 如果内存队列已满，尝试写入后端
			break
		}
	}

	// 尝试将消息写入后端队列
	err := writeMessageToBackend(m, t.backend)
	// 根据写入结果更新主题的健康状态
	t.nsqd.SetHealth(err)
	if err != nil {
		// 如果写入失败，记录错误并返回错误
		t.nsqd.logf(LOG_ERROR,
			"TOPIC(%s) ERROR: failed to write message to backend - %s",
			t.name, err)
		return err
	}
	// 写入成功
	return nil
}

// PutMessages 向主题(Topic)中添加多条消息。
// 参数:
//
//	msgs: 指向一个消息列表的指针，表示需要添加到主题中的多条消息。
//
// 返回值:
//
//	error: 表示操作是否成功，如果返回nil，则表示操作成功；如果返回非nil值，则表示操作中遇到错误。
func (t *Topic) PutMessages(msgs []*Message) error {
	// 加读锁，并在函数退出时自动解锁。
	t.RLock()
	defer t.RUnlock()

	// 检查主题是否正在退出。
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		// 如果主题正在退出，则返回错误。
		return errors.New("exiting")
	}

	// 初始化消息总字节数。
	messageTotalBytes := 0

	// 遍历消息列表，尝试添加每一条消息。
	for i, m := range msgs {
		// 将消息添加到主题中。
		err := t.put(m)
		if err != nil {
			// 如果添加消息失败，则更新已添加的消息计数和字节数，并返回错误。
			atomic.AddUint64(&t.messageCount, uint64(i))
			atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
			return err
		}
		// 更新消息总字节数。
		messageTotalBytes += len(m.Body)
	}

	// 更新主题中的消息字节数和消息计数。
	atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	// 所有消息添加成功，返回nil。
	return nil
}

// GetExistingChannel 通过频道名称获取已存在的频道。
//
// 参数:
//   - channelName: 要获取的频道的名称。
//
// 返回值:
//   - *Channel: 如果找到频道，则返回频道的指针。
//   - error: 如果没有找到频道，则返回一个错误，指示频道不存在。
//
// 该方法首先对channelMap进行只读锁定，然后检查channelName是否存在于map中。
// 如果存在，则返回相应的Channel对象；如果不存在，则返回一个指示频道不存在的错误。
func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// Depth 返回主题中的消息深度。
// 消息深度是指当前在内存和后端存储中等待处理的消息总数。
// 此函数通过计算内存中的消息数量（t.memoryMsgChan的长度）
// 和后端存储中的消息深度之和来确定总的消息深度。
func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// AggregateChannelE2eProcessingLatency 聚合所有频道的端到端处理延迟
// 该方法返回一个quantile.Quantile对象，该对象聚合了所有频道的端到端处理延迟统计信息
func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile

	// 加读锁以遍历channelMap
	t.RLock()
	// 创建一个切片，用于存储channelMap中的所有频道指针
	realChannels := make([]*Channel, 0, len(t.channelMap))
	// 遍历channelMap，将所有频道指针添加到realChannels切片中
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	// 解锁，结束对channelMap的遍历
	t.RUnlock()

	// 遍历realChannels切片，聚合每个频道的端到端处理延迟
	for _, c := range realChannels {
		// 如果频道的端到端处理延迟流为空，则跳过该频道
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		// 如果latencyStream尚未初始化，则创建一个新的quantile.Quantile对象
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		// 将频道的端到端处理延迟流合并到latencyStream中
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}

	// 返回聚合后的端到端处理延迟流，如果没有可聚合的数据，则返回nil
	return latencyStream
}

// Exiting 检查话题是否已经设置了退出标志。
//
// 通过原子操作读取 exitFlag 变量，以确定话题是否正在退出。
// 这个方法用于在并发环境下安全地检查退出状态，避免直接访问共享变量。
//
// 返回值:
//   - 如果话题正在退出，则返回true。
//   - 如果话题未退出，则返回false。
func (t *Topic) Exiting() bool {
	// 使用原子操作读取退出标志，确保线程安全。
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// Close 关闭主题
//
// 该方法是 Topic 结构的成员函数，负责关闭当前的主题。
// 参数:
//
//	无
//
// 返回值:
//
//	error: 表示关闭操作是否成功的错误信息。
//
// 注意: 这个函数实际上调用了 t.exit(false) 来执行关闭操作。
func (t *Topic) Close() error {
	return t.exit(false)
}
