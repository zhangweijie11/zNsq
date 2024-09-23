package nsqd

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID         MessageID     // 消息 ID
	Body       []byte        // 消息具体数据
	Timestamp  int64         // 消息时间
	Attempts   uint16        // 消息尝试次数
	deliveryTS time.Time     // 消息投递时间
	clientID   int64         // 接收消息的客户端 ID
	pri        int64         // 消息优先级
	index      int           // 消息索引
	deferred   time.Duration // 消息延迟发送时间
}

// NewMessage 创建并返回一个新的消息对象。
// 该函数接收一个 MessageID 类型的 id 和一个字节切片 body 作为参数，
// 并返回一个指向包含这些信息的消息对象的指针。
// 调用此函数时，并不会修改传入的参数。
func NewMessage(id MessageID, body []byte) *Message {
	// 使用传入的 id 和 body 参数初始化消息对象，
	// 当前时间以 UnixNano 格式记录到消息的时间戳中。
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// writeMessageToBackend 将消息写入后端队列。
// 该函数首先从缓冲池中获取一个缓冲区，用于临时存储消息数据。
// 然后，它调用消息的WriteTo方法将消息写入缓冲区。
// 最后，将缓冲区中的消息数据放入后端队列。
// 参数:
//
//	msg (*Message): 指向要写入的消息的指针。
//	bq (BackendQueue): 实现了Put方法的后端队列接口。
//
// 返回值:
//
//	error: 如果在写入过程中发生错误，则返回错误；否则返回nil。
func writeMessageToBackend(msg *Message, bq BackendQueue) error {
	// 从缓冲池中获取一个缓冲区。
	buf := bufferPoolGet()
	// 使用完毕后，确保缓冲区被放回池中，以供后续使用。
	defer bufferPoolPut(buf)

	// 将消息写入缓冲区。
	// 如果写入过程中发生错误，直接返回错误。
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}

	// 将缓冲区中的消息数据放入后端队列。
	// 注意：此处假设所有消息都被成功写入缓冲区，错误处理在WriteTo方法中进行。
	return bq.Put(buf.Bytes())
}

// WriteTo 将消息写入到指定的写入器中。
//
// 参数:
//
//	w: io.Writer接口的实现，用于写入消息数据。
//
// 返回值:
//
//	int64: 成功写入的字节数。
//	error: 写入过程中可能遇到的错误，如果没有错误则返回nil。
//
// 该方法首先将消息的Timestamp和Attempts信息按照大端字节序写入到一个10字节的缓冲区中，
// 然后将缓冲区的数据、消息ID和消息体依次写入到给定的写入器中。
// 在每次写入操作后，累计写入的字节数，并检查是否有写入错误。
// 如果在写入过程中发生错误，则停止写入并返回当前累计的写入字节数和遇到的错误。
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	// 将Timestamp转换为8字节的大端字节序并存入缓冲区
	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	// 将Attempts转换为2字节的大端字节序并存入缓冲区
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	// 将包含Timestamp和Attempts的缓冲区写入到写入器中
	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	// 将消息的ID写入到写入器中
	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	// 将消息体写入到写入器中
	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	// 返回成功写入的总字节数和没有错误的情况下nil错误值
	return total, nil
}

// decodeMessage 解码给定的字节切片b，生成一个Message对象。
// 该函数主要负责从字节切片中提取Timestamp、Attempts和ID，并忽略消息的剩余部分。
// 参数:
//
//	b []byte: 待解码的字节切片，包含Timestamp、Attempts、ID和可能的消息体。
//
// 返回值:
//
//	*Message: 解码后生成的Message对象指针。
//	error: 如果消息缓冲区大小无效，则返回错误。
func decodeMessage(b []byte) (*Message, error) {
	// 初始化Message结构体实例。
	var msg Message

	// 检查输入字节切片的长度是否满足最小有效消息长度要求。
	if len(b) < minValidMsgLength {
		// 如果长度不足，返回错误，提示消息缓冲区大小无效。
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	// 从字节切片的前8个字节解码Timestamp，使用大端字节序。
	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	// 从字节切片的接下来2个字节解码Attempts，使用大端字节序。
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	// 将字节切片中的接下来MsgIDLength长度的字节复制到msg.ID中。
	copy(msg.ID[:], b[10:10+MsgIDLength])
	// 将剩余的字节切片赋值给msg.Body。
	msg.Body = b[10+MsgIDLength:]

	// 返回解码后的Message对象指针和nil错误，表示解码成功。
	return &msg, nil
}
