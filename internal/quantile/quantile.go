package quantile

import (
	"github.com/bmizerany/perks/quantile"
	"github.com/zhangweijie11/zNsq/internal/stringy"
	"strings"
	"sync"
	"time"
)

type Result struct {
	Count       int                  `json:"count"`
	Percentiles []map[string]float64 `json:"percentiles"`
}

// String 将结果对象中的百分位数信息转换为字符串表示形式。
// 遍历结果对象的Percentiles字段，将每个百分位数的价值转换为人类可读的字符串格式，
// 并使用逗号分隔符连接所有转换后的字符串。
// 该方法主要用于提供结果对象的字符串表示，以便于打印或日志记录。
func (r *Result) String() string {
	// 初始化一个字符串切片，用于存储转换后的百分位数字符串。
	var s []string

	// 遍历r.Percentiles中的每个百分位数项。
	for _, item := range r.Percentiles {
		// 将百分位数项中的"value"字段值转换为人类可读的字符串，并添加到切片s中。
		s = append(s, stringy.NanoSecondToHuman(item["value"]))
	}

	// 使用逗号和空格作为分隔符，将所有百分位数字符串连接成一个单一的字符串，并返回。
	return strings.Join(s, ", ")
}

// Quantile 结构体用于计算数据流的分位数。
// 它通过同步机制管理两个quantile.Stream，以支持滑动窗口模型进行实时数据分析。
type Quantile struct {
	sync.Mutex                        // 用于同步访问Quantile的资源
	streams        [2]quantile.Stream // 两个交替使用的quantile.Stream实例
	currentIndex   uint8              // 当前正在使用的stream的索引
	lastMoveWindow time.Time          // 上一次移动窗口的时间
	currentStream  *quantile.Stream   // 当前正在使用的stream指针
	Percentiles    []float64          // 需要计算的百分位数组
	MoveWindowTime time.Duration      // 移动窗口的时间间隔
}

// New 创建并初始化一个Quantile实例。
// 参数:
//
//	WindowTime - 滑动窗口的时间长度。
//	Percentiles - 需要计算的百分位数组。
//
// 返回值:
//
//	*Quantile - 一个初始化好的Quantile实例指针。
func New(WindowTime time.Duration, Percentiles []float64) *Quantile {
	q := Quantile{
		currentIndex:   0,              // 初始化当前使用的stream索引为0
		lastMoveWindow: time.Now(),     // 记录当前时间作为上一次移动窗口的时间
		MoveWindowTime: WindowTime / 2, // 将窗口时间的一半作为移动时间间隔
		Percentiles:    Percentiles,    // 保存需要计算的百分位数组
	}

	// 初始化两个quantile.Stream实例
	for i := 0; i < 2; i++ {
		q.streams[i] = *quantile.NewTargeted(Percentiles...)
	}
	q.currentStream = &q.streams[0] // 将第一个stream设置为当前正在使用的stream
	return &q                       // 返回初始化好的Quantile实例指针
}

// Insert 插入消息开始时间到 Quantile 结构中，并根据当前时间检查数据是否过时。
// 参数:
//
//	msgStartTime - 消息的开始时间
func (q *Quantile) Insert(msgStartTime int64) {
	q.Lock()

	now := time.Now()
	for q.IsDataStale(now) {
		q.moveWindow()
	}

	q.currentStream.Insert(float64(now.UnixNano() - msgStartTime))
	q.Unlock()
}

// IsDataStale 检查当前数据是否过时。
// 参数:
//
//	now - 当前时间
//
// 返回值:
//
//	bool - 如果数据过时返回 true，否则返回 false
func (q *Quantile) IsDataStale(now time.Time) bool {
	return now.After(q.lastMoveWindow.Add(q.MoveWindowTime))
}

// moveWindow 移动时间窗口，切换当前数据流并更新最后移动时间。
func (q *Quantile) moveWindow() {
	q.currentIndex ^= 0x1
	q.currentStream = &q.streams[q.currentIndex]
	q.lastMoveWindow = q.lastMoveWindow.Add(q.MoveWindowTime)
	q.currentStream.Reset()
}

// Result 方法返回一个 Result 指针，该 Result 包含当前 Quantile 对象计算出的分位数和计数。
// 如果 Quantile 对象为 nil，则返回一个空的 Result 对象指针。
// 该方法的主要目的是通过 QueryHandler 对当前配置的百分位数进行查询，并将结果封装进 Result 结构体中。
func (q *Quantile) Result() *Result {
	// 当 q 为 nil 时，直接返回一个空的 Result 对象指针。
	// 这样做是为了避免在 q 为 nil 时引发空指针异常。
	if q == nil {
		return &Result{}
	}
	// 获取并使用 QueryHandler 进行后续计算。
	// QueryHandler 是负责执行百分位数查询的核心对象。
	queryHandler := q.QueryHandler()
	// 初始化 Result 结构体，用于存储计数和分位数结果。
	// Percentiles 列表的长度与当前配置的百分位数列表长度相同。
	result := Result{
		Count:       queryHandler.Count(),
		Percentiles: make([]map[string]float64, len(q.Percentiles)),
	}
	// 遍历配置的百分位数列表，对每个百分位数进行查询，并将结果存储到 Result 结构体中。
	for i, p := range q.Percentiles {
		// 对当前百分位数进行查询，并将百分位数和对应的查询结果一起存储。
		value := queryHandler.Query(p)
		result.Percentiles[i] = map[string]float64{"quantile": p, "value": value}
	}
	// 返回封装好的 Result 对象指针。
	return &result
}

// QueryHandler 函数是 Quantile 类的查询处理方法，它负责合并当前所有流中的样本并返回一个针对指定百分位数的合并后的流。
// 这个函数首先检查数据是否陈旧，如果陈旧，则移动窗口以确保数据新鲜。
// 随后，它会创建一个新的针对所需百分位数优化的合并目标（merged），并将当前所有的样本合并到这个目标中。
// 最后，函数返回这个合并后的流，供进一步的分析或使用。
func (q *Quantile) QueryHandler() *quantile.Stream {
	// 加锁以确保线程安全，因为在查询处理过程中会操作共享资源。
	q.Lock()
	// 获取当前时间，用于判断数据是否陈旧。
	now := time.Now()
	// 如果数据陈旧，移动窗口以丢弃旧数据并准备接收新数据。
	for q.IsDataStale(now) {
		q.moveWindow()
	}

	// 创建一个新的合并目标，针对所需的百分位数进行优化。
	merged := quantile.NewTargeted(q.Percentiles...)
	// 将两个流中的样本合并到合并目标中。
	merged.Merge(q.streams[0].Samples())
	merged.Merge(q.streams[1].Samples())
	// 解锁以释放锁，允许其他操作进行。
	q.Unlock()
	// 返回合并后的流，供进一步使用。
	return merged
}

// Merge 合并两个 Quantile 对象中的数据。
// 此方法确保两个 Quantile 对象的数据在合并过程中的一致性。
// 参数:
// - q: 调用合并的 Quantile 实例。
// - them: 被合并到 q 的 Quantile 实例。
func (q *Quantile) Merge(them *Quantile) {
	// 加锁以确保线程安全。
	q.Lock()
	them.Lock()

	// 索引表示当前活动的流。
	iUs := q.currentIndex
	iThem := them.currentIndex

	// 合并当前活动流中的样本。
	q.streams[iUs].Merge(them.streams[iThem].Samples())

	// 切换到另一个流的索引。
	iUs ^= 0x1
	iThem ^= 0x1

	// 合并非活动流中的样本。
	q.streams[iUs].Merge(them.streams[iThem].Samples())

	// 确保 lastMoveWindow 反映两个 Quantile 中的最新移动窗口。
	if q.lastMoveWindow.Before(them.lastMoveWindow) {
		q.lastMoveWindow = them.lastMoveWindow
	}

	// 解锁以释放线程。
	q.Unlock()
	them.Unlock()
}
