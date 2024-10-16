package quantile

import (
	"encoding/json"
	"math"
	"sort"
)

// E2eProcessingLatencyAggregate 结构体用于存储端到端处理延迟的聚合信息。
// 它包括统计的计数、百分位数、主题、频道和地址信息。
type E2eProcessingLatencyAggregate struct {
	Count       int                  `json:"count"`       // 统计事件的数量
	Percentiles []map[string]float64 `json:"percentiles"` // 各个延迟的百分位数信息
	Topic       string               `json:"topic"`       // 相关的主题名称
	Channel     string               `json:"channel"`     // 相关的频道名称
	Addr        string               `json:"host"`        // 相关的地址信息
}

// UnmarshalJSON 自定义JSON解码方法，用于处理Percentiles字段的特殊逻辑。
// 它接收JSON数据作为字节数组，填充结构体，并为每个百分位数添加额外的统计信息。
func (e *E2eProcessingLatencyAggregate) UnmarshalJSON(b []byte) error {
	// 定义一个临时结构体用于解码JSON数据
	var resp struct {
		Count       int                  `json:"count"`
		Percentiles []map[string]float64 `json:"percentiles"`
		Topic       string               `json:"topic"`
		Channel     string               `json:"channel"`
		Addr        string               `json:"host"`
	}
	// 使用json.Unmarshal解码字节数组到临时结构体
	err := json.Unmarshal(b, &resp)
	if err != nil {
		return err
	}

	// 对每个百分位数添加默认的min, max, average和count值
	for _, p := range resp.Percentiles {
		p["min"] = p["value"]
		p["max"] = p["value"]
		p["average"] = p["value"]
		p["count"] = float64(resp.Count)
	}

	// 将解码的数据复制到E2eProcessingLatencyAggregate实例
	e.Count = resp.Count
	e.Percentiles = resp.Percentiles
	e.Topic = resp.Topic
	e.Channel = resp.Channel
	e.Addr = resp.Addr

	return nil
}

// Len 返回百分位数列表的长度。
func (e *E2eProcessingLatencyAggregate) Len() int { return len(e.Percentiles) }

// Swap 交换百分位数列表中两个元素的位置。
func (e *E2eProcessingLatencyAggregate) Swap(i, j int) {
	e.Percentiles[i], e.Percentiles[j] = e.Percentiles[j], e.Percentiles[i]
}

// Less 比较百分位数列表中的两个元素，以确定它们的顺序。
func (e *E2eProcessingLatencyAggregate) Less(i, j int) bool {
	return e.Percentiles[i]["percentile"] > e.Percentiles[j]["percentile"]
}

// Add 将另一个E2eProcessingLatencyAggregate实例的数据合并到当前实例中。
// 这包括更新计数和根据需要调整百分位数信息。
func (e *E2eProcessingLatencyAggregate) Add(e2 *E2eProcessingLatencyAggregate) {
	e.Addr = "*"
	p := e.Percentiles
	e.Count += e2.Count
	for _, value := range e2.Percentiles {
		i := -1
		for j, v := range p {
			if value["quantile"] == v["quantile"] {
				i = j
				break
			}
		}
		if i == -1 {
			i = len(p)
			e.Percentiles = append(p, make(map[string]float64))
			p = e.Percentiles
			p[i]["quantile"] = value["quantile"]
		}
		p[i]["max"] = math.Max(value["max"], p[i]["max"])
		p[i]["min"] = math.Min(value["min"], p[i]["min"])
		p[i]["count"] += value["count"]
		if p[i]["count"] == 0 {
			p[i]["average"] = 0
			continue
		}
		delta := value["average"] - p[i]["average"]
		R := delta * value["count"] / p[i]["count"]
		p[i]["average"] = p[i]["average"] + R
	}
	sort.Sort(e)
}
