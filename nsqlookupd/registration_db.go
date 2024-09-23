package nsqlookupd

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// RegistrationDB 是一个注册表，用于存储生产者的注册信息。
type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]ProducerMap
}

// Registration 表示一个注册信息，包括类别、键和子键。
type Registration struct {
	Category string
	Key      string
	SubKey   string
}

// Registrations 表示一个注册信息的切片。
type Registrations []Registration

// PeerInfo 表示生产者的信息，包括远程地址、主机名、广播地址、TCP端口和HTTP端口。
type PeerInfo struct {
	lastUpdate       int64
	id               string
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

// Producer 表示一个生产者，包括其信息、是否被标记为墓碑状态以及墓碑状态的创建时间。
type Producer struct {
	peerInfo     *PeerInfo
	tombstoned   bool
	tombstonedAt time.Time
}

type Producers []*Producer
type ProducerMap map[string]*Producer

func (p *Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]", p.peerInfo.BroadcastAddress, p.peerInfo.TCPPort, p.peerInfo.HTTPPort)
}

// Tombstone 将生产者标记为墓碑状态。
// 设置 tombstoned 为 true 表示该生产者已被逻辑删除。
// tombstonedAt 记录生产者被标记为墓碑状态的时间。
func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

// IsTombstoned 判断生产者实例是否被标记为永久删除，并且距离被标记的时间不超过指定的生命周期。
//
// 参数:
//
//	lifetime - 需要判断的生命周期时长。
//
// 返回值:
//
//	如果生产者实例被标记为永久删除，并且距离被标记的时间不超过指定的生命周期时长，则返回true；否则返回false。
//
// 说明:
//
//	该方法用于确定生产者实例是否应该被视为永久删除状态。如果生产者实例已经被标记为永久删除
//	(tombstoned为true)，并且从被标记为永久删除至今的时间小于或等于给定的生命周期时长，则认为
//	该生产者实例处于永久删除的状态下。
func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Since(p.tombstonedAt) < lifetime
}

// NewRegistrationDB 创建并返回一个新的 RegistrationDB 实例。
// 该函数初始化一个空的 registrationMap，用于存储 Registration 到 ProducerMap 的映射。
// 返回值是一个指向初始化后的 RegistrationDB 实例的指针。
func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]ProducerMap),
	}
}

// AddRegistration 向注册表中添加注册信息。
// 该方法首先尝试获取注册表的锁，确保并发安全，然后检查给定的注册信息k是否已存在于注册表中。
// 如果不存在，该方法会在注册表中创建与该注册信息关联的生产者映射。
// 参数:
//
//	k - 要添加到注册表的注册信息。
//
// 返回值:
//
//	该方法没有显式返回值，但它根据注册信息的存在与否决定是否初始化一个生产者映射。
func (r *RegistrationDB) AddRegistration(k Registration) {
	// 获取锁以确保操作的并发安全。
	r.Lock()
	defer r.Unlock() // 延迟释放锁以确保它在方法退出前释放。

	// 检查注册信息是否已存在于注册表中。
	_, ok := r.registrationMap[k]
	if !ok {
		// 如果注册信息不存在，为该注册信息初始化生产者映射。
		r.registrationMap[k] = make(map[string]*Producer)
	}
}

// AddProducer 将一个 Producer 实例添加到 RegistrationDB 实例中。
// 如果添加成功，返回 true；否则，返回 false。
// 参数:
// - k: Registration 类型的键，用于标识注册项。
// - p: 指向 Producer 实例的指针。
// 返回值:
// - bool: 表示是否成功添加 Producer 实例。
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	// 加锁以确保线程安全。
	r.Lock()
	defer r.Unlock() // 延迟解锁以确保函数安全执行完毕。

	// 检查给定键是否已存在于注册表中。
	_, ok := r.registrationMap[k]
	// 如果不存在，为该键创建一个新的 Producer 映射。
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}

	// 获取与键关联的 Producer 映射。
	producers := r.registrationMap[k]
	// 检查给定 Producer 是否已存在于映射中。
	_, found := producers[p.peerInfo.id]
	// 如果不存在，将 Producer 添加到映射中。
	if !found {
		producers[p.peerInfo.id] = p
	}

	// 返回一个布尔值，表示是否成功添加 Producer。
	return !found
}

// RemoveProducer 从 RegistrationDB 中移除指定的生产者。
// 该函数接收一个 Registration 对象 k 和一个生产者 ID id。
// 如果成功移除生产者，返回 true 和剩余生产者的数量；
// 如果没有找到指定的生产者或出现其他错误，返回 false 和剩余生产者的数量。
func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	// 加锁以确保线程安全
	r.Lock()
	defer r.Unlock()

	// 检查给定的 Registration 对象 k 是否存在于注册表中
	producers, ok := r.registrationMap[k]
	if !ok {
		// 如果不存在，返回 false 和 0 作为剩余生产者数量
		return false, 0
	}

	// 初始化 removed 变量为 false，待确定是否成功移除生产者
	removed := false

	// 检查指定的生产者 ID id 是否存在于 k 对应的生产者列表中
	if _, exists := producers[id]; exists {
		// 如果存在，设置 removed 为 true
		removed = true
	}

	// 从生产者列表中移除指定的生产者 ID id
	// 注意：即使列表变为空，也会保留空列表在注册表中
	delete(producers, id)

	// 返回是否成功移除生产者和移除后的剩余生产者数量
	return removed, len(producers)
}

// RemoveRegistration 从注册表中移除一个注册信息。
//
// 此方法通过注册信息的键来删除对应的注册信息。它首先获取注册表的锁，以确保
// 并发安全性，然后从注册表映射中删除对应的键值对。
//
// 参数:
//
//	k Registration - 用于标识注册信息的键。
//
// 返回值:
//
//	无。
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	// 加锁以确保并发访问的安全性。
	r.Lock()
	defer r.Unlock() // 延迟解锁以确保函数退出时解锁。

	// 从注册表映射中删除指定的注册信息。
	delete(r.registrationMap, k)
}

func (r *RegistrationDB) needFilter(key string, subkey string) bool {
	return key == "*" || subkey == "*"
}

// FindRegistrations 根据类别、键和子键查找注册信息。
// 这个方法首先检查是否需要根据给定的key和subkey进行筛选。
// 如果不需要，它会尝试在注册映射中直接查找匹配的注册信息。
// 如果找到了匹配的注册信息，它将返回包含该注册信息的 Registrations 切片。
// 如果没有找到匹配的注册信息，它将返回一个空的 Registrations 切片。
// 如果需要进行筛选，则遍历注册映射，对每个键进行匹配检查，并将匹配的注册信息添加到结果中。
// 参数：
//
//	category - 注册信息的类别。
//	key - 注册信息的键。
//	subkey - 注册信息的子键。
//
// 返回值：
//
//	Registrations - 匹配给定条件的注册信息切片。
func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	// 加读锁，并在函数执行完毕后自动解锁。
	r.RLock()
	defer r.RUnlock()

	// 检查是否需要根据给定的key和subkey进行筛选。
	if !r.needFilter(key, subkey) {
		// 创建一个Registration实例，用于直接查找。
		k := Registration{category, key, subkey}
		// 检查注册映射中是否存在直接匹配的注册信息。
		if _, ok := r.registrationMap[k]; ok {
			// 如果找到了匹配的注册信息，返回包含该注册信息的 Registrations 切片。
			return Registrations{k}
		}
		// 如果没有找到匹配的注册信息，返回一个空的 Registrations 切片。
		return Registrations{}
	}

	// 如果需要进行筛选，初始化一个空的 Registrations 切片用于存储结果。
	results := Registrations{}
	// 遍历注册映射中的每个键。
	for k := range r.registrationMap {
		// 检查当前键是否匹配给定的类别、键和子键。
		if !k.IsMatch(category, key, subkey) {
			// 如果不匹配，继续检查下一个键。
			continue
		}
		// 将匹配的注册信息添加到结果中。
		results = append(results, k)
	}
	// 返回筛选后的结果。
	return results
}

// FindProducers 根据给定的类别、键和子键查找相关的生产者。
// 如果不需要过滤，则直接返回与给定条件完全匹配的生产者列表。
// 如果需要根据条件进行过滤，则遍历所有注册的生产者，找到部分条件匹配的生产者并返回。
func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	// 加读锁，保护registrationMap
	r.RLock()
	defer r.RUnlock()

	// 检查是否需要根据条件过滤生产者
	if !r.needFilter(key, subkey) {
		// 不需要过滤，直接返回完全匹配的生产者列表
		k := Registration{category, key, subkey}
		return ProducerMap2Slice(r.registrationMap[k])
	}

	// 创建一个临时map来存储找到的生产者ID，避免重复添加
	results := make(map[string]struct{})
	var retProducers Producers

	// 遍历registrationMap中的所有注册信息
	for k, producers := range r.registrationMap {
		// 检查当前注册信息是否部分匹配给定的查找条件
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		// 遍历部分匹配的注册信息中的所有生产者
		for _, producer := range producers {
			// 检查当前生产者是否已经被添加到结果中
			_, found := results[producer.peerInfo.id]
			if !found {
				// 如果当前生产者是新的，则添加到结果map和列表中
				results[producer.peerInfo.id] = struct{}{}
				retProducers = append(retProducers, producer)
			}
		}
	}
	// 返回找到的生产者列表
	return retProducers
}

// LookupRegistrations 通过ID查找注册项。
//
// id 参数是用于查找注册项的唯一标识符。
// 返回值 Registrations 是一个注册项的切片，包含所有与给定ID关联的注册项。
func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	// 加读锁，保护注册表数据的并发访问。
	r.RLock()
	defer r.RUnlock()

	// 初始化结果切片，用于存储找到的注册项。
	results := Registrations{}

	// 遍历注册映射，寻找与给定ID关联的注册项。
	for k, producers := range r.registrationMap {
		// 检查当前生产者映射中是否存在给定ID的注册项。
		if _, exists := producers[id]; exists {
			// 如果存在，将注册项添加到结果切片中。
			results = append(results, k)
		}
	}

	// 返回包含所有找到的注册项的切片。
	return results
}

// IsMatch 检查当前注册信息是否与给定的类别、键和子键匹配。
// 此函数用于确定注册信息是否适用于特定的类别、键和子键组合。
// 参数：
// - category: 待匹配的类别字符串。"*"表示通配符，可匹配任何类别。
// - key: 待匹配的键字符串。"*"表示通配符，可匹配任何键。
// - subkey: 待匹配的子键字符串。"*"表示通配符，可匹配任何子键。
// 返回值：
// - bool: 如果注册信息匹配给定的类别、键和子键，则返回true；否则返回false。
func (k Registration) IsMatch(category string, key string, subkey string) bool {
	// 检查类别是否匹配
	if category != k.Category {
		return false
	}
	// 检查键是否匹配，如果key不是通配符且与注册信息中的Key不匹配，则返回false
	if key != "*" && k.Key != key {
		return false
	}
	// 检查子键是否匹配，如果subkey不是通配符且与注册信息中的SubKey不匹配，则返回false
	if subkey != "*" && k.SubKey != subkey {
		return false
	}
	// 如果类别、键和子键都匹配，则返回true
	return true
}

func (rr Registrations) Filter(category string, key string, subkey string) Registrations {
	output := Registrations{}
	for _, k := range rr {
		if k.IsMatch(category, key, subkey) {
			output = append(output, k)
		}
	}
	return output
}

func (rr Registrations) Keys() []string {
	keys := make([]string, len(rr))
	for i, k := range rr {
		keys[i] = k.Key
	}
	return keys
}

func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
}

// FilterByActive 筛选掉不活跃的 Producer
// 该函数通过检查每个 Producer 的最后活跃时间以及是否被标记为 tombstone 来过滤出活跃的 Producer。
// 参数 inactivityTimeout 表示 Producer 被认为不活跃之前允许的最长不活跃时间。
// 参数 tombstoneLifetime 表示 Producer 被标记为 tombstone 后的生命周期。
// 返回值是一个筛选后的 Producer 切片，仅包含活跃的 Producer。
func (pp Producers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) Producers {
	// 获取当前时间，用于后续判断 Producer 是否活跃
	now := time.Now()
	// 初始化结果切片，用于存放活跃的 Producer
	results := Producers{}
	// 遍历所有 Producer，判断其是否活跃
	for _, p := range pp {
		// 获取 Producer 最后一次更新的时间
		cur := time.Unix(0, atomic.LoadInt64(&p.peerInfo.lastUpdate))
		// 如果 Producer 的最后更新时间超过 inactivityTimeout，或者 Producer 被标记为 tombstone 且超过生命周期时间，则忽略该 Producer
		if now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
			continue
		}
		// 将活跃的 Producer 添加到结果切片中
		results = append(results, p)
	}
	// 返回筛选后的活跃 Producer 切片
	return results
}

func (pp Producers) PeerInfo() []*PeerInfo {
	results := []*PeerInfo{}
	for _, p := range pp {
		results = append(results, p.peerInfo)
	}
	return results
}

// ProducerMap2Slice 将 ProducerMap 类型的参数转换为 Producers 类型的切片。
// 该函数遍历输入的 ProducerMap，并将每个键值对的值追加到一个新的 Producers 切片中。
// 参数:
//
//	pm - 需要转换的 ProducerMap。
//
// 返回值:
//
//	转换后的 Producers 切片。
func ProducerMap2Slice(pm ProducerMap) Producers {
	// 初始化一个空的 Producers 切片，用于存储转换后的结果。
	var producers Producers
	// 遍历 ProducerMap，将每个 producer 对象追加到切片中。
	for _, producer := range pm {
		producers = append(producers, producer)
	}

	// 返回转换后的 Producers 切片。
	return producers
}
