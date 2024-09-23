package stringy

// Add 函数将字符串 a 添加到字符串切片 s 中，如果 a 已存在于 s 中，则直接返回 s。
// 该函数用于确保切片中不包含重复的元素。
// 参数:
//
//	s []string: 原始字符串切片。
//	a string: 需要添加的字符串。
//
// 返回值:
//
//	[]string: 更新后的字符串切片。
func Add(s []string, a string) []string {
	for _, existing := range s {
		if a == existing {
			return s
		}
	}
	return append(s, a)

}

// Union 函数计算两个字符串切片的并集。
// 将切片 a 中的元素添加到切片 s 中，但不包含重复元素。
// 参数:
//
//	s []string: 原始字符串切片。
//	a []string: 需要合并的字符串切片。
//
// 返回值:
//
//	[]string: 两个切片的并集。
func Union(s []string, a []string) []string {
	for _, entry := range a {
		found := false
		for _, existing := range s {
			if entry == existing {
				found = true
				break
			}
		}
		if !found {
			s = append(s, entry)
		}
	}
	return s
}

// Uniq 函数从字符串切片 s 中去除重复元素。
// 该函数遍历 s，将唯一元素添加到结果切片 r 中。
// 参数:
//
//	s []string: 需要去重的字符串切片。
//
// 返回值:
//
//	[]string: 去重后的字符串切片。
func Uniq(s []string) (r []string) {
outerLoop:
	for _, entry := range s {
		for _, existing := range r {
			if existing == entry {
				continue outerLoop
			}
		}
		r = append(r, entry)
	}
	return
}
