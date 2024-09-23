package util

import "math/rand"

// UniqRands 生成指定数量的不重复随机数。
// 参数:
//
//	quantity - 需要生成的随机数的数量。
//	maxval - 随机数的上限。
//
// 返回值:
//
//	一个包含不重复随机数的切片。
func UniqRands(quantity int, maxval int) []int {
	// 如果上限小于所需数量，则调整数量为上限。
	if maxval < quantity {
		quantity = maxval
	}

	// 初始化一个长度为上限的整数切片。
	intSlice := make([]int, maxval)
	for i := 0; i < maxval; i++ {
		intSlice[i] = i
	}

	// 通过洗牌算法生成不重复的随机数。
	for i := 0; i < quantity; i++ {
		j := rand.Int()%maxval + i
		// 交换两个位置的元素。
		intSlice[i], intSlice[j] = intSlice[j], intSlice[i]
		maxval--

	}
	// 返回切片中前 quantity 个元素作为结果。
	return intSlice[0:quantity]
}
