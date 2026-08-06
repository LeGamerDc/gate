package gate

import (
	"math/bits"
	"sync"
	"unsafe"
)

// 全局并发安全分级池（06 字节所有权表里的「分级池」）。
//
// 类容量为 2 的幂：64B ~ 64MB。上限取 64MB 是因为最大池化对象是
// maxMessageSize(32MB) 的帧体加上 AEAD overhead，仍落在最大类内。
// 超过最大类的请求直接 make，归还时丢弃——那不是稳态路径。
//
// 池里存的是 *byte（底层数组首地址），Get/Put 都不产生装箱分配；
// 长度语义由取用方决定，poolGet 返回 len == n、cap == 类容量的切片。
const (
	poolMinBits = 6  // 64B
	poolMaxBits = 26 // 64MB
	poolClasses = poolMaxBits - poolMinBits + 1
)

var pools [poolClasses]sync.Pool

// poolClass 返回容纳 n 字节的最小类下标；n 超过最大类返回 -1。
func poolClass(n int) int {
	if n <= 1<<poolMinBits {
		return 0
	}
	b := bits.Len(uint(n - 1)) // 最小的 k 使 2^k >= n
	if b > poolMaxBits {
		return -1
	}
	return b - poolMinBits
}

// poolGet 借出一块 len == n 的缓冲，cap 是所属类的容量。
// 内容未清零——调用方按「先写后读」使用。
func poolGet(n int) []byte {
	c := poolClass(n)
	if c < 0 {
		return make([]byte, n)
	}
	size := 1 << (poolMinBits + c)
	if p, _ := pools[c].Get().(*byte); p != nil {
		return unsafe.Slice(p, size)[:n]
	}
	return make([]byte, size)[:n]
}

// poolPut 归还 poolGet 借出的缓冲。cap 不是任何类的精确容量时丢弃
// （来自超限 make 的大块，或调用方 append 换过底层数组）。
func poolPut(b []byte) {
	c := cap(b)
	if c < 1<<poolMinBits || c&(c-1) != 0 {
		return
	}
	k := bits.TrailingZeros(uint(c)) - poolMinBits
	if k >= poolClasses {
		return
	}
	pools[k].Put(unsafe.SliceData(b[:1]))
}
