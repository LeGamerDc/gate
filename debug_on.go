//go:build gatedebug

package gate

import "sync/atomic"

// gatedebug 构建：把分级池的借还配平变成可断言的数。
// 只在测试构建里打开，见 debug_off.go 的说明。
const debugAccounting = true

var poolBorrowed atomic.Int64

func trackPoolGet(n int) { poolBorrowed.Add(1) }
func trackPoolPut(n int) { poolBorrowed.Add(-1) }

// poolOutstanding 返回「借出但未归还」的块数。用例结束时它必须回到用例
// 开始时的值——不等于 0，因为进程里还有别的常驻缓冲。
func poolOutstanding() int64 { return poolBorrowed.Load() }
