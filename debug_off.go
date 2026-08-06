//go:build !gatedebug

package gate

// 资源配平的计数点（07 第 3 层的 afterEach 要求「每个池的借出计数 == 归还
// 计数、chunk slab 全部归还」）。默认编译成空函数——它们在热路径上，
// 每次 poolGet/poolPut 加一次原子操作会污染 0 allocs/op 之外的性能判据。
//
// CI 会额外跑一遍 `-tags gatedebug`，那时 harness 的 verifyConservation
// 才真正断言这些数。
const debugAccounting = false

func trackPoolGet(int) {}
func trackPoolPut(int) {}

func poolOutstanding() int64 { return 0 }
