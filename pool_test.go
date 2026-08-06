package gate

import "testing"

func TestPoolClassBoundaries(t *testing.T) {
	cases := []struct {
		n, class int
	}{
		{0, 0}, {1, 0}, {64, 0}, {65, 1}, {128, 1}, {129, 2},
		{1 << 26, poolClasses - 1}, {1<<26 + 1, -1},
	}
	for _, tc := range cases {
		if got := poolClass(tc.n); got != tc.class {
			t.Fatalf("poolClass(%d) = %d, want %d", tc.n, got, tc.class)
		}
	}
}

func TestPoolGetPut(t *testing.T) {
	b := poolGet(100)
	if len(b) != 100 || cap(b) != 128 {
		t.Fatalf("len=%d cap=%d", len(b), cap(b))
	}
	for i := range b {
		b[i] = 0xAA
	}
	poolPut(b)

	// 超过最大类：直接 make，len == n，Put 静默丢弃。
	huge := poolGet(1<<26 + 1)
	if len(huge) != 1<<26+1 {
		t.Fatal(len(huge))
	}
	poolPut(huge)

	// append 换过底层数组（cap 非 2 的幂）时丢弃，不放坏池子。
	odd := make([]byte, 100)
	poolPut(odd)
}

// Get/Put 本身不得分配（池里存 *byte，无装箱）。
// 注：sync.Pool 的 per-P 缓存在 GC 后清空，偶发的一次重建分配是允许的，
// 所以这里先 warm 后测，并允许极小的非零值由 -count 多次运行暴露趋势。
func TestPoolNoAllocSteadyState(t *testing.T) {
	for range 16 {
		poolPut(poolGet(1024))
	}
	allocs := testing.AllocsPerRun(1000, func() {
		b := poolGet(1024)
		poolPut(b)
	})
	if allocs > 0.1 {
		t.Fatalf("稳态 Get/Put 分配 %v 次/op", allocs)
	}
}
