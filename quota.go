package gate

import "sync/atomic"

// 全局出站预算（06「全局预算是近似的」）。
//
// 精确的全局计数需要一条被所有发送核心争抢的 cache line，这与 shared-nothing
// 直接冲突。所以采用**配额租约**：每个 loop 从 server 预算里租一段，用完再申请、
// 富余时归还。这是一个**刻意的近似**——瞬时总量可能超出 MaxOutboundBytes
// 一个租约粒度（每 loop 至多 leaseChunk 字节的富余）。
//
// 稳态下热路径只碰本 loop 的那个原子量，无跨核往返；只有租约耗尽或富余越过
// 两倍粒度时才去碰 server 的计数器。
//
// 租约粒度按总预算自适应：固定粒度在小预算下会让**一个** loop 把整份预算
// 租走、其余 loop 一个字节都拿不到——那不是近似，那是死锁。
const (
	maxLeaseChunk = 256 << 10
	minLeaseChunk = 4 << 10
)

type serverBudget struct {
	total     int64
	chunk     int64
	remaining atomic.Int64 // 尚未被任何 loop 租走的字节；total <= 0 表示不限
}

func newServerBudget(total int) *serverBudget {
	b := &serverBudget{total: int64(total)}
	if total > 0 {
		b.remaining.Store(int64(total))
		b.chunk = min(max(int64(total)/8, minLeaseChunk), maxLeaseChunk)
	}
	return b
}

func (b *serverBudget) unlimited() bool { return b == nil || b.total <= 0 }

// take 从 server 预算里取 n 字节；不足则不取，返回 false。
func (b *serverBudget) take(n int64) bool {
	for {
		cur := b.remaining.Load()
		if cur < n {
			return false
		}
		if b.remaining.CompareAndSwap(cur, cur-n) {
			return true
		}
	}
}

func (b *serverBudget) give(n int64) { b.remaining.Add(n) }

// quotaLease 是单个 loop 持有的租约。acquire/release 可能来自任意 goroutine
// （Send 不限线程），所以是原子的。
//
// outstanding 记「当前真正被账面占着的字节」：它归零时把整段租约还回 server。
// 没有这一步，小预算下每个 loop 都会永久扣住一段——8 个 loop 各发一个字节
// 再全部写完，server 侧就再也租不出任何东西了，而此刻真实积压是 0。
type quotaLease struct {
	srv         *serverBudget
	avail       atomic.Int64
	outstanding atomic.Int64
}

func newQuotaLease(srv *serverBudget) *quotaLease { return &quotaLease{srv: srv} }

// acquire 扣减 n 字节。返回 false 表示全局预算已耗尽——调用方按
// ErrSendQueueFull 处理（消息从未入队，帧流没有洞）。
func (q *quotaLease) acquire(n int) bool {
	if q == nil || q.srv.unlimited() {
		return true
	}
	for {
		cur := q.avail.Load()
		if cur >= int64(n) {
			if q.avail.CompareAndSwap(cur, cur-int64(n)) {
				q.outstanding.Add(int64(n))
				return true
			}
			continue
		}
		// 本 loop 的租约不够：一次多租一段，摊薄对 server 计数器的争用。
		want := max(q.srv.chunk, int64(n))
		if !q.srv.take(want) {
			// 整段租不到时只补差额：全局余量少于一段粒度时仍应把它用满，
			// 否则最后一段预算永远租不出去（总量小于 leaseChunk 时尤其明显）。
			deficit := int64(n) - cur
			if deficit <= 0 || !q.srv.take(deficit) {
				return false
			}
			want = deficit
		}
		q.avail.Add(want)
	}
}

// borrow 是**不可拒绝**的扣减：收尾帧（close）与 SendFunc commit 时因
// cipher epoch 变化产生的差额都已经过了准入，不能再退回去。它允许把租约
// 压成负数——那正是 06 说的「近似」：超额有界（每 loop 至多一个收尾帧 +
// 一个 Overhead 差额），而账面必须与 release 对称，否则 release 会凭空
// 造出额度，之后就能突破 MaxOutboundBytes。
func (q *quotaLease) borrow(n int) {
	if q == nil || q.srv.unlimited() || n == 0 {
		return
	}
	q.avail.Add(int64(-n))
	q.outstanding.Add(int64(n))
}

// release 归还 n 字节。占用归零时把整段租约还回 server；否则只在富余越过
// 两倍粒度时还一部分——前者防沉淀，后者摊薄对 server 计数器的争用。
func (q *quotaLease) release(n int) {
	if q == nil || q.srv.unlimited() || n == 0 {
		return
	}
	cur := q.avail.Add(int64(n))
	if q.outstanding.Add(int64(-n)) <= 0 {
		// 本 loop 已经没有在途占用：整段还回去，别让它饿死别的 loop。
		if cur > 0 && q.avail.CompareAndSwap(cur, 0) {
			q.srv.give(cur)
		}
		return
	}
	chunk := q.srv.chunk
	for cur > 2*chunk {
		back := cur - chunk
		if q.avail.CompareAndSwap(cur, cur-back) {
			q.srv.give(back)
			return
		}
		cur = q.avail.Load()
	}
}
