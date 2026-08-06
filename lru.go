package gate

// 侵入式 LRU 链（06「截止时刻」）：O(1) 入链/摘除/移尾，O(k) 过期扫描。
// 排序键 when 在单个 loop 内单调递增（后发生的事件时刻不早于先发生的），
// 所以从头扫到第一个未超时的就能停——这是它能替代时间堆的前提。
// 只在 loop 线程上触碰。
type lruNode struct {
	prev, next *lruNode
	when       int64 // 纳秒时刻（loop 注入的时钟）
	c          *connCore
}

type lruList struct {
	root lruNode // 哨兵，环形
}

func (l *lruList) init() {
	l.root.prev, l.root.next = &l.root, &l.root
}

func (l *lruList) empty() bool { return l.root.next == &l.root }

// front 返回最老的节点；空链返回 nil。
func (l *lruList) front() *lruNode {
	if l.empty() {
		return nil
	}
	return l.root.next
}

// pushBack 先摘再挂：同一个侵入式节点同时属于两条链会直接覆盖指针，
// 把两条链一起毁掉。unlink 摘的是节点**当前所在的那条链**，跨链也管用，
// 所以这一行是结构性的防御而不只是清理。
func (l *lruList) pushBack(n *lruNode, when int64) {
	n.unlink()
	n.when = when
	n.prev = l.root.prev
	n.next = &l.root
	l.root.prev.next = n
	l.root.prev = n
}

// unlink 把节点从它**当前所在**的链上摘下来；不在任何链上时是 no-op。
//
// 它是节点的方法而不是链的方法，因为它本来就不需要知道是哪条链——环形双链
// 的摘除只用到节点自己的两个指针。写成 `someList.remove(n)` 会给出一个假的
// 承诺：读起来像「从 someList 摘除」，实际是「从 n 所在的任何链摘除」，
// 传错链一样生效且静默。下一次加一条超时链时，那个假承诺就是 bug 的入口。
//
// 跨链互斥另有保证：读闸的归属与 ConnsPaused 计数由 enterPaused/exitPaused
// 成对维护，unlink 不碰计数。
func (n *lruNode) unlink() {
	if n.prev == nil {
		return
	}
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev, n.next = nil, nil
}

func (n *lruNode) inList() bool { return n.prev != nil }

// touch 移到尾部并更新时刻（idle 交付消息 / stall 写出字节）。
// pushBack 自己先摘再挂，这里不必再 remove 一次。
func (l *lruList) touch(n *lruNode, when int64) {
	l.pushBack(n, when)
}

// expire 弹出所有 when+timeout <= now 的节点并回调。timeout <= 0 表示该链禁用。
// fn 里可以安全地把连接从任何链上摘除（先出链再回调）。
func (l *lruList) expire(timeout, now int64, fn func(*connCore)) {
	if timeout <= 0 {
		return
	}
	for {
		n := l.front()
		if n == nil || n.when+timeout > now {
			return
		}
		n.unlink()
		fn(n.c)
	}
}

// deadline 返回链头的截止时刻；空链或禁用返回 0。
func (l *lruList) deadline(timeout int64) int64 {
	if timeout <= 0 {
		return 0
	}
	n := l.front()
	if n == nil {
		return 0
	}
	return n.when + timeout
}
