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

func (l *lruList) pushBack(n *lruNode, when int64) {
	n.when = when
	n.prev = l.root.prev
	n.next = &l.root
	l.root.prev.next = n
	l.root.prev = n
}

// remove 幂等：不在链上（prev==nil）直接返回。
func (l *lruList) remove(n *lruNode) {
	if n.prev == nil {
		return
	}
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev, n.next = nil, nil
}

func (n *lruNode) inList() bool { return n.prev != nil }

// touch 移到尾部并更新时刻（idle 交付消息 / stall 写出字节）。
func (l *lruList) touch(n *lruNode, when int64) {
	l.remove(n)
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
		l.remove(n)
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
