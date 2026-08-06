package gate

import "sync/atomic"

// 跨 goroutine 投递的两条通道（04「跨 goroutine 投递」）：
//
//   - dirtyList：侵入式 MPSC，「这条连接脏了」。节点内嵌在 outbound 里，
//     armed 标志保证单一成员资格（O14），push 侧无分配。
//   - inbox：闭包 MPSC，承载 Post、resume 控制项、macOS 的 accept 移交。
//     闭包本身要逃逸，节点分配不可避免，也不在热路径上。
//
// 两者都是 Treiber 栈 + drain 时整体反转（保持 FIFO），push 一次 CAS。

type dirtyList struct {
	head atomic.Pointer[outbound]
}

// push 把 o 挂进链。调用方持有 o.mu 且刚完成 armed false→true 的置位——
// 单一成员资格由 armed 保证，dirtyNext 因此不会被并发改写。
func (l *dirtyList) push(o *outbound) {
	for {
		old := l.head.Load()
		o.dirtyNext = old
		if l.head.CompareAndSwap(old, o) {
			return
		}
	}
}

// drain 一次性摘走整条链，按 push 顺序（FIFO）返回。只在 loop 线程调用。
func (l *dirtyList) drain() *outbound {
	n := l.head.Swap(nil)
	var rev *outbound
	for n != nil {
		next := n.dirtyNext
		n.dirtyNext = rev
		rev = n
		n = next
	}
	return rev
}

type inboxNode struct {
	fn   func()
	next *inboxNode
}

type inbox struct {
	head atomic.Pointer[inboxNode]
}

func (b *inbox) push(fn func()) {
	n := &inboxNode{fn: fn}
	for {
		old := b.head.Load()
		n.next = old
		if b.head.CompareAndSwap(old, n) {
			return
		}
	}
}

// drain 摘走整条链，按 push 顺序返回。只在 loop 线程调用。
func (b *inbox) drain() *inboxNode {
	n := b.head.Swap(nil)
	var rev *inboxNode
	for n != nil {
		next := n.next
		n.next = rev
		rev = n
		n = next
	}
	return rev
}

// waker 把「N 次投递合并成一次 notify 系统调用」：只有空→非空的第一个生产者
// 真正执行 notify（R7）。消费侧必须先 disarm 再 drain——反过来会丢事件：
// drain 完到清标志之间 push 的项目，其生产者看到 armed==true 跳过 notify。
type waker struct {
	armed atomic.Bool
	p     poller
}

func (w *waker) maybeNotify() {
	if w.armed.CompareAndSwap(false, true) {
		_ = w.p.notify()
	}
}

// disarm 在 loop 收到 notify 事件后、drain 之前调用。
func (w *waker) disarm() {
	w.armed.Store(false)
}
