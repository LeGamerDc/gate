package gate

import "time"

// poller 是 epoll / kqueue 的窄抽象（04）。窄是有意的：它必须窄到 epoll、
// kqueue 和（将来的）io_uring 都能实现，否则换底座又是一次破坏性改动。
//
// 所有方法（notify 除外）只能在拥有它的 loop 线程上调用（R2）。
type poller interface {
	// add 注册一个 fd，tok 会随事件原样返回。
	add(fd int, tok token, want interest) error
	// mod 修改兴趣集合（Pause / 出站积压变化时用）。
	mod(fd int, tok token, want interest) error
	del(fd int) error

	// wait 阻塞到有事件或超时。timeout < 0 表示无限等待。
	// 被信号打断（EINTR）返回 (0, nil)，调用方按超时归零处理再进下一轮。
	wait(out []event, timeout time.Duration) (int, error)

	// notify 让阻塞中的 wait 立刻返回。可从任意 goroutine 调用，幂等——
	// 连续多次 notify 只保证 wait 至少醒一次。
	notify() error

	close() error
}

type interest uint8

const (
	interestRead  interest = 1 << 0
	interestWrite interest = 1 << 1
)

type event struct {
	tok   token
	read  bool
	write bool
	// hup 表示对端关闭或出错。仍然要走一次 read 拿到确切原因
	// （EOF 还是具体的 errno），所以它只是一个提示。
	hup bool
}
