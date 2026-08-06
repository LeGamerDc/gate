//go:build linux

package gate

import (
	"encoding/binary"
	"errors"
	"time"

	"golang.org/x/sys/unix"
)

// epoll 实现（生产目标平台）。水平触发；token 直接进内核的 epoll_data.u64
// （EpollEvent 的 Fd/Pad 两个 int32 合起来就是那个 union），事件回来零查表。
// notify 用一个常驻 eventfd。
type epollPoller struct {
	ep   int
	efd  int // eventfd：notify
	ebuf []unix.EpollEvent
}

func newPoller() (poller, error) {
	ep, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	efd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		_ = unix.Close(ep)
		return nil, err
	}
	p := &epollPoller{ep: ep, efd: efd}
	ev := packEvent(notifyToken, unix.EPOLLIN)
	if err := unix.EpollCtl(ep, unix.EPOLL_CTL_ADD, efd, &ev); err != nil {
		_ = unix.Close(efd)
		_ = unix.Close(ep)
		return nil, err
	}
	return p, nil
}

func packEvent(tok token, events uint32) unix.EpollEvent {
	return unix.EpollEvent{Events: events, Fd: int32(uint64(tok)), Pad: int32(uint64(tok) >> 32)}
}

func unpackToken(ev *unix.EpollEvent) token {
	return token(uint64(uint32(ev.Fd)) | uint64(uint32(ev.Pad))<<32)
}

func epollEvents(want interest) uint32 {
	// EPOLLRDHUP 常开：对端半关也要报出来（hup 只是提示，仍走一次 read 拿原因）。
	ev := uint32(unix.EPOLLRDHUP)
	if want&interestRead != 0 {
		ev |= unix.EPOLLIN
	}
	if want&interestWrite != 0 {
		ev |= unix.EPOLLOUT
	}
	return ev
}

func (p *epollPoller) add(fd int, tok token, want interest) error {
	ev := packEvent(tok, epollEvents(want))
	return unix.EpollCtl(p.ep, unix.EPOLL_CTL_ADD, fd, &ev)
}

func (p *epollPoller) mod(fd int, tok token, want interest) error {
	ev := packEvent(tok, epollEvents(want))
	return unix.EpollCtl(p.ep, unix.EPOLL_CTL_MOD, fd, &ev)
}

func (p *epollPoller) del(fd int) error {
	return unix.EpollCtl(p.ep, unix.EPOLL_CTL_DEL, fd, nil)
}

func (p *epollPoller) wait(out []event, timeout time.Duration) (int, error) {
	ms := -1
	if timeout >= 0 {
		ms = int(timeout / time.Millisecond)
		if timeout > 0 && ms == 0 {
			ms = 1 // 向上取整：0 会退化成忙轮询
		}
	}
	if cap(p.ebuf) < len(out) {
		p.ebuf = make([]unix.EpollEvent, len(out))
	}
	n, err := unix.EpollWait(p.ep, p.ebuf[:len(out)], ms)
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			return 0, nil
		}
		return 0, err
	}
	cnt := 0
	for i := range n {
		kev := &p.ebuf[i]
		tok := unpackToken(kev)
		if tok == notifyToken {
			var b [8]byte
			_, _ = unix.Read(p.efd, b[:]) // 清计数；EAGAIN 无妨
		}
		out[cnt] = event{
			tok:   tok,
			read:  kev.Events&(unix.EPOLLIN|unix.EPOLLRDHUP) != 0,
			write: kev.Events&unix.EPOLLOUT != 0,
			hup:   kev.Events&(unix.EPOLLHUP|unix.EPOLLERR|unix.EPOLLRDHUP) != 0,
		}
		cnt++
	}
	return cnt, nil
}

func (p *epollPoller) notify() error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, err := unix.Write(p.efd, b[:])
	if errors.Is(err, unix.EAGAIN) {
		return nil // 计数已满 = 唤醒已在途，幂等语义满足
	}
	return err
}

func (p *epollPoller) close() error {
	_ = unix.Close(p.efd)
	return unix.Close(p.ep)
}
