//go:build darwin

package gate

import (
	"errors"
	"time"

	"golang.org/x/sys/unix"
)

// kqueue 实现。与 epoll 的两处本质差异（04「平台差异总表」）：
//
//  1. 读写是两个独立 filter，各报各的事件。wait 内按 token 聚合成一个 event
//     （R14）——上层「先写后读」的处理顺序以聚合为前提。
//  2. mod 不是「设置成这个集合」而是逐 filter 开关。这里在 add 时就把两个
//     filter 都 EV_ADD（按 want 决定 ENABLE/DISABLE），mod 恒提交两条
//     ENABLE/DISABLE——对已是目标状态的 filter 是 no-op，免去记忆当前兴趣。
//
// token 的携带：epoll 的 u64 是纯整数，token 直接进内核；kqueue 的
// Kevent_t.Udata 在 Go 里是 *byte——把非指针值塞进指针字段是精确 GC 的
// 隐患（扫描时按指针追踪）。所以这里用 fd 索引的 token 表替代 udata，
// 事件回来仍是 O(1) 查表、零哈希。macOS 是开发测试平台，表的内存无关紧要。
type kqPoller struct {
	kq   int
	toks []token         // fd → token
	kbuf []unix.Kevent_t // 内核事件暂存；读写分报，容量按 2×out 备
	rbuf []unix.Kevent_t // add/mod/del 的 EV_RECEIPT 回执
}

func newPoller() (poller, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	p := &kqPoller{kq: kq, rbuf: make([]unix.Kevent_t, 2)}
	// EVFILT_USER 承载 notify：EV_CLEAR 送达即复位。
	_, err = unix.Kevent(kq, []unix.Kevent_t{{
		Ident:  0,
		Filter: unix.EVFILT_USER,
		Flags:  unix.EV_ADD | unix.EV_CLEAR,
	}}, nil, nil)
	if err != nil {
		_ = unix.Close(kq)
		return nil, err
	}
	return p, nil
}

func (p *kqPoller) setTok(fd int, tok token) {
	for fd >= len(p.toks) {
		p.toks = append(p.toks, 0)
	}
	p.toks[fd] = tok
}

// submit 提交带 EV_RECEIPT 的变更并吞掉 ENOENT（重复删除等良性错误）。
func (p *kqPoller) submit(changes []unix.Kevent_t) error {
	for i := range changes {
		changes[i].Flags |= unix.EV_RECEIPT
	}
	n, err := unix.Kevent(p.kq, changes, p.rbuf[:len(changes)], nil)
	if err != nil {
		return err
	}
	for _, r := range p.rbuf[:n] {
		if r.Flags&unix.EV_ERROR != 0 && r.Data != 0 && syscallErrno(r.Data) != unix.ENOENT {
			return syscallErrno(r.Data)
		}
	}
	return nil
}

func syscallErrno(d int64) unix.Errno { return unix.Errno(d) }

func kqFlags(enable bool) uint16 {
	if enable {
		return unix.EV_ADD | unix.EV_ENABLE
	}
	return unix.EV_ADD | unix.EV_DISABLE
}

func (p *kqPoller) add(fd int, tok token, want interest) error {
	p.setTok(fd, tok)
	return p.submit([]unix.Kevent_t{
		{Ident: uint64(fd), Filter: unix.EVFILT_READ, Flags: kqFlags(want&interestRead != 0)},
		{Ident: uint64(fd), Filter: unix.EVFILT_WRITE, Flags: kqFlags(want&interestWrite != 0)},
	})
}

func (p *kqPoller) mod(fd int, tok token, want interest) error {
	p.setTok(fd, tok)
	return p.submit([]unix.Kevent_t{
		{Ident: uint64(fd), Filter: unix.EVFILT_READ, Flags: kqFlags(want&interestRead != 0)},
		{Ident: uint64(fd), Filter: unix.EVFILT_WRITE, Flags: kqFlags(want&interestWrite != 0)},
	})
}

func (p *kqPoller) del(fd int) error {
	if fd < len(p.toks) {
		p.toks[fd] = 0
	}
	return p.submit([]unix.Kevent_t{
		{Ident: uint64(fd), Filter: unix.EVFILT_READ, Flags: unix.EV_DELETE},
		{Ident: uint64(fd), Filter: unix.EVFILT_WRITE, Flags: unix.EV_DELETE},
	})
}

func (p *kqPoller) wait(out []event, timeout time.Duration) (int, error) {
	var ts *unix.Timespec
	if timeout >= 0 {
		t := unix.NsecToTimespec(timeout.Nanoseconds())
		ts = &t
	}
	if cap(p.kbuf) < 2*len(out)+1 {
		p.kbuf = make([]unix.Kevent_t, 2*len(out)+1)
	}
	n, err := unix.Kevent(p.kq, nil, p.kbuf[:cap(p.kbuf)], ts)
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			return 0, nil
		}
		return 0, err
	}

	// 按 token 聚合：同一 fd 的读写两个 filter 合成一个 event（R14）。
	cnt := 0
	for _, kev := range p.kbuf[:n] {
		var tok token
		switch kev.Filter {
		case unix.EVFILT_USER:
			tok = notifyToken
		default:
			if int(kev.Ident) < len(p.toks) {
				tok = p.toks[kev.Ident]
			}
		}
		var e *event
		for i := 0; i < cnt; i++ {
			if out[i].tok == tok {
				e = &out[i]
				break
			}
		}
		if e == nil {
			if cnt == len(out) {
				break // out 满了；水平触发下一轮还会再报
			}
			out[cnt] = event{tok: tok}
			e = &out[cnt]
			cnt++
		}
		switch kev.Filter {
		case unix.EVFILT_READ:
			e.read = true
		case unix.EVFILT_WRITE:
			e.write = true
		}
		if kev.Flags&unix.EV_EOF != 0 || kev.Flags&unix.EV_ERROR != 0 {
			e.hup = true
		}
	}
	return cnt, nil
}

func (p *kqPoller) notify() error {
	_, err := unix.Kevent(p.kq, []unix.Kevent_t{{
		Ident:  0,
		Filter: unix.EVFILT_USER,
		Fflags: unix.NOTE_TRIGGER,
	}}, nil, nil)
	return err
}

func (p *kqPoller) close() error {
	return unix.Close(p.kq)
}
