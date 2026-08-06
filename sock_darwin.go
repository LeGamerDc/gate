//go:build darwin

package gate

import "golang.org/x/sys/unix"

func newSocket(af int) (int, error) {
	fd, err := unix.Socket(af, unix.SOCK_STREAM, 0)
	if err != nil {
		return -1, err
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	unix.CloseOnExec(fd)
	return fd, nil
}

func sysAccept(lfd int) (int, unix.Sockaddr, error) {
	fd, sa, err := unix.Accept(lfd)
	if err != nil {
		return -1, nil, err
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return -1, nil, err
	}
	unix.CloseOnExec(fd)
	return fd, sa, nil
}

func setKeepAliveIdle(fd, secs int) {
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPALIVE, secs)
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, max(1, secs/3))
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPCNT, 3)
}

// Darwin 没有内核级 accept 负载均衡：单 listener + 轮转投递（R-D7）。
const platformReusePort = false
