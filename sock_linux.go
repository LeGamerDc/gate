//go:build linux

package gate

import "golang.org/x/sys/unix"

func newSocket(af int) (int, error) {
	return unix.Socket(af, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
}

func sysAccept(lfd int) (int, unix.Sockaddr, error) {
	return unix.Accept4(lfd, unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
}

func setKeepAliveIdle(fd, secs int) {
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPIDLE, secs)
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, max(1, secs/3))
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPCNT, 3)
}

// reusePortAccept：Linux 用 SO_REUSEPORT，每 loop 自己 listen（R-D7 对偶）。
const platformReusePort = true
