//go:build darwin || linux

package gate

import (
	"fmt"
	"net"
	"net/netip"

	"golang.org/x/sys/unix"
)

func unixClose(fd int) error { return unix.Close(fd) }

// listenTCP 创建一个非阻塞监听套接字。reusePort 只在 Linux 多 loop 路径为 true。
func listenTCP(addr string, reusePort bool) (fd int, bound netip.AddrPort, err error) {
	ta, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return -1, netip.AddrPort{}, err
	}
	ip := ta.IP
	v6 := ip != nil && ip.To4() == nil && len(ip) == net.IPv6len
	af := unix.AF_INET
	if v6 {
		af = unix.AF_INET6
	}
	fd, err = newSocket(af)
	if err != nil {
		return -1, netip.AddrPort{}, err
	}
	fail := func(e error) (int, netip.AddrPort, error) {
		_ = unix.Close(fd)
		return -1, netip.AddrPort{}, e
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return fail(err)
	}
	if reusePort {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
			return fail(err)
		}
	}
	var sa unix.Sockaddr
	if v6 {
		sa6 := &unix.SockaddrInet6{Port: ta.Port}
		copy(sa6.Addr[:], ip.To16())
		sa = sa6
	} else {
		sa4 := &unix.SockaddrInet4{Port: ta.Port}
		if ip4 := ip.To4(); ip4 != nil {
			copy(sa4.Addr[:], ip4)
		}
		sa = sa4
	}
	if err := unix.Bind(fd, sa); err != nil {
		return fail(fmt.Errorf("gate: bind %s: %w", addr, err))
	}
	if err := unix.Listen(fd, 1024); err != nil {
		return fail(err)
	}
	got, err := unix.Getsockname(fd)
	if err != nil {
		return fail(err)
	}
	return fd, sockaddrToAddrPort(got), nil
}

func sockaddrToAddrPort(sa unix.Sockaddr) netip.AddrPort {
	switch a := sa.(type) {
	case *unix.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(a.Addr), uint16(a.Port))
	case *unix.SockaddrInet6:
		return netip.AddrPortFrom(netip.AddrFrom16(a.Addr), uint16(a.Port))
	default:
		return netip.AddrPort{}
	}
}

// applySocketOpts 按 Socket 配置执行 setsockopt。
func applySocketOpts(fd int, so Socket, keepAlive int) {
	if !so.Nagle {
		_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	}
	if so.RecvBuffer > 0 {
		_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, so.RecvBuffer)
	}
	if so.SendBuffer > 0 {
		_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, so.SendBuffer)
	}
	if keepAlive > 0 {
		_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE, 1)
		setKeepAliveIdle(fd, keepAlive)
	}
}
