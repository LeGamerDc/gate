//go:build darwin || linux

package gate

import (
	"errors"

	"golang.org/x/sys/unix"
)

// sysIO 是 connIO 的生产实现：直通 syscall，EINTR 内部重试。
// 只在 loop 线程上调用（R2）。
type sysIO struct{}

func (sysIO) read(fd int, p []byte) (int, error) {
	for {
		n, err := unix.Read(fd, p)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return 0, err
		}
		return n, nil
	}
}

func (sysIO) writev(fd int, vec [][]byte) (int, error) {
	for {
		n, err := unix.Writev(fd, vec)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return 0, err
		}
		return n, nil
	}
}

func (sysIO) close(fd int) error {
	return unix.Close(fd)
}
