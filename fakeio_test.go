package gate

import (
	"bytes"
	"slices"
	"syscall"
	"testing"
)

// fakeIO 是 07 第 0 层要求的可编程 io：writev 能在任意字节位置返回部分成功
// （按字节，不按 iovec 边界），能编排「部分写 → EAGAIN → 真错误」序列，
// 并记录完整字节流水供逐字节一致断言。
type fakeIO struct {
	wrote      bytes.Buffer
	script     []ioStep // 每次 writev 消费一步；耗尽后全收
	writevN    int
	readScript []readStep
	closed     bool
}

type ioStep struct {
	accept int // 本次调用最多收多少字节；<0 = 全收
	err    error
}

type readStep struct {
	data []byte
	err  error
}

func (f *fakeIO) writev(_ int, vec [][]byte) (int, error) {
	f.writevN++
	step := ioStep{accept: -1}
	if len(f.script) > 0 {
		step = f.script[0]
		f.script = f.script[1:]
	}
	total := 0
	for _, b := range vec {
		total += len(b)
	}
	n := total
	if step.accept >= 0 && step.accept < n {
		n = step.accept
	}
	remain := n
	for _, b := range vec {
		if remain == 0 {
			break
		}
		k := min(remain, len(b))
		f.wrote.Write(b[:k])
		remain -= k
	}
	return n, step.err
}

func (f *fakeIO) read(_ int, p []byte) (int, error) {
	if len(f.readScript) == 0 {
		return 0, syscall.EAGAIN // 无剧本 = 内核没数据
	}
	step := f.readScript[0]
	n := copy(p, step.data)
	if n < len(step.data) {
		// p 装不下这一步的剩余数据：留给下一次 read（大帧直读分多次取）。
		f.readScript[0].data = step.data[n:]
		return n, nil
	}
	f.readScript = f.readScript[1:]
	if n == 0 && step.err == nil {
		return 0, nil // 显式 EOF 步
	}
	return n, step.err
}

// feed 追加读数据步。
func (f *fakeIO) feed(chunks ...[]byte) {
	for _, c := range chunks {
		f.readScript = append(f.readScript, readStep{data: c})
	}
}

func (f *fakeIO) close(_ int) error {
	f.closed = true
	return nil
}

// fakeLoop 记录 postDirty / maybeNotify，并断言 dirty 节点绝不重复挂链（O14）。
type fakeLoop struct {
	t        *testing.T
	dirty    []*outbound
	notifies int
}

func (l *fakeLoop) postDirty(o *outbound) {
	if slices.Contains(l.dirty, o) {
		l.t.Fatal("同一连接被重复挂进 dirty 链（O14 破坏侵入式链表）")
	}
	l.dirty = append(l.dirty, o)
}

func (l *fakeLoop) maybeNotify() { l.notifies++ }

// drainDirty 模拟 loop 的 dirty 处理：逐个弹出并 flush（消费即清 armed）。
func (l *fakeLoop) drainDirty() writeStatus {
	st := writeIdle
	for len(l.dirty) > 0 {
		o := l.dirty[0]
		l.dirty = l.dirty[1:]
		st = o.flush(true, false)
	}
	return st
}

// ─── 组装 ───

type outboundOpt func(*outConfig)

func newTestOutbound(t *testing.T, opts ...outboundOpt) (*outbound, *fakeIO, *fakeLoop) {
	t.Helper()
	cfg := normalizeOutConfig(Outbound{}, Limits{}, false)
	for _, opt := range opts {
		opt(&cfg)
	}
	io := &fakeIO{}
	loop := &fakeLoop{t: t}
	o := newOutbound(cfg, io, 3, newLoopEnv(encoderOptions{}), loop)
	return o, io, loop
}

// decodeStream 用生产 codec 把 fakeIO 的流水解回消息（仅 TCP 模式）。
func decodeStream(t *testing.T, stream []byte, recv Cipher, maxDecomp int) [][]byte {
	t.Helper()
	c := clientCodec(maxMessageSize, maxDecomp)
	c.requireEncrypt = recv != nil
	zdec := mustDecoder(t)
	var got [][]byte
	for len(stream) > 0 {
		f, n, ok, err := c.parse(stream)
		if !ok || err != nil {
			t.Fatalf("decodeStream: ok=%v err=%v", ok, err)
		}
		if err := c.deliver(f, recv, zdec, func(msg []byte, _ bool) error {
			got = append(got, append([]byte(nil), msg...))
			return nil
		}); err != nil {
			t.Fatalf("decodeStream deliver: %v", err)
		}
		stream = stream[n:]
	}
	return got
}
